from __future__ import annotations

import html
import json
import math
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TextIO, Tuple
from urllib.parse import urlparse

import requests
import yaml
from tqdm import tqdm

from hash_db import add_sha256, has_sha256, init_db
from pipeline_filter import PipelineFilter, exif_extract, sha256_bytes
from utils_fs import ensure_dirs, next_id, save_image_and_metadata


DEFAULT_API_URL = "https://commons.wikimedia.org/w/api.php"
DEFAULT_RETRY_BASE_SEC = 0.5
DEFAULT_RETRY_MAX_WAIT_SEC = 60.0

class RetryableHTTPError(RuntimeError):
    def __init__(self, *, url: str, status_code: int, retry_after_sec: Optional[float]) -> None:
        super().__init__(f"retryable http status={status_code} url={url}")
        self.url = url
        self.status_code = status_code
        self.retry_after_sec = retry_after_sec


class RateLimiter:
    def __init__(self, min_interval_sec: float) -> None:
        self.min_interval_sec = max(0.0, float(min_interval_sec))
        self._last_global = 0.0
        self._last_by_domain: Dict[str, float] = {}
        self._blocked_until_by_domain: Dict[str, float] = {}
        self._lock = threading.Lock()

    def update_min_interval(self, min_interval_sec: float) -> None:
        with self._lock:
            self.min_interval_sec = max(0.0, float(min_interval_sec))

    def _host_from_url(self, url: str) -> str:
        return urlparse(url).netloc or "global"

    def penalize(self, url: str, wait_sec: float) -> None:
        if wait_sec <= 0:
            return
        host = self._host_from_url(url)
        deadline = time.monotonic() + wait_sec
        with self._lock:
            self._blocked_until_by_domain[host] = max(self._blocked_until_by_domain.get(host, 0.0), deadline)

    def wait(self, url: str) -> None:
        host = self._host_from_url(url)

        with self._lock:
            now = time.monotonic()
            blocked_until = self._blocked_until_by_domain.get(host, 0.0)
            wait_sec = max(0.0, blocked_until - now)
            min_interval_sec = self.min_interval_sec
            if min_interval_sec > 0:
                last_domain = self._last_by_domain.get(host, 0.0)
                wait_sec = max(
                    wait_sec,
                    min_interval_sec - (now - self._last_global),
                    min_interval_sec - (now - last_domain),
                )
        if wait_sec > 0:
            time.sleep(wait_sec)

        with self._lock:
            ts = time.monotonic()
            self._last_global = ts
            self._last_by_domain[host] = ts

class ThroughputMonitor:
    def __init__(self, interval_sec: float = 60.0) -> None:
        self.interval_sec = interval_sec
        self.window_started = time.time()
        self.requests = 0
        self.r429 = 0
        self.download_attempts = 0
        self.download_success = 0
        self.filter_pass = 0

    def record_request(self, status_code: Optional[int]) -> None:
        self.requests += 1
        if status_code == 429:
            self.r429 += 1

    def record_download_attempt(self) -> None:
        self.download_attempts += 1

    def record_download_success(self) -> None:
        self.download_success += 1

    def record_filter_pass(self) -> None:
        self.filter_pass += 1

    def maybe_log(self, cat: str) -> None:
        now = time.time()
        elapsed = now - self.window_started
        if elapsed < self.interval_sec:
            return
        req_per_min = self.requests * 60.0 / max(elapsed, 1e-6)
        ratio_429 = self.r429 / max(self.requests, 1)
        dl_success_ratio = self.download_success / max(self.download_attempts, 1)
        filter_pass_ratio = self.filter_pass / max(self.download_success, 1)
        print(
            f"[Wikicommons][{cat}] throughput/min requests={req_per_min:.1f} "
            f"429_rate={ratio_429:.2%} download_success_rate={dl_success_ratio:.2%} "
            f"filter_pass_rate={filter_pass_ratio:.2%}"
        )
        self.window_started = now
        self.requests = 0
        self.r429 = 0
        self.download_attempts = 0
        self.download_success = 0
        self.filter_pass = 0

def load_cfg() -> dict:
    cfg_path = Path(__file__).with_name("config.yaml")
    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    dataset_root = Path(cfg.get("dataset_root", "."))
    if not dataset_root.is_absolute():
        cfg["dataset_root"] = str((cfg_path.parent / dataset_root).resolve())
    return cfg


def clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def normalize_license(value: str) -> str:
    v = value.lower().strip()
    v = v.replace("_", "-")
    v = v.replace(" ", "-")
    return re.sub(r"-+", "-", v)


def extract_license_fields(imageinfo: Dict[str, Any]) -> Tuple[str, str]:
    ext = imageinfo.get("extmetadata") or {}
    short_name = clean_text((ext.get("LicenseShortName") or {}).get("value"))
    code = clean_text((ext.get("License") or {}).get("value"))
    usage_terms = clean_text((ext.get("UsageTerms") or {}).get("value"))
    license_url = clean_text((ext.get("LicenseUrl") or {}).get("value"))

    license_type = short_name or code or usage_terms
    return license_type, license_url


def search_commons(
    *,
    api_url: str,
    keyword: str,
    continue_token: Optional[str],
    per_page: int,
    ua: str,
    rate_limiter: Optional[RateLimiter],
    max_retries: int,
    max_retry_wait_sec: float,
    monitor: Optional[ThroughputMonitor],
) -> Dict[str, Any]:
    params = {
        "action": "query",
        "format": "json",
        "generator": "search",
        "gsrsearch": keyword,
        "gsrnamespace": 6,  # File namespace
        "gsrlimit": per_page,
        "gsrwhat": "text",
        "prop": "imageinfo|info",
        "inprop": "url",
        "iiprop": "url|extmetadata|size|mime",
    }
    if continue_token:
        params["gsrcontinue"] = continue_token

    r = request_with_retry(
        api_url,
        ua=ua,
        timeout=30,
        max_retries=max_retries,
        rate_limiter=rate_limiter,
        min_interval_sec=0,
        params=params,
        max_retry_wait_sec=max_retry_wait_sec,
        monitor=monitor,
    )
    return r.json()


def parse_retry_after(headers: requests.structures.CaseInsensitiveDict) -> Optional[float]:
    value = headers.get("Retry-After")
    if not value:
        return None
    value = value.strip()

    if value.isdigit():
        return max(0.0, float(value))

    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None

    now = time.time()
    return max(0.0, retry_at.timestamp() - now)


def compute_retry_wait(
        *,
        retry_idx: int,
        min_interval_sec: float,
        retry_after_sec: Optional[float],
        base_backoff_sec: float = DEFAULT_RETRY_BASE_SEC,
        max_wait_sec: float = DEFAULT_RETRY_MAX_WAIT_SEC,
) -> float:
    exp_backoff = max(min_interval_sec, base_backoff_sec * (2 ** retry_idx))
    if retry_after_sec is not None:
        return min(max_wait_sec, max(retry_after_sec, exp_backoff))
    return min(max_wait_sec, exp_backoff)


def request_with_retry(
        url: str,
        *,
        ua: str,
        timeout: int,
        max_retries: int,
        rate_limiter: Optional[RateLimiter],
        min_interval_sec: float,
        max_retry_wait_sec: float,
        params: Optional[Dict[str, Any]] = None,
        monitor: Optional[ThroughputMonitor] = None,
) -> requests.Response:
    last_err: Optional[Exception] = None
    for retry in range(max_retries):
        try:
            if rate_limiter:
                rate_limiter.wait(url)
            r = requests.get(url, params=params, headers={"User-Agent": ua}, timeout=timeout)
            if monitor:
                monitor.record_request(r.status_code)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                retry_after_sec = parse_retry_after(r.headers)
                wait_sec = compute_retry_wait(
                    retry_idx=retry,
                    min_interval_sec=min_interval_sec,
                    retry_after_sec=retry_after_sec,
                    max_wait_sec=max_retry_wait_sec,
                )
                jitter_sec = min(2.0, wait_sec * 0.25) * random.random()
                effective_wait = wait_sec + jitter_sec
                print(f"[Wikicommons] retry status={r.status_code} wait={effective_wait:.2f}s url={url}")
                if rate_limiter:
                    rate_limiter.penalize(url, effective_wait)
                else:
                    time.sleep(effective_wait)
                last_err = RetryableHTTPError(url=url, status_code=r.status_code, retry_after_sec=retry_after_sec)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if retry == max_retries - 1:
                break
            wait_sec = compute_retry_wait(
                retry_idx=retry,
                min_interval_sec=min_interval_sec,
                retry_after_sec=None,
                max_wait_sec=max_retry_wait_sec,
            )
            time.sleep(wait_sec)

    if isinstance(last_err, RetryableHTTPError):
        raise last_err
    raise RuntimeError(f"request failed: {url} err={last_err}")

def allow_license(license_type: str, allowlist: List[str]) -> bool:
    if not allowlist:
        return True

    normalized = normalize_license(license_type)
    for allowed in allowlist:
        token = normalize_license(allowed)
        if token and token in normalized:
            return True
    return False

def prefilter_imageinfo(imageinfo: Dict[str, Any], filter_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    width = int(imageinfo.get("width") or 0)
    height = int(imageinfo.get("height") or 0)
    file_size_bytes = int(imageinfo.get("size") or 0)

    if width <= 0 or height <= 0:
        return False, "invalid_size_metadata"

    min_side = int(filter_cfg.get("min_side", 0))
    if min(width, height) < min_side:
        return False, "min_side_too_small"

    max_aspect_ratio = float(filter_cfg.get("max_aspect_ratio", 999.0))
    aspect_ratio = max(width, height) / max(1, min(width, height))
    if aspect_ratio > max_aspect_ratio:
        return False, "aspect_ratio_too_extreme"

    min_filesize_kb = float(filter_cfg.get("min_filesize_kb", 0))
    if file_size_bytes > 0 and (file_size_bytes / 1024.0) < min_filesize_kb:
        return False, "filesize_too_small"

    max_pixels = filter_cfg.get("max_pixels")
    if max_pixels is not None and (width * height) > int(max_pixels):
        return False, "too_many_pixels"

    return True, ""


def download(url: str, ua: str, timeout: int, max_retries: int, rate_limiter: Optional[RateLimiter], min_interval_sec: float, max_retry_wait_sec: float) -> bytes:
    response = request_with_retry(
        url,
        ua=ua,
        timeout=timeout,
        max_retries=max_retries,
        rate_limiter=rate_limiter,
        min_interval_sec=min_interval_sec,
        max_retry_wait_sec=max_retry_wait_sec,
    )
    return response.content

def write_jsonl(log_fh: TextIO, payload: Dict[str, Any]) -> None:
    log_fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    log_fh.flush()

def load_progress(progress_path: Path) -> Dict[str, Any]:
    if not progress_path.exists():
        return {}
    try:
        with progress_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def save_progress(progress_path: Path, progress: Dict[str, Any]) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    with progress_path.open("w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def count_existing_wcm_images(dataset_root: str, category: str) -> int:
    cat_dir = Path(dataset_root) / "raw_images" / category
    if not cat_dir.exists():
        return 0
    return sum(1 for p in cat_dir.glob("WCM_*") if p.is_file())


def build_backfill_keywords(category: str, keywords: List[str]) -> List[str]:
    category_term = category.replace("_", " ").strip()
    pool = list(keywords)
    if category_term:
        pool.extend([category_term, f"{category_term} photography", f"{category_term} photo"])

    expanded: List[str] = []
    seen = set()
    for kw in pool:
        kw_norm = str(kw).strip()
        if not kw_norm:
            continue
        if kw_norm.lower() in seen:
            continue
        seen.add(kw_norm.lower())
        expanded.append(kw_norm)
    return expanded


def build_metadata(
    *,
    image_id: str,
    category: str,
    original_url: str,
    source_page_url: str,
    author: str,
    license_type: str,
    license_url: str,
    search_keyword: str,
    resolution_hw: Tuple[int, int],
    exif_data: Dict[str, Any],
    pipeline_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "image_id": image_id,
        "category": category,
        "source_platform": "Wikimedia Commons",
        "original_url": original_url,
        "source_page_url": source_page_url,
        "author": author,
        "license_type": license_type,
        "license_url": license_url,
        "search_keyword": search_keyword,
        "resolution": [resolution_hw[1], resolution_hw[0]],
        "exif_data": exif_data,
        "pipeline_metrics": pipeline_metrics,
    }


def main() -> None:
    cfg = load_cfg()
    dataset_root = cfg["dataset_root"]

    ensure_dirs(dataset_root, cfg["categories"])
    init_db(dataset_root)

    ua = cfg["download"]["user_agent"]
    max_retries_per_try = int(cfg["download"].get("max_retries_per_try", cfg["download"].get("max_retries", 2)))
    max_total_attempts_per_url = int(cfg["download"].get("max_total_attempts_per_url", 4))
    max_retry_wait_sec = float(cfg["download"].get("max_retry_wait_sec", DEFAULT_RETRY_MAX_WAIT_SEC))
    search_sleep_sec = float(cfg["download"].get("api_sleep_sec", cfg["download"].get("sleep_sec", 1.0)))
    download_sleep_sec = float(cfg["download"].get("image_sleep_sec", cfg["download"].get("sleep_sec", 1.0)))
    search_rate_limiter = RateLimiter(search_sleep_sec)
    download_rate_limiter = RateLimiter(download_sleep_sec)

    download_workers = max(1, int(cfg["download"].get("download_workers", 4)))
    monitor_interval_sec = float(cfg["download"].get("monitor_interval_sec", 60))
    pf = PipelineFilter(cfg["pipeline_filter"])
    filter_cfg = cfg["pipeline_filter"]

    prefixes = cfg["naming"]["prefixes"]
    digits = int(cfg["naming"]["digits"])

    wc_cfg = cfg["wikicommons"]
    api_url = wc_cfg.get("api_url", DEFAULT_API_URL)
    per_page = int(wc_cfg.get("per_page", 50))
    search_max_retries = int(wc_cfg.get("search_max_retries", 5))
    primary_ratio = float(wc_cfg.get("primary_ratio", 0.7))
    primary_ratio = max(0.0, min(1.0, primary_ratio))
    secondary_mode = str(wc_cfg.get("secondary_mode", "round_robin")).strip().lower()
    stop_when_exhausted = bool(wc_cfg.get("stop_when_exhausted", True))
    allow_cross_keyword_backfill = bool(wc_cfg.get("allow_cross_keyword_backfill", False))
    if cfg["licenses"].get("mode") == "cc0_only":
        allowlist = ["cc0", "public domain", "pdm"]
    else:
        allowlist = wc_cfg.get("license_allowlist") or cfg["licenses"].get("allowlist_wikicommons", []) or []

    log_path = Path(dataset_root) / "logs" / "wikicommons_download_retry.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    progress_path = Path(__file__).parent / "logs" / "wikicommons_progress.json"
    progress = load_progress(progress_path)

    with log_path.open("a", encoding="utf-8") as retry_log:
        for cat, spec in cfg["categories"].items():
            target = int(spec.get("target_count", 0))
            keywords = spec.get("keywords", [])
            monitor = ThroughputMonitor(interval_sec=monitor_interval_sec)

            if not keywords:
                print(f"\n[Wikicommons] category={cat} has no keywords, skip.")
                continue

            cat_progress = progress.get(cat, {}) if isinstance(progress.get(cat, {}), dict) else {}
            saved = int(cat_progress.get("saved_count", 0) or 0)
            existing_saved = count_existing_wcm_images(dataset_root, cat)
            if existing_saved > saved:
                saved = existing_saved

            primary_saved = int(cat_progress.get("primary_saved", 0) or 0)
            secondary_saved = int(cat_progress.get("secondary_saved", 0) or 0)
            stats = cat_progress.get("stats", {}) if isinstance(cat_progress.get("stats", {}), dict) else {}
            cumulative_stats = {
                "candidates": int(stats.get("candidates", 0) or 0),
                "downloaded": int(stats.get("downloaded", 0) or 0),
                "filter_passed": int(stats.get("filter_passed", 0) or 0),
                "accepted": int(stats.get("accepted", 0) or 0),
            }
            deferred_urls: List[Dict[str, Any]] = []
            seen_urls: Set[str] = set(cat_progress.get("seen_urls") or [])
            seen_pageids: Set[str] = set(str(v) for v in (cat_progress.get("seen_pageids") or []))
            state_lock = threading.Lock()

            primary_kw = keywords[0]
            secondary_kws = keywords[1:]

            primary_target = min(target, math.ceil(target * primary_ratio))
            secondary_target = target - primary_target

            primary_continue: Optional[str] = cat_progress.get("primary_continue")
            secondary_continue_map: Dict[str, Optional[str]] = {
                kw: token
                for kw, token in (cat_progress.get("secondary_continue_map") or {}).items()
                if isinstance(kw, str)
            }
            exhausted_kws = set(cat_progress.get("exhausted_keywords") or [])

            print(
                f"\n[Wikicommons] category={cat} target={target} saved_init={saved} existing_saved={existing_saved} "
                f"primary_kw={primary_kw} secondary_kws={secondary_kws} "
                f"primary_target={primary_target} secondary_target={secondary_target} "
                f"secondary_mode={secondary_mode} stop_when_exhausted={stop_when_exhausted} "
                f"allow_cross_keyword_backfill={allow_cross_keyword_backfill}"
            )

            def flush_progress() -> None:
                progress[cat] = {
                    "saved_count": saved,
                    "primary_saved": primary_saved,
                    "secondary_saved": secondary_saved,
                    "primary_continue": primary_continue,
                    "secondary_continue_map": secondary_continue_map,
                    "exhausted_keywords": sorted(exhausted_kws),
                    "stats": cumulative_stats,
                    "seen_urls": sorted(seen_urls)[-5000:],
                    "seen_pageids": sorted(seen_pageids)[-5000:],
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                save_progress(progress_path, progress)

            def adapt_download_throttle() -> None:
                if monitor.requests < 5:
                    return
                r429 = monitor.r429 / max(monitor.requests, 1)
                curr = download_rate_limiter.min_interval_sec
                if r429 > 0.15:
                    download_rate_limiter.update_min_interval(min(curr + 0.2, 3.0))
                elif r429 < 0.02:
                    download_rate_limiter.update_min_interval(max(curr - 0.1, 0.3))

            def handle_download_candidate(candidate: Dict[str, Any], from_deferred: bool) -> Dict[str, Any]:
                nonlocal saved, primary_saved, secondary_saved

                with state_lock:
                    if saved >= target:
                        return {"outcome": "skip", "downloaded": 0, "filter_passed": False, "accepted": False}

                img_url = candidate["img_url"]
                kw = candidate["kw"]
                stage = candidate["stage"]
                monitor.record_download_attempt()

                try:
                    b = download(
                        img_url,
                        ua=ua,
                        timeout=int(cfg["download"]["timeout_sec"]),
                        max_retries=max_retries_per_try,
                        rate_limiter=download_rate_limiter,
                        min_interval_sec=download_rate_limiter.min_interval_sec,
                        max_retry_wait_sec=max_retry_wait_sec,
                    )
                except RetryableHTTPError as e:
                    if candidate["attempt_count"] >= max_total_attempts_per_url:
                        write_jsonl(retry_log, {
                            "event": "download_final_drop",
                            "category": cat,
                            "keyword": kw,
                            "stage": stage,
                            "url": img_url,
                            "status_code": e.status_code,
                            "attempt_count": candidate["attempt_count"],
                            "accum_wait_sec": round(candidate["accum_wait_sec"], 3),
                            "result": "dropped_max_attempts",
                        })
                        return {"outcome": "drop", "downloaded": 0, "filter_passed": False, "accepted": False}

                    retry_wait = compute_retry_wait(
                        retry_idx=max(0, candidate["attempt_count"] - 1),
                        min_interval_sec=download_rate_limiter.min_interval_sec,
                        retry_after_sec=e.retry_after_sec,
                        max_wait_sec=max_retry_wait_sec,
                    )
                    candidate["next_retry_ts"] = time.time() + retry_wait
                    candidate["accum_wait_sec"] += retry_wait
                    candidate["last_status_code"] = e.status_code
                    with state_lock:
                        deferred_urls.append(candidate)
                    write_jsonl(retry_log, {
                        "event": "download_deferred",
                        "category": cat,
                        "keyword": kw,
                        "stage": stage,
                        "url": img_url,
                        "status_code": e.status_code,
                        "attempt_count": candidate["attempt_count"],
                        "next_retry_ts": round(candidate["next_retry_ts"], 3),
                        "accum_wait_sec": round(candidate["accum_wait_sec"], 3),
                        "result": "queued",
                        "from_deferred": from_deferred,
                    })
                    return {"outcome": "deferred", "downloaded": 0, "filter_passed": False, "accepted": False}
                except Exception:
                    write_jsonl(retry_log, {
                        "event": "download_non_retryable_error",
                        "category": cat,
                        "keyword": kw,
                        "stage": stage,
                        "url": img_url,
                        "status_code": None,
                        "attempt_count": candidate["attempt_count"],
                        "accum_wait_sec": round(candidate["accum_wait_sec"], 3),
                        "result": "skipped_non_retryable",
                    })
                    return {"outcome": "skip", "downloaded": 0, "filter_passed": False, "accepted": False}

                monitor.record_download_success()
                s256 = sha256_bytes(b)
                with state_lock:
                    if has_sha256(dataset_root, s256):
                        return {"outcome": "skip", "downloaded": 1, "filter_passed": False, "accepted": False}

                ok, metrics, reject_reason = pf.validate(b)
                if not ok:
                    write_jsonl(retry_log, {
                        "event": "download_rejected",
                        "category": cat,
                        "keyword": kw,
                        "stage": stage,
                        "url": img_url,
                        "status_code": 200,
                        "attempt_count": candidate["attempt_count"],
                        "accum_wait_sec": round(candidate["accum_wait_sec"], 3),
                        "result": f"rejected:{reject_reason}",
                    })
                    return {"outcome": "skip", "downloaded": 1, "filter_passed": False, "accepted": False}

                monitor.record_filter_pass()
                image_id = next_id(dataset_root, prefixes["wikicommons"], digits)
                exif_data = exif_extract(b)
                h = int(metrics.get("H", 0))
                w = int(metrics.get("W", 0))
                md = build_metadata(
                    image_id=image_id,
                    category=cat,
                    original_url=img_url,
                    source_page_url=candidate["source_page_url"],
                    author=candidate["author"],
                    license_type=candidate["license_type"],
                    license_url=candidate["license_url"],
                    search_keyword=kw,
                    resolution_hw=(h, w),
                    exif_data=exif_data,
                    pipeline_metrics=metrics,
                )

                with state_lock:
                    if has_sha256(dataset_root, s256):
                        return {"outcome": "skip", "downloaded": 1, "filter_passed": False, "accepted": False}
                    save_image_and_metadata(dataset_root, cat, image_id, b, md)
                    add_sha256(dataset_root, s256, image_id, "wikicommons")

                write_jsonl(retry_log, {
                    "event": "download_success",
                    "category": cat,
                    "keyword": kw,
                    "stage": stage,
                    "url": img_url,
                    "status_code": 200,
                    "attempt_count": candidate["attempt_count"],
                    "accum_wait_sec": round(candidate["accum_wait_sec"], 3),
                    "result": "saved",
                })
                with state_lock:
                    saved += 1
                    if stage == "primary":
                        primary_saved += 1
                    else:
                        secondary_saved += 1

                return {"outcome": "accepted", "downloaded": 1, "filter_passed": True, "accepted": True}

            def process_deferred_queue() -> int:
                if not deferred_urls or saved >= target:
                    return 0
                now_ts = time.time()
                ready, pending = [], []
                for item in deferred_urls:
                    if item.get("next_retry_ts", 0.0) <= now_ts:
                        ready.append(item)
                    else:
                        pending.append(item)
                deferred_urls.clear()
                deferred_urls.extend(pending)

                accepted = 0
                for item in ready:
                    if saved >= target:
                        deferred_urls.append(item)
                        continue
                    item["attempt_count"] += 1
                    res = handle_download_candidate(item, from_deferred=True)
                    if res["filter_passed"]:
                        cumulative_stats["filter_passed"] += 1
                    if res["accepted"]:
                        cumulative_stats["accepted"] += 1
                    accepted += 1
                return accepted

            def process_keyword_page(kw: str, gsrcontinue: Optional[str], stage: str) -> Tuple[int, Optional[str], bool]:
                data = search_commons(
                    api_url=api_url,
                    keyword=kw,
                    continue_token=gsrcontinue,
                    per_page=per_page,
                    ua=ua,
                    rate_limiter=search_rate_limiter,
                    max_retries=search_max_retries,
                    max_retry_wait_sec=max_retry_wait_sec,
                    monitor=monitor,
                )
                pages = (data.get("query") or {}).get("pages") or {}
                if not pages:
                    flush_progress()
                    monitor.maybe_log(cat)
                    adapt_download_throttle()
                    return 0, None, False

                page_candidates = 0
                page_downloaded = 0
                page_filter_passed = 0
                page_accepted = 0
                pending: List[Dict[str, Any]] = []

                for pageid, page in pages.items():
                    if saved >= target:
                        break

                    imageinfo_list = page.get("imageinfo") or []
                    if not imageinfo_list:
                        continue

                    imageinfo = imageinfo_list[0]
                    mime = str(imageinfo.get("mime") or "")
                    if not mime.startswith("image/"):
                        continue

                    img_url = imageinfo.get("url")
                    if not img_url or img_url in seen_urls or str(pageid) in seen_pageids:
                        continue

                    ok_meta, reason = prefilter_imageinfo(imageinfo, filter_cfg)
                    if not ok_meta:
                        write_jsonl(retry_log, {
                            "event": "candidate_prefilter_reject",
                            "category": cat,
                            "keyword": kw,
                            "stage": stage,
                            "url": img_url,
                            "result": reason,
                        })
                        continue

                    license_type, license_url = extract_license_fields(imageinfo)
                    if not allow_license(license_type, allowlist):
                        continue

                    source_page_url = imageinfo.get("descriptionurl") or page.get("fullurl") or ""
                    ext = imageinfo.get("extmetadata") or {}
                    author = clean_text((ext.get("Artist") or {}).get("value"))
                    seen_urls.add(img_url)
                    seen_pageids.add(str(pageid))
                    page_candidates += 1

                    pending.append({
                        "img_url": img_url,
                        "source_page_url": source_page_url,
                        "license_type": license_type,
                        "license_url": license_url,
                        "author": author,
                        "kw": kw,
                        "stage": stage,
                        "attempt_count": 1,
                        "next_retry_ts": 0.0,
                        "accum_wait_sec": 0.0,
                        "last_status_code": None,
                    })

                if pending:
                    with ThreadPoolExecutor(max_workers=download_workers) as ex:
                        futures = [ex.submit(handle_download_candidate, cand, False) for cand in pending]
                        for fut in as_completed(futures):
                            res = fut.result()
                            page_downloaded += int(res["downloaded"] > 0)
                            if res["filter_passed"]:
                                page_filter_passed += 1
                            if res["accepted"]:
                                page_accepted += 1

                deferred_accepted = process_deferred_queue()
                page_accepted += deferred_accepted

                cumulative_stats["candidates"] += page_candidates
                cumulative_stats["downloaded"] += page_downloaded
                cumulative_stats["filter_passed"] += page_filter_passed
                cumulative_stats["accepted"] += page_accepted

                next_continue = (data.get("continue") or {}).get("gsrcontinue")
                print(
                    f"[Wikicommons][{cat}:{kw}][{stage}] page_stats "
                    f"candidates={page_candidates} downloaded={page_downloaded} "
                    f"filter_passed={page_filter_passed} accepted={page_accepted} deferred_pending={len(deferred_urls)}"
                )
                flush_progress()
                monitor.maybe_log(cat)
                adapt_download_throttle()
                return page_accepted, next_continue, True

            with tqdm(total=target, initial=min(saved, target), desc=f"WCM {cat}", unit="img") as pbar:
                while saved < primary_target:
                    accepted, primary_continue, has_pages = process_keyword_page(primary_kw, primary_continue, "primary")
                    if accepted > 0:
                        pbar.update(accepted)
                    if not has_pages or not primary_continue:
                        break

                if saved < target and secondary_kws:
                    if secondary_mode != "round_robin":
                        print(f"[Wikicommons] unsupported secondary_mode={secondary_mode}, fallback to round_robin")
                    if not secondary_continue_map:
                        secondary_continue_map = {kw: None for kw in secondary_kws}
                    else:
                        for kw in secondary_kws:
                            secondary_continue_map.setdefault(kw, None)
                    exhausted_kws.intersection_update(set(secondary_kws))

                    while saved < target and len(exhausted_kws) < len(secondary_kws):
                        for kw in secondary_kws:
                            if saved >= target:
                                break
                            if kw in exhausted_kws:
                                continue

                            accepted, next_continue, has_pages = process_keyword_page(kw,
                                                                                      secondary_continue_map.get(kw),
                                                                                      "secondary")

                            if accepted > 0:
                                pbar.update(accepted)

                            if not has_pages or not next_continue:
                                exhausted_kws.add(kw)
                            else:
                                secondary_continue_map[kw] = next_continue

                            flush_progress()

                    if (
                        saved < target
                        and allow_cross_keyword_backfill
                        and (not stop_when_exhausted or len(exhausted_kws) >= len(secondary_kws))
                    ):
                        backfill_keywords = [kw for kw in build_backfill_keywords(cat, keywords) if kw not in set(keywords)]
                        for backfill_kw in backfill_keywords:
                            if saved >= target:
                                break
                            continue_token: Optional[str] = None
                            while saved < target:
                                accepted, continue_token, has_pages = process_keyword_page(backfill_kw, continue_token, "secondary")
                                if accepted > 0:
                                    pbar.update(accepted)
                                if not has_pages or not continue_token:
                                    exhausted_kws.add(backfill_kw)
                                    break

                while saved < target and deferred_urls:
                    accepted = process_deferred_queue()
                    if accepted > 0:
                        pbar.update(accepted)
                        flush_progress()
                    if accepted == 0:
                        next_retry_ts = min(item.get("next_retry_ts", time.time()) for item in deferred_urls)
                        sleep_sec = max(0.0, min(max_retry_wait_sec, next_retry_ts - time.time()))
                        if sleep_sec <= 0:
                            break
                        time.sleep(sleep_sec)

                for item in deferred_urls:
                    write_jsonl(retry_log, {
                        "event": "download_final_drop",
                        "category": cat,
                        "keyword": item["kw"],
                        "stage": item["stage"],
                        "url": item["img_url"],
                        "status_code": item.get("last_status_code"),
                        "attempt_count": item["attempt_count"],
                        "accum_wait_sec": round(item["accum_wait_sec"], 3),
                        "result": "dropped_unfinished",
                    })
                flush_progress()

    print("\n[Wikicommons] done.")


if __name__ == "__main__":
    main()
