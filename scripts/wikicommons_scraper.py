from __future__ import annotations

import html
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import yaml
from tqdm import tqdm

from hash_db import add_sha256, has_sha256, init_db
from pipeline_filter import PipelineFilter, exif_extract, sha256_bytes
from utils_fs import ensure_dirs, next_id, save_image_and_metadata


DEFAULT_API_URL = "https://commons.wikimedia.org/w/api.php"

class RateLimiter:
    def __init__(self, min_interval_sec: float) -> None:
        self.min_interval_sec = max(0.0, float(min_interval_sec))
        self._last_global = 0.0
        self._last_by_domain: Dict[str, float] = {}

    def wait(self, url: str) -> None:
        if self.min_interval_sec <= 0:
            return

        now = time.monotonic()
        host = urlparse(url).netloc or "global"
        last_domain = self._last_by_domain.get(host, 0.0)
        wait_sec = max(
            0.0,
            self.min_interval_sec - (now - self._last_global),
            self.min_interval_sec - (now - last_domain),
        )
        if wait_sec > 0:
            time.sleep(wait_sec)

        ts = time.monotonic()
        self._last_global = ts
        self._last_by_domain[host] = ts

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

    if rate_limiter:
        rate_limiter.wait(api_url)

    r = requests.get(api_url, params=params, headers={"User-Agent": ua}, timeout=30)
    r.raise_for_status()
    return r.json()


def allow_license(license_type: str, allowlist: List[str]) -> bool:
    if not allowlist:
        return True

    normalized = normalize_license(license_type)
    for allowed in allowlist:
        token = normalize_license(allowed)
        if token and token in normalized:
            return True
    return False


def download(url: str, ua: str, timeout: int, max_retries: int, rate_limiter: Optional[RateLimiter], min_interval_sec: float) -> bytes:
    last_err: Optional[Exception] = None
    for retry in range(max_retries):
        try:
            if rate_limiter:
                rate_limiter.wait(url)
            r = requests.get(url, headers={"User-Agent": ua}, timeout=timeout)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                wait_sec = max(min_interval_sec, 0.5 * (2 ** retry))
                print(f"[Wikicommons] retry status={r.status_code} wait={wait_sec:.2f}s url={url}")
                time.sleep(wait_sec)
                last_err = RuntimeError(f"http {r.status_code}")
                continue
            r.raise_for_status()
            return r.content
        except Exception as e:
            last_err = e
            time.sleep(max(min_interval_sec, 0.5 * (2**retry)))

    raise RuntimeError(f"download failed: {url} err={last_err}")


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
    min_interval_sec = float(cfg["download"].get("sleep_sec", 0))
    rate_limiter = RateLimiter(min_interval_sec)
    pf = PipelineFilter(cfg["pipeline_filter"])

    prefixes = cfg["naming"]["prefixes"]
    digits = int(cfg["naming"]["digits"])

    wc_cfg = cfg["wikicommons"]
    api_url = wc_cfg.get("api_url", DEFAULT_API_URL)
    per_page = int(wc_cfg.get("per_page", 50))
    if cfg["licenses"].get("mode") == "cc0_only":
        allowlist = ["cc0", "public domain", "pdm"]
    else:
        allowlist = wc_cfg.get("license_allowlist") or cfg["licenses"].get("allowlist_wikicommons", []) or []

    for cat, spec in cfg["categories"].items():
        target = int(spec.get("target_count", 0))
        keywords = spec.get("keywords", [])
        saved = 0

        print(f"\n[Wikicommons] category={cat} target={target} keywords={keywords}")

        for kw in keywords:
            if saved >= target:
                break

            gsrcontinue: Optional[str] = None
            with tqdm(total=target, initial=saved, desc=f"WCM {cat}:{kw}", unit="img") as pbar:
                while saved < target:
                    data = search_commons(
                        api_url=api_url,
                        keyword=kw,
                        continue_token=gsrcontinue,
                        per_page=per_page,
                        ua=ua,
                        rate_limiter=rate_limiter,
                    )
                    pages = (data.get("query") or {}).get("pages") or {}
                    if not pages:
                        break

                    page_candidates = 0
                    page_downloaded = 0
                    page_filter_passed = 0

                    for _, page in pages.items():
                        if saved >= target:
                            break

                        imageinfo_list = page.get("imageinfo") or []
                        if not imageinfo_list:
                            continue

                        imageinfo = imageinfo_list[0]
                        mime = str(imageinfo.get("mime") or "")
                        if not mime.startswith("image/"):
                            continue
                        page_candidates += 1

                        img_url = imageinfo.get("url")
                        source_page_url = imageinfo.get("descriptionurl") or page.get("fullurl") or ""
                        if not img_url:
                            continue

                        license_type, license_url = extract_license_fields(imageinfo)
                        if not allow_license(license_type, allowlist):
                            continue

                        try:
                            b = download(
                                img_url,
                                ua=ua,
                                timeout=int(cfg["download"]["timeout_sec"]),
                                max_retries=int(cfg["download"]["max_retries"]),
                                rate_limiter=rate_limiter,
                                min_interval_sec=min_interval_sec,
                            )
                            page_downloaded += 1
                        except Exception:
                            continue

                        s256 = sha256_bytes(b)
                        if has_sha256(dataset_root, s256):
                            continue

                        ok, metrics, _ = pf.validate(b)
                        if not ok:
                            continue
                        page_filter_passed += 1

                        image_id = next_id(dataset_root, prefixes["wikicommons"], digits)
                        exif_data = exif_extract(b)

                        ext = imageinfo.get("extmetadata") or {}
                        author = clean_text((ext.get("Artist") or {}).get("value"))

                        h = int(metrics.get("H", 0))
                        w = int(metrics.get("W", 0))
                        md = build_metadata(
                            image_id=image_id,
                            category=cat,
                            original_url=img_url,
                            source_page_url=source_page_url,
                            author=author,
                            license_type=license_type,
                            license_url=license_url,
                            search_keyword=kw,
                            resolution_hw=(h, w),
                            exif_data=exif_data,
                            pipeline_metrics=metrics,
                        )

                        save_image_and_metadata(dataset_root, cat, image_id, b, md)
                        add_sha256(dataset_root, s256, image_id, "wikicommons")

                        saved += 1
                        pbar.update(1)

                    print(
                        f"[Wikicommons][{cat}:{kw}] page_stats "
                        f"candidates={page_candidates} downloaded={page_downloaded} filter_passed={page_filter_passed}"
                    )

                    gsrcontinue = (data.get("continue") or {}).get("gsrcontinue")
                    if not gsrcontinue:
                        break

    print("\n[Wikicommons] done.")


if __name__ == "__main__":
    main()
