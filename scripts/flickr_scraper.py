from __future__ import annotations
import json
import time
import random
import logging
import requests
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import yaml
from tqdm import tqdm

from utils_fs import ensure_dirs, next_id, save_image_and_metadata
from pipeline_filter import PipelineFilter, sha256_bytes, exif_extract
from hash_db import init_db, has_sha256, add_sha256

FLICKR_REST = "https://api.flickr.com/services/rest/"

_SESSION: Optional[requests.Session] = None
LOGGER = logging.getLogger(__name__)


def build_api_headers(user_agent: str) -> Dict[str, str]:
    return {"User-Agent": user_agent}


def build_download_headers(
        *,
        user_agent: str,
        headers_profile: str,
        referer: Optional[str] = None,
        referer_enabled: bool = True,
) -> Dict[str, str]:
    profiles: Dict[str, Dict[str, str]] = {
        "minimal": {},
        "browser_like": {
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        },
    }
    profile_headers = profiles.get(headers_profile, profiles["browser_like"]).copy()
    profile_headers["User-Agent"] = user_agent
    if referer_enabled and referer:
        profile_headers["Referer"] = referer
    return profile_headers


def _error_type(exc: Exception) -> str:
    if isinstance(exc, requests.Timeout):
        return "requests.Timeout"
    if isinstance(exc, requests.HTTPError):
        return "requests.HTTPError"
    return type(exc).__name__


def log_failure(
        *,
        photo_id: str,
        url: str,
        kw: str,
        page: int,
        error_type: str,
        message: str,
        log_path: Optional[Path] = None,
) -> None:
    payload: Dict[str, Any] = {
        "photo_id": photo_id,
        "url": url,
        "kw": kw,
        "page": page,
        "error_type": error_type,
        "message": message,
        "ts": int(time.time()),
    }
    print(
        "[Flickr][warn] "
        f"photo_id={photo_id} kw={kw} page={page} error_type={error_type} "
        f"url={url} msg={message}"
    )
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def load_cfg() -> dict:
    cfg_path = Path(__file__).with_name("config.yaml")
    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    dataset_root = Path(cfg.get("dataset_root", "."))
    if not dataset_root.is_absolute():
        cfg["dataset_root"] = str((cfg_path.parent / dataset_root).resolve())
    return cfg


def flickr_call(api_key: str, method: str, params: dict, ua: str) -> dict:
    base = {
        "method": method,
        "api_key": api_key,
        "format": "json",
        "nojsoncallback": 1,
    }
    base.update(params)
    r = requests.get(FLICKR_REST, params=base, headers=build_api_headers(ua), timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("stat") != "ok":
        raise RuntimeError(f"Flickr API error: {data}")
    return data


def get_license_map(api_key: str, ua: str) -> Dict[str, dict]:
    data = flickr_call(api_key, "flickr.photos.licenses.getInfo", {}, ua)
    m = {}
    for it in data["licenses"]["license"]:
        # it: {id, name, url}
        m[it["id"]] = it
    return m


def is_nd_license(name: str) -> bool:
    # Flickr license name 一般会包含 "NoDerivs" 字样
    n = name.lower()
    return ("noderiv" in n) or ("no deriv" in n) or ("nd" in n and "cc" in n)


def choose_allowed_license_ids(license_map: Dict[str, dict], mode: str) -> List[str]:
    """
    mode=cc0_only: 只取 Public Domain / CC0
    mode=allowlist: 取所有 CC / Public Domain（排除 ND 和 All Rights Reserved）
    """
    ids: List[str] = []

    for lid, info in license_map.items():
        lid_str = str(lid)
        name = (info.get("name") or "").lower()
        url = (info.get("url") or "").lower()

        # 1) 永远排除 All Rights Reserved
        if "all rights reserved" in name:
            continue

        # 2) 永远排除 ND
        if is_nd_license(name):
            continue

        # 3) 只保留 CC / Public Domain（避免混入非开放许可）
        if ("creativecommons.org" not in url) and ("publicdomain" not in url):
            continue

        if mode == "cc0_only":
            # 只保留 CC0 / Public Domain
            if ("cc0" in name) or ("public domain" in name) or ("zero" in name) or ("publicdomain" in url):
                ids.append(lid_str)
        else:
            # allowlist：允许 CC BY / BY-SA / BY-NC / BY-NC-SA / CC0 / PDM（仍已排除 ND）
            ids.append(lid_str)

    return ids


def best_download_url(photo: dict) -> Optional[str]:
    # extras 里如果有 url_o（原图）就用，否则降级
    for k in ["url_b", "url_c", "url_l", "url_o", "url_z"]:
        if k in photo:
            return photo[k]
    return None


def get_sizes_best(api_key: str, photo_id: str, ua: str) -> Optional[str]:
    data = flickr_call(api_key, "flickr.photos.getSizes", {"photo_id": photo_id}, ua)
    sizes = data["sizes"]["size"]
    # 按面积选最大
    best = None
    best_area = -1
    for s in sizes:
        try:
            w = int(s["width"])
            h = int(s["height"])
            area = w * h
            if area > best_area:
                best_area = area
                best = s.get("source")
        except (TypeError, ValueError, KeyError):
            continue
    return best


def get_session() -> requests.Session:
    """复用 Session，并预热获取 Flickr CDN Cookie"""
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        # 预热：访问 Flickr 首页获取 session cookie，模拟真实浏览器行为
        try:
            _SESSION.get("https://www.flickr.com/", timeout=10)
            time.sleep(random.uniform(1.0, 2.0))
        except Exception:
            pass
    return _SESSION


def download(url: str, headers: Dict[str, str], timeout: int, max_retries: int) -> bytes:
    global _SESSION
    session = get_session()
    last_err = None

    for attempt in range(max_retries):
        try:
            referer = headers.get("Referer", "")
            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug(
                    "download request referer_host=%s url_host=%s url=%s",
                    urlparse(referer).netloc if referer else "",
                    urlparse(url).netloc,
                    url,
                )
            r = session.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.content

        except requests.HTTPError as e:
            last_err = e
            status = e.response.status_code if e.response is not None else 0

            if status == 429:
                retry_after = e.response.headers.get("Retry-After")
                if retry_after:
                    wait = float(retry_after)
                else:
                    # 指数退避 + 随机抖动，避免规律性请求
                    base_wait = 10.0 * (3 ** attempt)
                    wait = base_wait + random.uniform(0, base_wait * 0.3)
                print(f"[Flickr][429] sleeping {wait:.1f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
                # 429 后重建 session，刷新连接状态
                _SESSION = None
                print("[Flickr][429] session reset done")
            else:
                time.sleep(random.uniform(0.3, 1.0))

        except (requests.Timeout, requests.RequestException) as e:
            last_err = e
            time.sleep(random.uniform(0.5, 1.5))

    if last_err is not None:
        raise last_err
    raise RuntimeError(f"download failed: {url}")


def build_metadata(
        image_id: str,
        category: str,
        original_url: str,
        photo_page_url: str,
        author: str,
        license_type: str,
        license_url: str,
        search_keyword: str,
        resolution_hw: tuple[int, int],
        exif_data: dict,
        pipeline_metrics: dict,
) -> dict:
    return {
        "image_id": image_id,
        "category": category,
        "source_platform": "Flickr",
        "original_url": original_url,
        "source_page_url": photo_page_url,
        "author": author,
        "license_type": license_type,
        "license_url": license_url,
        "search_keyword": search_keyword,
        "resolution": [resolution_hw[1], resolution_hw[0]],
        "exif_data": exif_data,
        "pipeline_metrics": pipeline_metrics,
    }


def main():
    cfg = load_cfg()
    dataset_root = cfg["dataset_root"]
    ensure_dirs(dataset_root, cfg["categories"])
    init_db(dataset_root)

    ua = cfg["download"]["user_agent"]
    headers_profile = cfg["download"].get("headers_profile", "browser_like")
    referer_enabled = bool(cfg["download"].get("referer_enabled", True))
    pf = PipelineFilter(cfg["pipeline_filter"])

    prefixes = cfg["naming"]["prefixes"]
    digits = int(cfg["naming"]["digits"])

    fcfg = cfg["flickr"]
    fail_log_path = Path("logs/flickr_failures.jsonl")
    breaker_threshold = int(fcfg.get("consecutive_fail_threshold", 8))
    breaker_sleep_sec = float(fcfg.get("consecutive_fail_sleep_sec", 5))
    consecutive_download_failures = 0
    api_key = fcfg.get("api_key", "").strip()
    if not api_key:
        raise RuntimeError("Flickr api_key is empty. Fill scripts/config.yaml -> flickr.api_key")

    license_map = get_license_map(api_key, ua)
    allowed_license_ids = choose_allowed_license_ids(license_map, cfg["licenses"]["mode"])

    print(f"[Flickr] allowed_license_ids count={len(allowed_license_ids)}")
    if not allowed_license_ids:
        raise RuntimeError("No allowed Flickr license IDs found. Check cc0_only matching logic.")

    for cat, spec in cfg["categories"].items():
        target = int(spec.get("target_count", 0))
        keywords = spec.get("keywords", [])
        saved = 0

        print(f"\n[Flickr] category={cat} target={target} keywords={keywords}")

        for kw in keywords:
            if saved >= target:
                break
            page = 1

            with tqdm(total=target, initial=saved, desc=f"FLK {cat}:{kw}", unit="img") as pbar:
                while saved < target:
                    params = {
                        "text": kw,
                        "page": page,
                        "per_page": int(fcfg.get("per_page", 250)),
                        "sort": fcfg.get("sort", "relevance"),
                        "safe_search": int(fcfg.get("safe_search", 1)),
                        "content_type": int(fcfg.get("content_type", 1)),
                        "media": "photos",
                        "license": ",".join(map(str, allowed_license_ids)),
                        # flickr.photos.search 支持多个 license id :contentReference[oaicite:12]{index=12}
                        "extras": ",".join([
                            "license",
                            "owner_name",
                            "url_o", "url_l", "url_c", "url_z",
                            "o_dims",
                            "date_taken",
                            "tags",
                        ]),
                    }
                    data = flickr_call(api_key, "flickr.photos.search", params, ua)
                    photos = data["photos"]["photo"]
                    if not photos:
                        break

                    for ph in photos:
                        if saved >= target:
                            break

                        photo_id = ph["id"]
                        # 许可与落地页
                        lid = str(ph.get("license", ""))
                        lic_info = license_map.get(lid, {})
                        lic_name = lic_info.get("name", "")
                        lic_url = lic_info.get("url", "")

                        if cfg["licenses"]["mode"] == "cc0_only":
                            # 再保险：只要不是 PD/CC0 就跳过（因为文本匹配可能宽松）
                            nm = (lic_name or "").lower()
                            if not (("public domain" in nm) or ("cc0" in nm) or ("zero" in nm)):
                                continue

                        # 下载 URL
                        url = best_download_url(ph)
                        owner = ph.get("owner", "")
                        photo_page = f"https://www.flickr.com/photos/{owner}/{photo_id}/"
                        if not url:
                            # 兜底：getSizes 再查一次最大图
                            base = float(cfg["download"]["sleep_sec"])
                            time.sleep(base + random.uniform(0, base * 0.5))
                            try:
                                url = get_sizes_best(api_key, photo_id, ua)
                            except requests.Timeout as e:
                                log_failure(
                                    photo_id=photo_id,
                                    url="",
                                    kw=kw,
                                    page=page,
                                    error_type=_error_type(e),
                                    message=str(e),
                                    log_path=fail_log_path,
                                )
                                continue
                            except requests.HTTPError as e:
                                log_failure(
                                    photo_id=photo_id,
                                    url="",
                                    kw=kw,
                                    page=page,
                                    error_type=_error_type(e),
                                    message=str(e),
                                    log_path=fail_log_path,
                                )
                                continue
                            except Exception as e:
                                log_failure(
                                    photo_id=photo_id,
                                    url="",
                                    kw=kw,
                                    page=page,
                                    error_type=_error_type(e),
                                    message=str(e),
                                    log_path=fail_log_path,
                                )
                                continue
                        if not url:
                            continue

                        base = float(cfg["download"]["sleep_sec"])
                        time.sleep(base + random.uniform(0, base * 0.5))
                        b = None
                        try:
                            b = download(
                                url,
                                headers=build_download_headers(
                                    user_agent=ua,
                                    headers_profile=headers_profile,
                                    referer=photo_page,
                                    referer_enabled=referer_enabled,
                                ),
                                timeout=int(cfg["download"]["timeout_sec"]),
                                max_retries=int(cfg["download"]["max_retries"]),
                            )
                        except requests.Timeout as e:
                            consecutive_download_failures += 1
                            log_failure(
                                photo_id=photo_id,
                                url=url,
                                kw=kw,
                                page=page,
                                error_type=_error_type(e),
                                message=str(e),
                                log_path=fail_log_path,
                            )
                        except requests.HTTPError as e:
                            consecutive_download_failures += 1
                            log_failure(
                                photo_id=photo_id,
                                url=url,
                                kw=kw,
                                page=page,
                                error_type=_error_type(e),
                                message=str(e),
                                log_path=fail_log_path,
                            )
                        except Exception as e:
                            consecutive_download_failures += 1
                            log_failure(
                                photo_id=photo_id,
                                url=url,
                                kw=kw,
                                page=page,
                                error_type=_error_type(e),
                                message=str(e),
                                log_path=fail_log_path,
                            )
                        else:
                            consecutive_download_failures = 0

                        if consecutive_download_failures >= breaker_threshold:
                            print(
                                "[Flickr][breaker] "
                                f"consecutive_download_failures={consecutive_download_failures} "
                                f"sleep={breaker_sleep_sec}s"
                            )
                            time.sleep(breaker_sleep_sec)
                            consecutive_download_failures = 0

                        if b is None:
                            continue

                        try:
                            ok, metrics, reason = pf.validate(b)
                        except requests.Timeout as e:
                            log_failure(
                                photo_id=photo_id,
                                url=url,
                                kw=kw,
                                page=page,
                                error_type=_error_type(e),
                                message=str(e),
                                log_path=fail_log_path,
                            )
                            continue
                        except requests.HTTPError as e:
                            log_failure(
                                photo_id=photo_id,
                                url=url,
                                kw=kw,
                                page=page,
                                error_type=_error_type(e),
                                message=str(e),
                                log_path=fail_log_path,
                            )
                            continue
                        except Exception as e:
                            log_failure(
                                photo_id=photo_id,
                                url=url,
                                kw=kw,
                                page=page,
                                error_type=_error_type(e),
                                message=str(e),
                                log_path=fail_log_path,
                            )
                            continue
                        s256 = sha256_bytes(b)
                        if has_sha256(dataset_root, s256):
                            continue

                        if not ok:
                            continue

                        image_id = next_id(dataset_root, prefixes["flickr"], digits)
                        exif_data = exif_extract(b)

                        h = int(metrics.get("H", 0))
                        w = int(metrics.get("W", 0))

                        md = build_metadata(
                            image_id=image_id,
                            category=cat,
                            original_url=url,
                            photo_page_url=photo_page,
                            author=ph.get("ownername", ""),
                            license_type=lic_name,
                            license_url=lic_url,
                            search_keyword=kw,
                            resolution_hw=(h, w),
                            exif_data=exif_data,
                            pipeline_metrics=metrics,
                        )

                        save_image_and_metadata(dataset_root, cat, image_id, b, md)
                        add_sha256(dataset_root, s256, image_id, "flickr")

                        saved += 1
                        pbar.update(1)

                    page += 1

    print("\n[Flickr] done.")


if __name__ == "__main__":
    main()
