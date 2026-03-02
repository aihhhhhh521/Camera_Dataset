from __future__ import annotations
import time
import requests
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from tqdm import tqdm

from utils_fs import ensure_dirs, next_id, save_image_and_metadata
from pipeline_filter import PipelineFilter, sha256_bytes, exif_extract
from hash_db import init_db, has_sha256, add_sha256

FLICKR_REST = "https://api.flickr.com/services/rest/"

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
    r = requests.get(FLICKR_REST, params=base, headers={"User-Agent": ua}, timeout=30)
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
    for k in ["url_o", "url_l", "url_c", "url_z"]:
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
        except Exception:
            continue
    return best

def download(url: str, ua: str, timeout: int, max_retries: int) -> bytes:
    last_err = None
    for _ in range(max_retries):
        try:
            r = requests.get(url, headers={"User-Agent": ua}, timeout=timeout)
            r.raise_for_status()
            return r.content
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    raise RuntimeError(f"download failed: {url} err={last_err}")

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
    pf = PipelineFilter(cfg["pipeline_filter"])

    prefixes = cfg["naming"]["prefixes"]
    digits = int(cfg["naming"]["digits"])

    fcfg = cfg["flickr"]
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
                        "license": ",".join(map(str, allowed_license_ids)),  # flickr.photos.search 支持多个 license id :contentReference[oaicite:12]{index=12}
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
                        if not url:
                            # 兜底：getSizes 再查一次最大图
                            time.sleep(cfg["download"]["sleep_sec"])
                            url = get_sizes_best(api_key, photo_id, ua)
                        if not url:
                            continue

                        time.sleep(cfg["download"]["sleep_sec"])
                        try:
                            b = download(
                                url,
                                ua=ua,
                                timeout=int(cfg["download"]["timeout_sec"]),
                                max_retries=int(cfg["download"]["max_retries"]),
                            )
                        except Exception:
                            continue

                        s256 = sha256_bytes(b)
                        if has_sha256(dataset_root, s256):
                            continue

                        ok, metrics, reason = pf.validate(b)
                        if not ok:
                            continue

                        image_id = next_id(dataset_root, prefixes["flickr"], digits)
                        exif_data = exif_extract(b)

                        # Flickr 照片页
                        owner = ph.get("owner", "")
                        photo_page = f"https://www.flickr.com/photos/{owner}/{photo_id}/"

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