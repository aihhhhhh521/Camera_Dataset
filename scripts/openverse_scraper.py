from __future__ import annotations
import os
import time
import requests
from typing import Dict, Any, List, Optional
from pathlib import Path

import yaml
from tqdm import tqdm

from utils_fs import ensure_dirs, next_id, save_image_and_metadata
from pipeline_filter import PipelineFilter, sha256_bytes, exif_extract
from hash_db import init_db, has_sha256, add_sha256

def load_cfg() -> dict:
    cfg_path = Path(__file__).with_name("config.yaml")
    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    dataset_root = Path(cfg.get("dataset_root", "."))
    if not dataset_root.is_absolute():
        cfg["dataset_root"] = str((cfg_path.parent / dataset_root).resolve())

    # 允许通过环境变量覆盖 Openverse 凭据，避免把密钥写入配置文件。
    ov_cfg = cfg.setdefault("openverse", {})
    ov_cfg["client_id"] = os.getenv("OPENVERSE_CLIENT_ID", ov_cfg.get("client_id", ""))
    ov_cfg["client_secret"] = os.getenv("OPENVERSE_CLIENT_SECRET", ov_cfg.get("client_secret", ""))
    return cfg

def get_token(base_url: str, client_id: str, client_secret: str, ua: str) -> Optional[str]:
    """
    Openverse 官方 JS client 支持 clientId/clientSecret 自动换 token。:contentReference[oaicite:8]{index=8}
    这里提供一个“可选”token获取：你没有凭据就返回 None（匿名跑）。
    """
    client_id = (client_id or "").strip()
    client_secret = (client_secret or "").strip()

    if not client_id or not client_secret:
        return None

    token_url = base_url.rstrip("/") + "/auth_tokens/token/"
    headers = {"User-Agent": ua}

    # 兼容不同 OAuth 服务端实现：优先标准 OAuth2 form，再尝试 Basic Auth 方案。
    attempts = [
        {
            "name": "oauth2_form",
            "kwargs": {
                "data": {
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                }
            },
        },
        {
            "name": "oauth2_basic_auth",
            "kwargs": {
                "data": {"grant_type": "client_credentials"},
                "auth": (client_id, client_secret),
            },
        },
    ]

    for idx, attempt in enumerate(attempts, start=1):
        r = requests.post(token_url, headers=headers, timeout=30, **attempt["kwargs"])
        if r.status_code != 200:
            print(
                f"[WARN] Openverse token attempt#{idx} ({attempt['name']}) failed: "
                f"{r.status_code} {r.text[:200]}"
            )
            continue

        token = (r.json().get("access_token") or "").strip()
        if token:
            return token
        print(f"[WARN] Openverse token attempt#{idx} succeeded but access_token is empty")

    return None

def ov_search(
    base_url: str,
    q: str,
    page: int,
    page_size: int,
    license_list: List[str],
    sources: List[str],
    token: Optional[str],
    ua: str
) -> Dict[str, Any]:
    url = base_url.rstrip("/") + "/images/"
    params = {
        "q": q,
        "page": page,
        "page_size": page_size,
    }
    # Openverse license 参数可用于过滤；我们这里默认 cc0_only。:contentReference[oaicite:9]{index=9}
    if license_list:
        params["license"] = ",".join(license_list)
    if sources:
        # Openverse 支持 source 过滤；文档示例中包含 source=flickr 等。:contentReference[oaicite:10]{index=10}
        params["source"] = ",".join(sources)

    headers = {"User-Agent": ua}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def ov_search_with_retry(
    base_url: str,
    q: str,
    page: int,
    page_size: int,
    license_list: List[str],
    sources: List[str],
    token: Optional[str],
    ua: str,
    client_id: str,
    client_secret: str,
) -> tuple[Dict[str, Any], Optional[str]]:
    """
    对 Openverse 查询做一次 401 自动恢复：
    - 优先按当前 token 请求。
    - 401 时尝试刷新 token 并重试。
    - 若仍 401，自动回退匿名请求（不带 Authorization），避免因应用权限配置问题直接中断。
    """
    def _anonymous_fallback(hint: str) -> tuple[Dict[str, Any], Optional[str]]:
        try:
            data = ov_search(
                base_url=base_url,
                q=q,
                page=page,
                page_size=page_size,
                license_list=license_list,
                sources=sources,
                token=None,
                ua=ua,
            )
            print(f"[WARN] {hint} 已自动回退到匿名请求并继续。")
            return data, None
        except requests.HTTPError as e3:
            status3 = e3.response.status_code if e3.response is not None else None
            if status3 == 401:
                raise RuntimeError(
                    "Openverse 认证请求与匿名请求均返回 401。"
                    "请检查本机网络/代理是否改写了请求头，"
                    "并确认 Openverse API 在当前网络可直连。"
                ) from e3
            raise

    try:
        return ov_search(
            base_url=base_url,
            q=q,
            page=page,
            page_size=page_size,
            license_list=license_list,
            sources=sources,
            token=token,
            ua=ua,
        ), token
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if status != 401:
            raise

        if token or (client_id and client_secret):
            new_token = get_token(base_url, client_id, client_secret, ua)
            if not new_token:
                return _anonymous_fallback(
                    "Openverse 返回 401，且 token 刷新失败。"
                    "请检查 openverse.client_id / client_secret 是否有效。",
                )
            try:
                data = ov_search(
                    base_url=base_url,
                    q=q,
                    page=page,
                    page_size=page_size,
                    license_list=license_list,
                    sources=sources,
                    token=new_token,
                    ua=ua,
                )
                return data, new_token
            except requests.HTTPError as e2:
                status2 = e2.response.status_code if e2.response is not None else None
                if status2 == 401:
                    return _anonymous_fallback(
                        "Openverse token 获取成功但访问 /images/ 仍返回 401。"
                        "这通常表示 client_id/client_secret 对应应用未获得该 API 访问权限，"
                        "或当前凭据已失效（请在 Openverse 控制台重置并重新申请权限）。",
                    )
                raise

        return _anonymous_fallback(
            "Openverse API 返回 401 Unauthorized，当前未使用可用凭据。",
        )


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
    source_platform: str,
    original_url: str,
    author: str,
    license_type: str,
    search_keyword: str,
    resolution_hw: tuple[int, int],
    exif_data: dict,
    pipeline_metrics: dict,
    source_page_url: str,
    license_url: str,
) -> dict:
    return {
        "image_id": image_id,
        "category": category,
        "source_platform": source_platform,
        "original_url": original_url,
        "source_page_url": source_page_url,
        "author": author,
        "license_type": license_type,
        "license_url": license_url,
        "search_keyword": search_keyword,
        "resolution": [resolution_hw[1], resolution_hw[0]],  # [W,H] 更贴近你示例
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

    ov_cfg = cfg["openverse"]
    base_url = ov_cfg["base_url"]

    # 许可策略：默认 cc0_only（最稳）
    if cfg["licenses"]["mode"] == "cc0_only":
        license_list = ["cc0"]
    else:
        license_list = cfg["licenses"].get("allowlist_openverse", ["cc0"])

    token = get_token(base_url, ov_cfg.get("client_id", ""), ov_cfg.get("client_secret", ""), ua)
    client_id = ov_cfg.get("client_id", "")
    client_secret = ov_cfg.get("client_secret", "")
    sources = ov_cfg.get("sources", []) or []

    for cat, spec in cfg["categories"].items():
        target = int(spec.get("target_count", 0))
        keywords = spec.get("keywords", [])
        saved = 0

        print(f"\n[Openverse] category={cat} target={target} keywords={keywords}")

        for kw in keywords:
            if saved >= target:
                break
            page = 1

            with tqdm(total=target, initial=saved, desc=f"OV {cat}:{kw}", unit="img") as pbar:
                while saved < target:
                    data, token = ov_search_with_retry(
                        base_url=base_url,
                        q=kw,
                        page=page,
                        page_size=200,
                        license_list=license_list,
                        sources=sources,
                        token=token,
                        ua=ua,
                        client_id=client_id,
                        client_secret=client_secret,
                    )
                    results = data.get("results", [])
                    if not results:
                        break

                    for it in results:
                        if saved >= target:
                            break

                        img_url = it.get("url")  # Openverse 返回可下载的 url
                        if not img_url:
                            continue

                        # 下载
                        time.sleep(cfg["download"]["sleep_sec"])
                        try:
                            b = download(
                                img_url,
                                ua=ua,
                                timeout=int(cfg["download"]["timeout_sec"]),
                                max_retries=int(cfg["download"]["max_retries"]),
                            )
                        except Exception:
                            continue

                        s256 = sha256_bytes(b)
                        if has_sha256(dataset_root, s256):
                            continue

                        # pipeline filter
                        ok, metrics, reason = pf.validate(b)
                        if not ok:
                            continue

                        image_id = next_id(dataset_root, prefixes["openverse"], digits)

                        # exif
                        exif_data = exif_extract(b)

                        # metadata
                        h = int(metrics.get("H", 0))
                        w = int(metrics.get("W", 0))
                        md = build_metadata(
                            image_id=image_id,
                            category=cat,
                            source_platform="Openverse",
                            original_url=img_url,
                            author=it.get("creator") or "",
                            license_type=f"{it.get('license','')}".upper(),
                            search_keyword=kw,
                            resolution_hw=(h, w),
                            exif_data=exif_data,
                            pipeline_metrics=metrics,
                            source_page_url=it.get("foreign_landing_url") or "",
                            license_url=it.get("license_url") or "",
                        )

                        save_image_and_metadata(dataset_root, cat, image_id, b, md)
                        add_sha256(dataset_root, s256, image_id, "openverse")

                        saved += 1
                        pbar.update(1)

                    page += 1

    print("\n[Openverse] done.")

if __name__ == "__main__":
    main()