import json
import os
from pathlib import Path
from typing import Dict, Tuple

def ensure_dirs(dataset_root: str, categories: Dict[str, dict]) -> None:
    root = Path(dataset_root)
    (root / "raw_images").mkdir(parents=True, exist_ok=True)
    (root / "metadata").mkdir(parents=True, exist_ok=True)
    (root / "metadata" / "_cache").mkdir(parents=True, exist_ok=True)

    for cat in categories.keys():
        (root / "raw_images" / cat).mkdir(parents=True, exist_ok=True)

def counters_path(dataset_root: str) -> str:
    return str(Path(dataset_root) / "metadata" / "_cache" / "counters.json")

def load_counters(dataset_root: str) -> Dict[str, int]:
    p = counters_path(dataset_root)
    if not os.path.exists(p):
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def save_counters(dataset_root: str, counters: Dict[str, int]) -> None:
    p = counters_path(dataset_root)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(counters, f, ensure_ascii=False, indent=2)

def next_id(dataset_root: str, prefix: str, digits: int) -> str:
    counters = load_counters(dataset_root)
    n = int(counters.get(prefix, 0)) + 1
    counters[prefix] = n
    save_counters(dataset_root, counters)
    return f"{prefix}_{n:0{digits}d}"

def save_image_and_metadata(
    dataset_root: str,
    category: str,
    image_id: str,
    image_bytes: bytes,
    metadata: dict,
) -> Tuple[str, str]:
    root = Path(dataset_root)
    img_path = root / "raw_images" / category / f"{image_id}.jpg"
    meta_path = root / "metadata" / f"{image_id}.json"

    with open(img_path, "wb") as f:
        f.write(image_bytes)

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    return str(img_path), str(meta_path)