import sqlite3
from pathlib import Path

def db_path(dataset_root: str) -> str:
    return str(Path(dataset_root) / "metadata" / "_cache" / "hashes.sqlite")

def init_db(dataset_root: str) -> None:
    p = db_path(dataset_root)
    con = sqlite3.connect(p)
    try:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hashes (
              sha256 TEXT PRIMARY KEY,
              image_id TEXT,
              provider TEXT
            );
            """
        )
        con.commit()
    finally:
        con.close()

def has_sha256(dataset_root: str, sha256: str) -> bool:
    con = sqlite3.connect(db_path(dataset_root))
    try:
        cur = con.cursor()
        cur.execute("SELECT 1 FROM hashes WHERE sha256 = ? LIMIT 1;", (sha256,))
        return cur.fetchone() is not None
    finally:
        con.close()

def add_sha256(dataset_root: str, sha256: str, image_id: str, provider: str) -> None:
    con = sqlite3.connect(db_path(dataset_root))
    try:
        cur = con.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO hashes (sha256, image_id, provider) VALUES (?, ?, ?);",
            (sha256, image_id, provider),
        )
        con.commit()
    finally:
        con.close()