# app.d/config/paths.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import os

def _env_path(name: str, default: Path) -> Path:
    v = os.getenv(name)
    return Path(v).expanduser() if v else default

@dataclass(frozen=True)
class Paths:
    data_root: Path
    persist_root: Path
    hot_root: Path
    meta_root: Path
    event_store_db: Path
    journal_db: Path

    def hot_dir(self, provider: str, stream: str) -> Path:
        """Directory for a provider/stream 'hot' data (flat or nested layout)."""
        layout = os.getenv("DEEPFEEDER_HOT_LAYOUT", "flat").lower()
        if layout == "nested":
            return (self.hot_root / provider / stream)
        return (self.hot_root / f"{provider}_{stream}")  # default, matches your main branch

    def ensure(self) -> None:
        # Create all base dirs and DB parent dirs (no-op if they exist)
        for p in {self.data_root, self.persist_root, self.hot_root, self.meta_root,
                  self.event_store_db.parent, self.journal_db.parent}:
            p.mkdir(parents=True, exist_ok=True)

def build_paths() -> Paths:
    # Base roots (allow legacy env + new, more specific envs)
    data_root   = _env_path("DEEPNODE_DATA_ROOT", Path("/data"))
    persist_root = _env_path("DEEPFEEDER_PERSIST_ROOT", data_root / "persistence")
    hot_root     = _env_path("DEEPFEEDER_HOT_ROOT", persist_root / "hot")
    meta_root    = _env_path("DEEPFEEDER_META_ROOT", persist_root / "meta")

    # DB files (override if desired)
    event_store_db = _env_path("DEEPFEEDER_EVENT_STORE_DB", hot_root / "event_store.duckdb")
    journal_db     = _env_path("DEEPFEEDER_JOURNAL_DB",     hot_root / "journal.duckdb")

    paths = Paths(
        data_root=data_root,
        persist_root=persist_root,
        hot_root=hot_root,
        meta_root=meta_root,
        event_store_db=event_store_db,
        journal_db=journal_db,
    )
    paths.ensure()
    return paths

# Singleton-ish accessor
PATHS = build_paths()
