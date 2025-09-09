"""Utility: run CHECKPOINT + VACUUM on durable DuckDB files offline.

Usage (from repository root):
    python feeder_server/data/app.d/scripts/vacuum_databases.py --dir "C:\path\to\persistence\hot"

Defaults to feeder_server/data/persistence/hot inside the repo if --dir not provided.

This script opens each .duckdb file it finds (except WAL files), runs CHECKPOINT then VACUUM and closes the connection.
It prints sizes before/after. Run with the server stopped to ensure handles are not held by other processes.
"""
from __future__ import annotations
import argparse
import duckdb
import pathlib
from typing import Optional
try:
    # prefer package PATHS when used from the app package
    from ..config.paths import PATHS
except Exception:
    PATHS = None


def human(n: int) -> str:
    return f"{n/1024/1024:.2f} MB"


def vacuum_file(path: pathlib.Path) -> None:
    print(f"\nProcessing: {path}")
    before = path.stat().st_size
    print(f"  size before: {human(before)}")
    con = None
    try:
        con = duckdb.connect(str(path))
        try:
            con.execute("CHECKPOINT")
        except Exception as e:
            print(f"  CHECKPOINT failed: {e!r}")
        try:
            con.execute("VACUUM")
        except Exception as e:
            print(f"  VACUUM failed: {e!r}")
    except Exception as e:
        print(f"  open failed: {e!r}")
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    try:
        after = path.stat().st_size
        print(f"  size after:  {human(after)}")
    except Exception:
        print("  could not stat file after vacuum")


def run_vacuum(dir: Optional[str] = None) -> None:
    """Run CHECKPOINT + VACUUM on .duckdb files in the given directory.

    When called programmatically, pass dir (string) or leave None to use the
    project's configured hot directory (if available via PATHS). This avoids
    calling argparse.parse_args() at import time.
    """
    if dir:
        target = pathlib.Path(dir)
    else:
        if PATHS is not None:
            target = PATHS.hot_root
        else:
            # Fallback: try to derive repo root safely
            repo_root = pathlib.Path(__file__).resolve().parents[3]
            target = repo_root / 'feeder_server' / 'data' / 'persistence' / 'hot'

    if not target.exists() or not target.is_dir():
        raise FileNotFoundError(f"Directory not found: {target}")

    files = sorted([p for p in target.glob('*.duckdb')])
    if not files:
        print(f"No .duckdb files found in {target}")
        return

    print(f"Found {len(files)} .duckdb files in {target}")
    for f in files:
        vacuum_file(f)

    print('\nDone. Ensure the server is stopped when running this script to allow file shrinkage.')


if __name__ == '__main__':
    run_vacuum(r"C:\Users\chica\Quant\projects\deepfeeder\feeder_server\data\persistence\hot")
    # p = argparse.ArgumentParser(description='VACUUM DuckDB files in a directory (offline).')
    # p.add_argument('--dir', '-d', default=None, help='Directory containing .duckdb files (defaults to persistence/hot in repo)')
    # args = p.parse_args()
    # run_vacuum(args.dir)
