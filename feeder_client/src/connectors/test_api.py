import json
import time
from contextlib import closing
from datetime import datetime, timezone
from urllib.parse import urlencode
from websocket import create_connection
from websocket._exceptions import WebSocketConnectionClosedException, WebSocketTimeoutException


def ns_to_iso(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).isoformat()


def main() -> None:
    provider = "tradingview"
    schema = "ohlcv_1m"
    symbol = "INDV2025"
    params = {
        # "since_ns": 1758549600000000000,
        "fields": "Timestamp,Open,High,Low,Close,Volume,Symbol,Exchange,BarId",
        "only_completed": "true",
    }
    qs = urlencode({k: v for k, v in params.items() if v is not None})
    base = f"ws://localhost:8083/v1/ws/{provider}/{schema}/{symbol}"
    url = f"{base}?{qs}" if qs else base
    print("Connecting to:", url)
    with closing(create_connection(url, timeout=60)) as ws:  # allow up to 60s between frames
        start = time.time()
        max_preview = 3
        while True:
            try:
                raw = ws.recv()
            except WebSocketTimeoutException:
                # No data within timeout window; continue waiting (server ping/pong should keep the connection open)
                continue
            except WebSocketConnectionClosedException:
                print("Connection closed by server")
                break
            if raw is None or raw == "" or raw == b"":
                print("Received empty frame; waiting for next or exiting...")
                time.sleep(0.25)
                continue
            if isinstance(raw, (bytes, bytearray)):
                try:
                    raw = raw.decode("utf-8")
                except Exception:
                    print(f"Received non-UTF8 binary frame of len={len(raw)}; skipping")
                    continue
            try:
                env = json.loads(raw)
            except json.JSONDecodeError:
                print(f"Non-JSON frame: {raw[:120]!r} ... (len={len(raw)})")
                continue

            phase = env.get("phase")
            part = env.get("part")
            rows = env.get("rows", [])
            wm_ns = env.get("watermark_ns")
            seq = (env.get("meta") or {}).get("seq")

            if phase == "connected":
                print(
                    f"phase=connected provider={env.get('provider')} "
                    f"schema={env.get('data_schema')} symbol={env.get('symbol')}"
                )
                continue
            if phase == "error":
                print(f"phase=error message={env.get('message')}")
                break
            if phase == "snapshot_boundary":
                print(
                    f"seq={seq if seq is not None else '?'} phase=snapshot_boundary wm_ns={wm_ns} "
                    f"iso={ns_to_iso(wm_ns) if wm_ns else None}"
                )
            else:
                print(
                    f"seq={seq if seq is not None else '?'} phase={phase} part={part} rows={len(rows)} wm={wm_ns}"
                )
                if rows:
                    preview = rows[:max_preview]
                    for i, r in enumerate(preview):
                        print(f"  row[{i}]: {json.dumps(r, ensure_ascii=False)}")
                    if len(rows) > max_preview:
                        print(f"  ... {len(rows) - max_preview} more rows omitted")
            if time.time() - start > 120:
                break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
