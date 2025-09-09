from __future__ import annotations
import time
import json
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import PlainTextResponse, JSONResponse

from fanout.core import market_feeder

router = APIRouter()


# -----------------------------
# HTTP: Basic connectivity
# -----------------------------

@router.get("/ping", response_class=PlainTextResponse)
def ping() -> str:
    return "pong"

@router.head("/health")
@router.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "service": "deepfeeder-fanout",
        "time_iso": datetime.now(timezone.utc).isoformat(),
        "time_epoch_s": int(time.time()),
    }

@router.get("/time")
def server_time() -> dict:
    now = datetime.now(timezone.utc)
    return {
        "iso": now.isoformat(),
        "epoch_s": int(now.timestamp()),
        "epoch_ms": int(now.timestamp() * 1000),
        "epoch_ns": int(now.timestamp() * 1_000_000_000),
        "tz": "UTC",
    }

@router.get("/echo")
def echo(q: Optional[str] = Query(None, description="String to echo back")) -> dict:
    return {"echo": q}


# -----------------------------
# HTTP: Fanout core snapshot
# -----------------------------

@router.get("/stats")
def stats() -> JSONResponse:
    """
    Returns MarketFeeder internal stats (no DH access beyond what's already running).
    Useful to confirm the process is alive and listeners/subscriptions are tracked.
    """
    s = market_feeder.stats()
    # Keep payload small-ish; you can remove this truncation if you want the full dump.
    listeners = s.get("listeners", [])
    if len(listeners) > 200:
        s["listeners"] = listeners[:200] + [{"note": f"... truncated {len(listeners)-200} more"}]
    return JSONResponse(s)


# -----------------------------
# WebSocket: Echo / Ping
# -----------------------------

@router.websocket("/ws/echo")
async def ws_echo(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            msg = await ws.receive_text()
            await ws.send_text(json.dumps({
                "type": "echo",
                "received": msg,
                "time": datetime.now(timezone.utc).isoformat(),
            }))
    except WebSocketDisconnect:
        pass

@router.websocket("/ws/ping")
async def ws_ping(ws: WebSocket, every_ms: int = 1000):
    """
    Sends a 'pong' message every N ms and echoes any text received.
    Useful to test firewalls/proxies and client WS handling.
    """
    await ws.accept()
    try:
        last = time.time()
        while True:
            # Non-blocking receive with small timeout so we can also send heartbeats
            try:
                msg = await ws.receive_text()
                await ws.send_text(json.dumps({
                    "type": "echo",
                    "received": msg,
                    "time": datetime.now(timezone.utc).isoformat(),
                }))
            except Exception:
                # Ignore if no message ready; we still send heartbeat below
                pass

            now = time.time()
            if (now - last) * 1000.0 >= max(1, every_ms):
                await ws.send_text(json.dumps({
                    "type": "pong",
                    "time": datetime.now(timezone.utc).isoformat(),
                }))
                last = now
            # Tiny sleep to avoid a hot loop
            await ws.receive_text()  # will block until message; replaced by heartbeat above
    except WebSocketDisconnect:
        pass
    except Exception:
        # Best-effort close; swallow to keep test endpoint robust
        try:
            await ws.close()
        except Exception:
            pass
