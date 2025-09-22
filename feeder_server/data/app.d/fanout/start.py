# app.d/fanout/start.py
from __future__ import annotations
import os
import threading
import asyncio
import atexit
import uvicorn

from fanout.routes import create_app

# Config via env (override in docker-compose as needed)
HOST = os.getenv("FANOUT_HOST", "0.0.0.0")
PORT = int(os.getenv("FANOUT_PORT", "8081"))
LOG_LEVEL = os.getenv("FANOUT_LOG_LEVEL", "info")

# WS config: prefer native ping/pong via 'websockets' implementation
WS_IMPL = os.getenv("FANOUT_WS_IMPL", "websockets")  # websockets | wsproto | auto
try:
    WS_PING_INTERVAL = float(os.getenv("FANOUT_WS_PING_INTERVAL", "30"))
except Exception:
    WS_PING_INTERVAL = 30.0
try:
    WS_PING_TIMEOUT = float(os.getenv("FANOUT_WS_PING_TIMEOUT", "15"))
except Exception:
    WS_PING_TIMEOUT = 15.0

_server: uvicorn.Server | None = None
_thread: threading.Thread | None = None

def _run_uvicorn():
    # Dedicated event loop in this thread to avoid clashing with DH / JPy loops
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    config = uvicorn.Config(
        app=create_app(),
        host=HOST,
        port=PORT,
        log_level=LOG_LEVEL,
        reload=False,       # don't use reload inside DH
        lifespan="off",     # keep simple; we don't need ASGI lifespan here
        access_log=False,
        proxy_headers=True,
        ws=WS_IMPL,
        ws_ping_interval=WS_PING_INTERVAL,
        ws_ping_timeout=WS_PING_TIMEOUT,
    )
    global _server
    _server = uvicorn.Server(config)
    _server.run()          # blocking inside this thread

def start_fanout():
    """Idempotent start – safe to import multiple times in App Mode."""
    global _thread
    if _thread and _thread.is_alive():
        return
    _thread = threading.Thread(target=_run_uvicorn, name="fanout-uvicorn", daemon=True)
    _thread.start()

def stop_fanout():
    """Graceful stop (called on kernel/app shutdown)."""
    try:
        if _server is not None:
            _server.should_exit = True
    except Exception:
        pass

atexit.register(stop_fanout)
