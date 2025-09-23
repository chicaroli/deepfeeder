from __future__ import annotations
import asyncio
import json
from typing import Optional, Iterable, List, Tuple
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse

from fanout.core import market_feeder, SCHEMAS
from fanout.dh_ctx import use_dh_ctx
from runtime.eventlog import emit_event
from .runtime_bridge import adapt_bar_row, adapt_trade_row, _is_bar_schema

router = APIRouter()

# ------------------------------
# HTTP: range snapshot (NDJSON)
# ------------------------------

@router.get("/schemas/list_schemas")
def list_schemas(provider: Optional[str] = Query(default=None)) -> JSONResponse:
    """
    List all (provider, schema) tuples available in the schema registry.
    Optionally filter by provider.

    Args:
        provider (Optional[str]): Provider name to filter results.

    Returns:
        JSONResponse: List of (provider, schema) tuples.
    """
    keys: List[Tuple[str, str]] = list(SCHEMAS.keys())
    if provider is not None:
        keys = [k for k in keys if k[0] == provider]
    return JSONResponse(keys)

@router.get("/range/{provider}/{schema}/{symbol}")
def range_snapshot(
    provider: str,
    schema: str,
    symbol: str,
    exchange: Optional[str] = Query(None, description="exchange code for multi-exchange schemas"),
    from_ns: Optional[int] = Query(None),
    to_ns: Optional[int] = Query(None),
    fields: Optional[str] = Query(None, description="comma-separated column list to project"),
):
    """Stream a non-ticking snapshot as NDJSON."""
    fields_list: Optional[Iterable[str]] = [f.strip() for f in fields.split(",")] if fields else None
    is_bars = _is_bar_schema(provider, schema)

    # Get schema spec and columns
    spec = SCHEMAS.get((provider, schema))
    if spec is None:
        raise HTTPException(status_code=400, detail=f"Unknown provider/schema: {provider}/{schema}")
    cols = spec.cols

    # Only pass exchange if it's in the schema columns
    exchange_arg = exchange if exchange and "Exchange" in cols else None

    try:
        # Enter Deephaven ExecutionContext on this request thread
        with use_dh_ctx():
            arr = market_feeder.snapshot_range(
                provider, schema, symbol,
                exchange=exchange_arg,
                start_ns=from_ns, end_ns=to_ns, fields=fields_list
            )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    def gen():
        # Emit each row as a 'snapshot' with closed=True
        for row in arr.to_pylist():
            msg = (
                adapt_bar_row(row, provider=provider, schema=schema, part="snapshot", fields_list=fields_list)
                if is_bars else
                adapt_trade_row(row, provider=provider, schema=schema, part="snapshot", fields_list=fields_list)
            )
            yield (json.dumps(msg) + "\n").encode("utf-8")

    return StreamingResponse(gen(), media_type="application/x-ndjson")

# ------------------------------
# WS: gapless attach + live
# ------------------------------

@router.websocket("/ws/{provider}/{schema}/{symbol}")
async def ws_stream(
    ws: WebSocket,
    provider: str,
    schema: str,
    symbol: str,
    since_ns: Optional[int] = Query(None, description="resume watermark (epoch ns)"),
    fields: Optional[str] = Query(None, description="comma-separated column list to project"),
    only_completed: Optional[bool] = Query(None, description="bars default True; trades default False"),
):
    def _emit_event(level: str, code: str, message: str, details: dict):
        emit_event(
            "fanout", f"{provider}:{schema}:{symbol}", "api", level, code,
            message,
            details,
        )

    # Accept and acknowledge
    await ws.accept()
    # Log connection
    _emit_event("INFO", "WS_CONNECTED","WebSocket client connected",
        {
            "since_ns": since_ns,
            "fields": fields.split(",") if fields else [],
            "only_completed": only_completed,
        },
    )
    # Send connected ack
    try:
        await ws.send_text(json.dumps({
            "version": 1,
            "phase": "connected",
            "provider": provider,
            "data_schema": schema,
            "symbol": symbol,
            "since_ns": since_ns,
        }))
    except Exception as e:
        print(f"[ws_stream] failed to send connected ack: {e!r}")
        _emit_event("ERROR", "WS_ACK_FAIL", "Failed sending connected ack", {"error": str(e)})
        try:
            await ws.close(code=1011)
        except Exception:
            pass
        return

    fields_list: Optional[Iterable[str]] = [f.strip() for f in fields.split(",")] if fields else None
    is_bars = _is_bar_schema(provider, schema)

    loop = asyncio.get_running_loop()

    # State for graceful shutdown of this connection
    closed = False
    unsub_done = False
    handle: Optional[str] = None
    send_err_count = 0

    async def _do_unsubscribe():
        nonlocal unsub_done, handle
        if unsub_done:
            return
        try:
            with use_dh_ctx():
                if handle:
                    market_feeder.unsubscribe(handle)
        except Exception:
            pass
        unsub_done = True

    async def send_event(env: dict):
        nonlocal closed, send_err_count
        if closed:
            return
        try:
            await ws.send_text(json.dumps(env))
        except Exception as e:
            send_err_count += 1
            if send_err_count <= 1:
                print(f"[ws_stream] send_event error: {e!r}; closing WS (suppressed further logs)")
                _emit_event("WARN", "WS_SEND_ERR", "Error sending frame; closing WS", {"error": str(e)})
            closed = True
            # Best-effort close and unsubscribe immediately to stop further emissions
            try:
                await ws.close(code=1011)
            except Exception:
                pass
            try:
                loop.create_task(_do_unsubscribe())
            except Exception:
                pass

    def emit_event_sync(env: dict):
        if closed:
            return
        try:
            # Inline row adaptation to keep code simple and avoid extra indirection
            phase_val = env.get("phase")
            # For snapshot envelopes, expose Phase as "snapshot" in adapted rows
            part = "snapshot" if phase_val == "snapshot" else (env.get("part") or "completed")
            rows = env.get("rows") or []
            if rows:
                if is_bars:
                    rows_adapted = [
                        adapt_bar_row(r, provider=provider, schema=schema, part=part, fields_list=fields_list)
                        for r in rows
                    ]
                else:
                    rows_adapted = [
                        adapt_trade_row(r, provider=provider, schema=schema, part=part, fields_list=fields_list)
                        for r in rows
                    ]
                env = {**env, "rows": rows_adapted}
            loop.call_soon_threadsafe(asyncio.create_task, send_event(env))
        except RuntimeError:
            pass

    # Attach with logging
    try:
        with use_dh_ctx():
            handle, wm_ns = market_feeder.attach_gapless(
                provider,
                schema,
                symbol,
                emit=emit_event_sync,
                fields=fields_list,
                only_completed=(only_completed if only_completed is not None else is_bars),
                start_ns=since_ns,
                snapshot_batch=True,
            )
        _emit_event("INFO", "WS_ATTACH_OK", "Client attached gapless",
                    {"handle": handle, "watermark_ns": wm_ns}
                    )
    except Exception as e:
        # Send error frame + log
        try:
            await ws.send_text(json.dumps({
                "version": 1,
                "phase": "error",
                "message": str(e),
                "provider": provider,
                "data_schema": schema,
                "symbol": symbol,
            }))
        except Exception:
            pass
        _emit_event("ERROR", "WS_ATTACH_ERR","attach_gapless failed", {"error": str(e)})
        try:
            await ws.close(code=1011)
        except Exception:
            pass
        return

    # Block until the client disconnects; this makes cleanup timely
    try:
        while True:
            try:
                _ = await ws.receive_text()
                # Optionally handle incoming client messages here
            except Exception:
                await asyncio.sleep(0)
    except WebSocketDisconnect:
        pass
    finally:
        # Ensure we unsubscribe if not already done
        try:
            await _do_unsubscribe()
        except Exception:
            pass
        _emit_event("INFO", "WS_DISCONNECTED","WebSocket client disconnected",
                    {"handle": handle}
                    )
