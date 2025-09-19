from __future__ import annotations
import asyncio
import json
from typing import Optional, Iterable, List, Tuple
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse

from fanout.core import market_feeder, SCHEMAS
from fanout.dh_ctx import use_dh_ctx
from .runtime_bridge import ConnPipe, adapt_bar_row, adapt_trade_row, _is_bar_schema

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
    await ws.accept()
    fields_list: Optional[Iterable[str]] = [f.strip() for f in fields.split(",")] if fields else None
    is_bars = _is_bar_schema(provider, schema)

    # Unified JSON envelope sender
    async def send_event(env: dict):
        try:
            await ws.send_text(json.dumps(env))
        except Exception:
            try:
                await ws.close(code=1011)
            except Exception:
                pass

    # Sync shim to schedule async send on the current loop
    def emit_event_sync(env: dict):
        asyncio.get_event_loop().create_task(send_event(env))

    # Attach gapless (snapshot -> snapshot_boundary -> replay -> live) using unified envelopes
    try:
        with use_dh_ctx():
            handle, wm_ns = market_feeder.attach_gapless(
                provider,
                schema,
                symbol,
                emit_event=emit_event_sync,
                fields=fields_list,
                only_completed=(only_completed if only_completed is not None else is_bars),
                start_ns=since_ns,
                snapshot_batch=True,
            )
    except Exception:
        await ws.close(code=1011)
        return

    # Keep the websocket open to continue receiving live envelopes
    try:
        while True:
            await asyncio.sleep(3600)
    except WebSocketDisconnect:
        pass
    finally:
        try:
            with use_dh_ctx():
                market_feeder.unsubscribe(handle)
        except Exception:
            pass
