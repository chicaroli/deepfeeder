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
                adapt_bar_row(row, provider=provider, schema=schema, part="snapshot")
                if is_bars else
                adapt_trade_row(row, provider=provider, schema=schema, part="snapshot")
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

    # Async helpers to send rows to this socket
    async def emit_snapshot_row(row: dict):
        msg = adapt_bar_row(row, provider=provider, schema=schema, part="snapshot") if is_bars \
              else adapt_trade_row(row, provider=provider, schema=schema, part="snapshot")
        await ws.send_text(json.dumps(msg))

    async def emit_replay_row_async(part: str, row: dict):
        msg = adapt_bar_row(row, provider=provider, schema=schema, part=part) if is_bars \
              else adapt_trade_row(row, provider=provider, schema=schema, part=part)
        await ws.send_text(json.dumps(msg))

    # Bridges from sync → async
    def emit_snapshot_row_sync(row: dict):
        asyncio.get_event_loop().create_task(emit_snapshot_row(row))

    def emit_replay_row_sync(part: str, row: dict):
        asyncio.get_event_loop().create_task(emit_replay_row_async(part, row))

    # 1) Snapshot + short replay (gapless attach) under DH context
    try:
        with use_dh_ctx():
            handle, wm_ns = market_feeder.attach_gapless(
                provider, schema, symbol,
                emit_snapshot_row=emit_snapshot_row_sync,
                emit_replay_row=emit_replay_row_sync,
                fields=fields_list,
                only_completed=(only_completed if only_completed is not None else is_bars),
                start_ns=since_ns,
            )
        # We only needed to ensure the listener exists; drop the temporary handle
        with use_dh_ctx():
            market_feeder.unsubscribe(handle)
    except Exception:
        await ws.close(code=1011)
        return

    # 2) Live stream using the proven subscription path (subscribe also needs DH context)
    pipe = ConnPipe(provider, schema, symbol, maxsize=5000)
    try:
        with use_dh_ctx():
            pipe.attach(fields=fields_list, only_completed=only_completed)
        while True:
            payload = await pipe.q.get()
            await ws.send_text(payload)
    except WebSocketDisconnect:
        pass
    finally:
        with use_dh_ctx():
            pipe.detach()
