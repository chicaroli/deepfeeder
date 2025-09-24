# app.d/fanout/protocol.py
ENVELOPE_VERSION: int = 1
PROTOCOL_ID: str = "deepfeeder.fanout.envelope"

ROW_TYPE_BAR = "bar"
ROW_TYPE_TRADE = "trade"

def envelope_header() -> dict:
    return {"version": ENVELOPE_VERSION, "protocol": PROTOCOL_ID}
