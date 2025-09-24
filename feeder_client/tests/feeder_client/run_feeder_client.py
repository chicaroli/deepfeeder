from __future__ import annotations
import time

from feeder_client.client import DeepFeederClient
from feeder_client.models import Envelope, Bar, Trade


def on_data(env: Envelope):
    print(env)
    if env.is_boundary:
        print("boundary", env.seq, env.watermark_ns)
        return
    if env.rows and env.is_bar:
        for row in env.rows[:min(5, len(env.rows)-5)]:
            print("bar", env.phase, env.part, row.symbol, row.timestamp, row.close)
    elif env.rows and env.is_trade:
        for row in env.rows[:min(5, len(env.rows) - 5)]:
            print("trade", env.phase, env.part, row.symbol, row.timestamp, row.price)


if __name__ == "__main__":
    client = DeepFeederClient()
    s = client.subscribe(
        provider="tradingview",
        schema="ohlcv_1m",
        symbol="INDV2025",
        exchange="BMFBOVESPA",
        fields="Timestamp,Open,High,Low,Close,Volume,Symbol,Exchange,BarId",
        only_completed=True,
        on_data=on_data,
    )
    try:
        time.sleep(5*60)
    finally:
        client.unsubscribe(s)
        client.close()

