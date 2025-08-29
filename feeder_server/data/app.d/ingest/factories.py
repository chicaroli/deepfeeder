# app.d/ingest/factories.py
from typing import List
from runtime.heartbeat import Heartbeater
from core.contracts import EventBus, Tick
from runtime.feeder_specs import FeederSpec

# import your existing workers
# from feeders.binance.ws import BinanceWsWorker
# from feeders.tradingview.ws import TradingViewWsWorker  # stateful additive
# from feeders.tradingview.backfill import TradingViewBarsBackfillWorker

# import provider adapters (JSON->Tick) you already mapped
from providers.binance.adapter import trade_json_to_tick
from providers.tradingview.adapter import quote_json_to_tick, api_bar_to_tick

def make_binance_ws(spec: FeederSpec, bus: EventBus):
    """
    Wrap your Binance WS worker: on each message -> Tick -> microbatch -> bus.publish()
    """
    class Producer:
        provider = "binance"
        name = f"{spec.provider}:{spec.name}"
        def __init__(self): 
            self._hb = Heartbeater("feeder", self.name, "ws")
            self._batch: List[Tick] = []
        def start(self):
            # self._ws = BinanceWsWorker(spec.symbols, on_message=self._on_msg, hb=self._hb)
            # self._ws.start()
            ...
        def stop(self):
            # self._ws.stop()
            ...
        def subscribe(self, symbols: List[str]): 
            # self._ws.subscribe([s.upper() for s in symbols])
            ...
        def _on_msg(self, raw: dict):
            t = trade_json_to_tick(raw)
            self._batch.append(t)
            if len(self._batch) >= 512: 
                bus.publish(self._batch); self._batch.clear()
    return Producer()

def make_tradingview_ws(spec: FeederSpec, bus: EventBus):
    """
    Reuse your stateful TV worker; after it computes the merged quote snapshot, convert to Tick and publish.
    """
    class Producer:
        provider = "tradingview"
        name = f"{spec.provider}:{spec.name}"
        def __init__(self):
            self._hb = Heartbeater("feeder", self.name, "ws")
            self._batch: List[Tick] = []
        def start(self):
            # self._ws = TradingViewWsWorker(spec.symbols, on_snapshot=self._on_snapshot, hb=self._hb)
            # self._ws.start()
            ...
        def stop(self):
            # self._ws.stop()
            ...
        def subscribe(self, symbols: List[str]): 
            # self._ws.subscribe(symbols)
            ...
        def _on_snapshot(self, symbol: str, snapshot: dict):
            # snapshot is your already-normalized state (tested). Wrap to Tick, don’t change logic.
            t = quote_json_to_tick(symbol, snapshot)
            self._batch.append(t)
            if len(self._batch) >= 512:
                bus.publish(self._batch); self._batch.clear()
    return Producer()

def make_tradingview_backfill(spec: FeederSpec, bus: EventBus):
    """
    Wrap your TV backfill worker: rows -> Tick -> batched publish
    """
    class Producer:
        provider = "tradingview"
        name = f"{spec.provider}:{spec.name}/backfill"
        def __init__(self):
            self._hb = Heartbeater("feeder", self.name, "rest")
            self._batch: List[Tick] = []
        def start(self):
            # self._bf = TradingViewBarsBackfillWorker(spec.symbols, on_row=self._on_row, hb=self._hb)
            # self._bf.start()
            ...
        def stop(self):
            # self._bf.stop()
            ...
        def subscribe(self, symbols: List[str]): 
            # self._bf.add_symbols(symbols)
            ...
        def _on_row(self, symbol: str, bar: dict):
            self._batch.append(api_bar_to_tick(symbol, bar))
            if len(self._batch) >= 2048:
                bus.publish(self._batch); self._batch.clear()
    return Producer()
