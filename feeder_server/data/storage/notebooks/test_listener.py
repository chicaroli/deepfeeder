import deepfeeder as mfb
import threading
import queue
import time
import pyarrow as pa

# Background queue for lightweight dict snapshots (not live tables)
_work_q: queue.Queue = queue.Queue(maxsize=512)
_stop = False

def _worker():
    while not _stop:
        try:
            row = _work_q.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            # row is already a dict snapshot
            ts = row.get('Timestamp')
            print(f"COMPLETED {ts} | {row}")
        except Exception as e:
            print("worker error printing snapshot:", e)
        finally:
            _work_q.task_done()

_thread = threading.Thread(target=_worker, daemon=True)
_thread.start()

# Lightweight callback: snapshot last completed bar immediately (no deferred table access)
def print_batch(batch):
    completed = batch.get('completed')
    if not isinstance(completed, pa.Table) or getattr(completed, 'num_rows', 0) == 0:
        return
    try:
        # Snapshot last row values now to avoid later mutation issues
        idx = completed.num_rows - 1
        names = list(completed.schema.names)
        snap = {name: completed.column(i)[idx].as_py() for i, name in enumerate(names)}
        _work_q.put_nowait(snap)
    except queue.Full:
        pass
    except Exception as e:
        print("callback snapshot error:", e)

provider = "binance"
data_schema = "ohlcv_1m"
symbol = "BTCUSDT"
cols = ['Timestamp', 'Symbol', 'BarId', 'Close', 'Volume']
spec = mfb.fanout.get_schemas()[(provider, data_schema)]
view = spec.table_fn().where(f"{spec.symbol_col}=='{symbol}'").view(cols)
SymListener = mfb.fanout.get_sym_listener()
listener = SymListener(provider, data_schema, symbol, spec, view, print_batch, debug=False)

# Optional: graceful stop helper (call manually if needed)

def stop():
    global _stop
    if _stop:
        return
    _stop = True
    try:
        listener.stop()
    except Exception:
        pass
    deadline = time.time() + 2
    while not _work_q.empty() and time.time() < deadline:
        time.sleep(0.05)
