# /data/app.d/binance_ui.py
from deephaven.appmode import get_app_state
from deephaven import ui
import deepfeeder_bindings as dfb

app = get_app_state()

@ui.component
def binance_controls():
    name, set_name = ui.use_state("btc_only")
    syms_csv, set_syms_csv = ui.use_state("btcusdt")

    def _start():
        syms = [s.strip().lower() for s in syms_csv.split(",") if s.strip()]
        print(dfb.start_feeder(name, syms))

    def _stop():
        print(dfb.stop_feeder(name))

    def _status():
        print(dfb.status_feeders())

    return ui.flex(
        ui.text_field(value=name, on_change=set_name, label="Feed name"),
        ui.text_field(value=syms_csv, on_change=set_syms_csv, label="Symbols (csv)"),
        ui.action_button("Start", on_press=_start),
        ui.action_button("Stop", on_press=_stop),
        ui.action_button("Status", on_press=_status),
        direction="row"
    )

# IMPORTANT: dashboard takes ONE element (a row or column). No title kwarg.
binance_dashboard = ui.dashboard(
    ui.column(
        ui.panel(binance_controls(), title="Controls"),
        ui.panel(ui.table(dfb.binance_trades), title="Binance Trades"),
    )
)

# Register so it appears under Applications
app["binance_dashboard"] = binance_dashboard
