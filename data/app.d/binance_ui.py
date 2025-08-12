# /data/app.d/binance_ui.py
from deephaven.appmode import get_app_state
from deephaven import ui
import deepfeeder_bindings as dfb
import json

app = get_app_state()

@ui.component
def create_dashboard():
    # state to hold the status JSON as a string
    status_str, set_status_str = ui.use_state("No status")

    def refresh_status():
        try:
            data = dfb.status_feeders()
            set_status_str(json.dumps(data, indent=2))
        except Exception as e:
            set_status_str(f"Error: {e}")

    def start_btc():
        ui.toast(dfb.start_feeder("btc_only", ["btcusdt"]))

    def stop_btc():
        ui.toast(dfb.stop_feeder("btc_only"))

    return ui.column(
        ui.panel(
            ui.flex(
                ui.action_button("Start BTC", on_press=start_btc),
                ui.action_button("Stop BTC", on_press=stop_btc),
                ui.action_button("Refresh status", on_press=refresh_status),
                direction="row",
            ),
            title="Controls",
        ),
        ui.panel(ui.table(dfb.binance_trades), title="Trades"),
        ui.panel(ui.text(status_str), title="Feeders status"),
    )

# top-level dashboard with a single child (column)
binance_dashboard = ui.dashboard(create_dashboard())
