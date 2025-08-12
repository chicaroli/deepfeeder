# /data/app.d/binance_ui.py
from deephaven.appmode import get_app_state
from deephaven import ui
import deepfeeder_bindings as dfb

app = get_app_state()

binance_dashboard = ui.dashboard(
    ui.column(
        ui.panel(
            ui.flex(
                ui.action_button("Start BTC", on_press=lambda: print(dfb.start_feeder("btc_only", ["btcusdt"]))),
                ui.action_button("Stop BTC",  on_press=lambda: print(dfb.stop_feeder("btc_only"))),
                ui.action_button("Status",    on_press=lambda: print(dfb.status_feeders())),
                direction="row"
                ),
            title="Controls"
        ),
        ui.panel(
            ui.table(dfb.binance_trades),
            title="Trades"
        ),
    )
)
