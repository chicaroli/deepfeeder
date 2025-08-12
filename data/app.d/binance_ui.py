# /data/app.d/binance_ui.py
from deephaven.appmode import get_app_state
from deephaven import ui
import deepfeeder_bindings as dfb

app = get_app_state()

binance_dashboard = ui.dashboard(
    ui.column(
        ui.panel(
            ui.flex(
                ui.button_group(
                    ui.button("Start BTC",
                              on_press=lambda: ui.toast(dfb.start_feeder("binance", "btc_only", ["btcusdt"])),
                              variant="accent"
                              ),
                    ui.button("Stop BTC",
                              on_press=lambda: ui.toast(dfb.stop_feeder("binance", "btc_only")),
                              variant="primary", style="outline"
                              ),
                ),
                # later: use a text input to call start_feeder_csv("binance", name, symbols_csv)
            ),
            title="Controls",
        ),
        ui.panel(ui.table(dfb.binance_trades), title="Trades"),
        ui.panel(ui.table(dfb.status_table),  title="Feeders status (live)"),
    )
)
