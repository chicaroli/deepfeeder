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
                              on_press=lambda: ui.toast(dfb.start_feeder("btc_only", ["btcusdt"])),
                              variant="accent"
                              ),
                    ui.button("Stop BTC",
                              on_press=lambda: ui.toast(dfb.stop_feeder("btc_only")),
                              variant="primary", style="outline"
                              ),
                ),
                # ui.action_button("Start BTC", on_press=lambda: ui.toast(dfb.start_feeder("btc_only", ["btcusdt"]))),
                # ui.action_button("Stop BTC",  on_press=lambda: ui.toast(dfb.stop_feeder("btc_only"))),
                # direction="row",
            ),
            title="Controls",
        ),
        ui.panel(ui.table(dfb.binance_trades), title="Trades"),
        ui.panel(ui.table(dfb.status_table), title="Feeders status (live)"),
    )
)

