# app.d/ui/dashboard.py
from deephaven import ui
import deepfeeder_bindings as dfb

# Controls as a component (stateful inputs)
@ui.component
def controls():
    provider, set_provider = ui.use_state("binance")
    name, set_name = ui.use_state("custom")
    symbols, set_symbols = ui.use_state("btcusdt")

    def start():
        ui.toast(dfb.start_feeder_csv(provider, name, symbols))
    def stop():
        ui.toast(dfb.stop_feeder(provider, name))

    return ui.panel(
        ui.flex(
            ui.picker("binance", selected_key=provider, on_change=set_provider, label="Provider"),
            ui.text_field(label="Feeder name", value=name, on_change=set_name),     # Text Field API. :contentReference[oaicite:0]{index=0}
            ui.text_field(label="Symbols CSV", value=symbols, on_change=set_symbols),
            ui.action_button("Start", on_press=start),
            ui.action_button("Stop", on_press=stop),
            direction="row",
        ),
        title="Controls",
    )

# Top-level dashboard with ONE child (doc rule). :contentReference[oaicite:1]{index=1}
binance_dashboard = ui.dashboard(
    ui.column(
        controls(),
        ui.stack(
            ui.panel(ui.table(dfb.trades_table), title="Trades (canonical)"),
            ui.panel(ui.table(dfb.binance_trades_detailed), title="Binance Trades (detailed)"),
            ui.panel(ui.table(dfb.binance_ohlcv_1m), title="Binance OHLCV 1m"),
            active_item_index=0,  # which tab opens first
        ),
        ui.panel(ui.table(dfb.status_table),  title="Feeders status (live)"),
    )
)
