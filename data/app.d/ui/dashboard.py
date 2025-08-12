# app.d/ui/dashboard.py
from deephaven import ui
import deepfeeder_bindings as dfb

@ui.component
def config_manager():
    # Build picker options from the registry list; refresh button recomputes
    refresh, set_refresh = ui.use_state(0)
    cfgs = ui.use_memo(lambda: dfb.configs_list(), [refresh])
    keys = [f"{c['provider']}:{c['name']}" for c in cfgs]
    selected_key, set_selected_key = ui.use_state(keys[0] if keys else "")

    # Fields to create/update configs
    provider, set_provider = ui.use_state("binance")
    name, set_name = ui.use_state("btc_only")
    symbols_csv, set_symbols_csv = ui.use_state("btcusdt")
    autostart, set_autostart = ui.use_state(True)

    def _split_csv(s: str):
        return [p.strip() for p in s.split(",") if p.strip()]

    def save():
        ui.toast(dfb.upsert_config(provider, name, _split_csv(symbols_csv), autostart))
        set_refresh(refresh + 1)

    def remove_selected():
        if not selected_key:
            return ui.toast("Select a config first")
        p, n = selected_key.split(":", 1)
        ui.toast(dfb.remove_config(p, n))
        set_refresh(refresh + 1)

    def start_selected():
        if not selected_key:
            return ui.toast("Select a config first")
        p, n = selected_key.split(":", 1)
        ui.toast(dfb.start_feeder(p, n, []))

    def stop_selected():
        if not selected_key:
            return ui.toast("Select a config first")
        p, n = selected_key.split(":", 1)
        ui.toast(dfb.stop_feeder(p, n))

    def start_autostart():
        ui.toast(dfb.stop_all())  # optional: stop current first
        ui.toast(dfb.start_all_autostart())

    def load_selected_into_form():
        if not selected_key:
            return ui.toast("Select a config first")
        p, n = selected_key.split(":", 1)
        cfgs_map = {f"{c['provider']}:{c['name']}": c for c in cfgs}
        c = cfgs_map.get(selected_key)
        if not c:
            return ui.toast("Config not found")
        set_provider(c["provider"])
        set_name(c["name"])
        set_symbols_csv(",".join(c["symbols"]))
        set_autostart(bool(c.get("autostart", False)))

    return ui.panel(
        ui.column(
            ui.flex(
                ui.picker(*keys, selected_key=selected_key, on_change=set_selected_key, label="Existing configs"),
                ui.action_button("Start Selected", on_press=start_selected),
                ui.action_button("Stop Selected", on_press=stop_selected),
                ui.action_button("Remove Selected", on_press=remove_selected),
                ui.action_button("Refresh", on_press=lambda: set_refresh(refresh + 1)),
                ui.action_button("Load → Form", on_press=load_selected_into_form),
                ui.action_button("Reload from disk",
                                 on_press=lambda: (ui.toast(dfb.reload_configs()), set_refresh(refresh + 1))),
                direction="row",
            ),
            ui.flex(
                ui.text_field(label="Provider", value=provider, on_change=set_provider),
                ui.text_field(label="Name", value=name, on_change=set_name),
                ui.text_field(label="Symbols CSV", value=symbols_csv, on_change=set_symbols_csv),
                ui.radio_group(
                    ui.radio("Autostart: On"),
                    ui.radio("Autostart: Off"),
                    value="Autostart: On" if autostart else "Autostart: Off",
                    on_change=lambda v: set_autostart(v.endswith("On")),
                ),
                ui.action_button("Save / Update Config", on_press=save),
                ui.action_button("Start All Autostart", on_press=start_autostart),
                direction="row",
            ),
        ),
        title="Config Manager",
    )

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
            ui.button_group(
                ui.button("Start", on_press=start, variant="accent"),
                ui.button("Stop", on_press=stop, variant="primary", style="outline", static_color="red"),
                ui.button(
                    "Save Config",
                    on_press=lambda: ui.toast(dfb.upsert_config(provider, name, symbols.split(","), autostart=True))
                    ),
                ui.button(
                    "Start from Config",
                    on_press=lambda: ui.toast(dfb.start_feeder(provider, name, []))
                    ),
                ui.button("Stop All", on_press=lambda: ui.toast(dfb.stop_all())),
            ),
            direction="row",
        ),
        title="Controls",
    )

# Top-level dashboard with ONE child (doc rule)
FeederDashboard = ui.dashboard(
    ui.column(
        controls(),
        config_manager(),
        ui.stack(
            ui.panel(ui.table(dfb.trades_table), title="Trades (canonical)"),
            ui.panel(ui.table(dfb.binance_trades_detailed), title="Binance Trades (detailed)"),
            ui.panel(ui.table(dfb.binance_ohlcv_1m), title="Binance OHLCV 1m"),
            active_item_index=0,  # which tab opens first
        ),
        ui.panel(ui.table(dfb.status_table),  title="Feeders status (live)"),
    ),
)
