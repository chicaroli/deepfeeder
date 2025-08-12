# app.d/ui/dashboard.py
from deephaven import ui
import deepfeeder_bindings as dfb
from core.bus import get_configs_table


@ui.component
def feeders_toolbar():
    # Rebuild picker options when refreshed
    refresh, set_refresh = ui.use_state(0)
    cfgs = ui.use_memo(lambda: dfb.configs_list(), [refresh])
    keys = [f"{c['provider']}:{c['name']}" for c in cfgs]
    selected_key, set_selected_key = ui.use_state(keys[0] if keys else "")

    def _ensure_selected():
        if not selected_key:
            ui.toast("Select a feeder first")
            return None, None
        p, n = selected_key.split(":", 1)
        return p, n

    def start_selected():
        p, n = _ensure_selected()
        if p: ui.toast(dfb.start_feeder(p, n, []))

    def stop_selected():
        p, n = _ensure_selected()
        if p: ui.toast(dfb.stop_feeder(p, n))

    def start_all():
        # Start *all* configs found (ignores autostart)
        started = 0
        for c in cfgs:
            res = dfb.start_feeder(c["provider"], c["name"], [])
            if "started" in str(res).lower():
                started += 1
        ui.toast(f"Requested start for {len(cfgs)} feeders ({started} reported 'started').")

    def stop_all():
        ui.toast(dfb.stop_all())

    def reload_from_disk():
        ui.toast(dfb.reload_configs())
        set_refresh(refresh + 1)

    return ui.panel(
        ui.flex(
            ui.picker(*keys, selected_key=selected_key, on_change=set_selected_key, label="Feeders"),
            ui.button_group(
                ui.button("Start", on_press=start_selected, variant="primary", style="outline"),
                ui.button("Stop", on_press=stop_selected, variant="primary", style="outline"),
                orientation="vertical",
            ),
            ui.button_group(
                ui.button("Start All", on_press=start_all, variant="accent"),
                ui.button("Stop All", on_press=stop_all, variant="accent"),
                ui.button("Reload", on_press=reload_from_disk, variant="primary", style="outline"),
                orientation="vertical",
            ),
            direction="row",
        ),
        title="Feeder Controls",
    )

# Top-level dashboard with ONE child (doc rule)
FeederDashboard = ui.dashboard(
    ui.column(
        # Top area (30%)
        ui.row(
            feeders_toolbar(),
            ui.stack(
                ui.panel(ui.table(dfb.status_table), title="Feeders status (live)"),
                ui.panel(ui.table(dfb.configs_live_table), title="Configs (live)"),
                active_item_index=0,
            ),
            height=25,
        ),
        # Bottom area (70%) – the tabbed stack
        ui.stack(
            ui.panel(ui.table(dfb.trades_table), title="Trades (canonical)"),
            ui.panel(ui.table(dfb.binance_trades_detailed), title="Binance Trades (detailed)"),
            ui.panel(ui.table(dfb.binance_ohlcv_1m), title="Binance OHLCV 1m"),
            active_item_index=0,
            height=75,
        ),
    ),
)