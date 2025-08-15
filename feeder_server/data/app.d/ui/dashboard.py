# app.d/ui/dashboard.py
from deephaven import ui
import deepfeeder_bindings as dfb

# --- Toolbar (uses shared selection) ---
@ui.component
def feeders_control():
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
        ui.toast(dfb.start_all())

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

# --- Dashboard ---
FeederDashboard = ui.dashboard(
    ui.column(
        ui.row(
            ui.stack(
                feeders_control(),
                width=30,
            ),
            ui.stack(
                ui.panel(ui.table(
                    dfb.status_table,
                    format_=[
                        ui.TableFormat(cols="alive", if_="alive", background_color="positive", color="white"),
                        ui.TableFormat(cols="alive", if_="!alive", background_color="negative", color="white"),
                    ],
                ), title="Feeders status (live)"),
                ui.panel(ui.table(dfb.configs_live_table), title="Configs (live)"),
                active_item_index=0,
            ),
            height=20,
        ),
        ui.stack(
            ui.panel(ui.table(dfb.binance_trades), title="Binance Trades"),
            ui.panel(ui.table(dfb.binance_ohlcv_1m), title="Binance OHLCV 1m"),
            ui.panel(ui.table(dfb.tv_quotes), title="TV Quotes (delayed)"),
            ui.panel(ui.table(dfb.tv_ohlcv_1m), title="TV OHLCV 1m"),
            ui.panel(ui.table(dfb.tv_ohlcv_5m), title="TV OHLCV 5m"),
            active_item_index=0,
            height=80,
        ),
    ),
)
