"""Feeder lifecycle controls component.

Exports a `feeder_controls` ui.component providing start/stop actions.
"""
from __future__ import annotations
from typing import List
from deephaven import ui
import deepfeeder as dfb

__all__ = ["feeder_controls"]


@ui.component
def feeder_controls():  # type: ignore[no-untyped-def]
    """Toolbar providing lifecycle controls for configured feeders."""
    refresh, set_refresh = ui.use_state(0)
    cfgs: List[dict] = ui.use_memo(lambda: dfb.feeder_manager.list_configs(), [refresh])  # type: ignore[assignment]
    keys: List[str] = [f"{c['provider']}:{c['name']}" for c in cfgs]
    selected_key, set_selected_key = ui.use_state(keys[0] if keys else "")

    def _sync_selection():
        nonlocal selected_key
        if selected_key and selected_key not in keys:
            set_selected_key(keys[0] if keys else "")
    _sync_selection()

    def _ensure_selected():
        if not selected_key:
            ui.toast("Select a feeder first")
            return None, None
        p, n = selected_key.split(":", 1)
        return p, n

    def start_selected():
        if not keys:
            return
        p, n = _ensure_selected()
        if p:
            ui.toast(dfb.feeder_manager.start(p, n, []))

    def stop_selected():
        if not keys:
            return
        p, n = _ensure_selected()
        if p:
            ui.toast(dfb.feeder_manager.stop(p, n))

    def start_all():
        if not keys:
            return
        ui.toast(dfb.feeder_manager.start_all())

    def stop_all():
        if not keys:
            return
        ui.toast(dfb.feeder_manager.stop_all())

    def reload_from_disk():
        ui.toast(dfb.feeder_manager.reload_configs())
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
