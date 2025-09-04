"""Feeder lifecycle controls component.

Exports a `feeder_controls` ui.component providing start/stop actions.
Updated to use the new FeedManager service instead of the legacy feeder_manager / Orchestrator.
"""
from __future__ import annotations
from typing import List, Dict, Any
from deephaven import ui
import deepfeeder as dfb
import json

__all__ = ["feeder_controls"]


# Helper: load feeder specs from registered path service (set in app.py)
_def_specs_path = "/data/storage/notebooks/feeders.json"


def _load_specs() -> List[Dict[str, Any]]:
    path = None
    try:
        path = dfb.get_service("feeder_specs_path")  # registered in app.py
    except Exception:
        path = None
    path = path or _def_specs_path
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # ensure required fields and normalized shape
        specs: List[Dict[str, Any]] = []
        for item in data:
            provider = str(item.get("provider", "")).strip().lower()
            name = str(item.get("name", "")).strip()
            if not provider or not name:
                continue
            specs.append({
                "provider": provider,
                "name": name,
                "symbols": ",".join(item.get("symbols", [])),  # display only
                "autostart": bool(item.get("autostart", False)),
            })
        return specs
    except Exception:
        return []


def _get_feed_manager():
    # Prefer new service name then fall back to legacy alias
    for key in ("feed_manager", "orchestrator"):
        try:
            return dfb.services.get(key)
        except Exception:
            try:
                return dfb.get_service(key)
            except Exception:
                pass
    return None


@ui.component
def feeder_controls():  # type: ignore[no-untyped-def]
    """Toolbar providing lifecycle controls for configured feeders via FeedManager."""
    refresh, set_refresh = ui.use_state(0)
    cfgs: List[dict] = ui.use_memo(_load_specs, [refresh])  # type: ignore[assignment]
    keys: List[str] = [f"{c['provider']}:{c['name']}" for c in cfgs]
    selected_key, set_selected_key = ui.use_state(keys[0] if keys else "")

    fm = _get_feed_manager()

    def _sync_selection():
        nonlocal selected_key
        if selected_key and selected_key not in keys:
            set_selected_key(keys[0] if keys else "")
    _sync_selection()

    def _ensure_selected():
        if not selected_key:
            ui.toast("Select a feeder first")
            return None
        return selected_key

    def _matching_producers(fm, service_key: str) -> list[str]:
        """Map a service key 'provider:name' to all producer names 'provider:stream:name'."""
        try:
            provider, service = service_key.split(":", 1)
        except ValueError:
            return []
        matches = []
        for n in fm.list_producers():
            parts = n.split(":", 2)  # ["provider","stream","name"]
            if len(parts) == 3 and parts[0] == provider and parts[2] == service:
                matches.append(n)
        return matches

    def start_selected():
        if not fm:
            ui.toast("FeedManager unavailable");
            return
        key = _ensure_selected()
        if not key:
            return
        names = _matching_producers(fm, key)
        if not names:
            ui.toast(f"No producers found for {key}");
            return
        errs = 0
        for n in names:
            try:
                fm.start_producer(n)
            except Exception as e:  # noqa: BLE001
                errs += 1
                ui.toast(f"Start failed for {n}: {e!r}")
        if errs == 0:
            ui.toast(f"Started service {key} ({len(names)} stream(s))")

    def stop_selected():
        if not fm:
            ui.toast("FeedManager unavailable");
            return
        key = _ensure_selected()
        if not key:
            return
        names = _matching_producers(fm, key)
        if not names:
            ui.toast(f"No producers found for {key}");
            return
        errs = 0
        for n in names:
            try:
                fm.stop_producer(n)
            except Exception as e:  # noqa: BLE001
                errs += 1
                ui.toast(f"Stop failed for {n}: {e!r}")
        if errs == 0:
            ui.toast(f"Stopped service {key} ({len(names)} stream(s))")

    def start_all():
        if not fm:
            ui.toast("FeedManager unavailable")
            return
        try:
            fm.start_all_producers()
            ui.toast("Started all producers")
        except Exception as e:  # noqa: BLE001
            ui.toast(f"Start all failed: {e!r}")

    def stop_all():
        if not fm:
            ui.toast("FeedManager unavailable")
            return
        try:
            fm.stop_all_producers()
            ui.toast("Stopped all producers")
        except Exception as e:  # noqa: BLE001
            ui.toast(f"Stop all failed: {e!r}")

    def reload_from_disk():
        # Just re-read specs JSON (manager no longer persists live changes here)
        set_refresh(refresh + 1)
        ui.toast("Reloaded specs")

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
