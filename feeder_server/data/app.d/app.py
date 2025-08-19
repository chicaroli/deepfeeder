"""Deephaven app-mode entrypoint for deepfeeder.

This file imports the explicit binding packages that provide a minimal,
import-safe API for app-mode. Heavy side effects (starting feeders) are
only performed via explicit function calls.
"""

import os

# Import the public binding packages we provide under app.d/
# These are intentionally lightweight: they expose functions that construct
# or return tables lazily and provide explicit control (start/stop etc.).
import deepfeeder as dfb
# import fanout

print("[deepfeeder] deepfeeder package available as import deepfeeder as dfb")
print("[deepfeeder] fanout package available as import fanout")

# optional: autostart on boot (guarded by env)
if os.getenv("DEEPFEEDER_AUTOSTART", "1") not in ("0", "false", "False"):
    print("[deepfeeder] auto-starting configured feeders...")
    # explicit call into the package; import is safe and lazy
    print(dfb.start_all_autostart())

# Register UI components (importing the ui.dashboard module registers the
# dashboard with Deephaven UI). This is guarded by DEEPFEEDER_REGISTER_UI so
# you can disable UI registration in environments without Deephaven UI.
if os.getenv("DEEPFEEDER_REGISTER_UI", "1") not in ("0", "false", "False"):
    try:
        import ui.dashboard  # import for side effects: register dashboard
        FeederDashboard = ui.dashboard.FeederDashboard
        print("[deepfeeder] UI dashboard registered (import ui.dashboard)")

    except Exception as _e:
        # Log but do not fail startup if UI is not available in this runtime
        print(f"[deepfeeder] warning: ui.dashboard import failed: {_e}")
