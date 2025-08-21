"""DeepFeeder UI package exports.

Provides modular access to dashboard components:

* controls.feeder_controls
* status_panel.status_panel
* eventlog.eventlog_table
* dashboard.FeederDashboard (composed)
"""

from .controls import feeder_controls  # noqa: F401
from .status_panel import status_panel  # noqa: F401
from .eventlog import eventlog_table, eventlog_formats  # noqa: F401
from .dashboard import FeederDashboard  # noqa: F401

__all__ = [
	"feeder_controls",
	"status_panel",
	"eventlog_table",
	"eventlog_formats",
	"FeederDashboard",
]
