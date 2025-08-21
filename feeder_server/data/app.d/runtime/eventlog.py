import json
from typing import Optional, Mapping
from datetime import datetime, timezone
from deephaven.time import to_j_instant
from runtime.eventlog_bus import get_eventlog_writer

def emit_event(
	service: str,
	name: str,
	role: str,
	level: str,
	code: str,
	message: str,
	meta: Optional[Mapping] = None,
	corr_id: str = "",
	parent_corr_id: str = "",
):
	"""Append a single event row to the global event log.

	Best-effort: any exception during write is swallowed so callers are not impacted.
	"""
	try:
		w = get_eventlog_writer()
		w.write_row(
			to_j_instant(datetime.now(timezone.utc)),
			service,
			name,
			role,
			level,
			code,
			message,
			json.dumps(meta or {}, separators=(",", ":")),
			corr_id or "",
			parent_corr_id or "",
		)
	except Exception:
		pass
