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
	def make_json_safe(obj):
		if isinstance(obj, dict):
			return {k: make_json_safe(v) for k, v in obj.items()}
		elif isinstance(obj, (list, tuple)):
			return [make_json_safe(v) for v in obj]
		elif hasattr(obj, 'isoformat'):
			return obj.isoformat()
		elif isinstance(obj, (str, int, float, bool)) or obj is None:
			return obj
		else:
			return str(obj)

	try:
		w = get_eventlog_writer()
		try:
			meta_json = json.dumps(make_json_safe(meta or {}), separators=(",", ":"))
		except Exception as e:
			meta_json = f"{e}"

		w.write_row(
			to_j_instant(datetime.now(timezone.utc)),
			service,
			name,
			role,
			level,
			code,
			message,
			meta_json,
			corr_id or "",
			parent_corr_id or "",
		)
	except Exception:
		pass
