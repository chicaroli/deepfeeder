# app.d/ingest/utils.py
from typing import Iterable, List, Union, Tuple, Optional

def normalize_symbols(syms: Union[str, Iterable[str]]) -> List[str]:
    if isinstance(syms, str):
        parts = syms.split(",")
    else:
        parts = syms
    return sorted({p.strip().lower() for p in parts if p and p.strip()})
