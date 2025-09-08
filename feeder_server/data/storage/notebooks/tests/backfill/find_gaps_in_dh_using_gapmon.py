def _to_pandas(tbl) -> pd.DataFrame:
    """Convert a Deephaven table-like object to pandas.DataFrame.

    Tries these strategies in order:
      1) tbl.to_pandas()
      2) tbl.snapshot().to_pandas()
      3) manual extraction from snapshot-like objects exposing schema/num_rows/column(i)
    """
    # 1) direct
    try:
        if hasattr(tbl, "to_pandas"):
            return tbl.to_pandas()
    except Exception:
        pass

    # 2) snapshot
    snap = None
    try:
        if hasattr(tbl, "snapshot"):
            snap = tbl.snapshot()
            if hasattr(snap, "to_pandas"):
                return snap.to_pandas()
    except Exception:
        snap = None

    # 3) manual extraction from an Arrow-like snapshot
    src = snap if snap is not None else tbl
    try:
        # schema names
        names = None
        if hasattr(src, "schema") and getattr(src, "schema") is not None:
            names = getattr(src, "schema").names
        elif hasattr(src, "column_names"):
            names = getattr(src, "column_names")
        # num_rows
        num_rows = getattr(src, "num_rows", None)
        if names is None or num_rows is None:
            raise RuntimeError("Table snapshot missing schema/num_rows for manual extraction")

        rows = []
        for i in range(int(num_rows)):
            row = {}
            for j, name in enumerate(names):
                try:
                    col = src.column(j)
                    cell = col[i]
                    val = cell.as_py() if hasattr(cell, "as_py") else cell
                except Exception:
                    val = None
                row[name] = val
            rows.append(row)
        return pd.DataFrame(rows)
    except Exception as e:
        raise RuntimeError("Failed to convert table to pandas: " + repr(e))
