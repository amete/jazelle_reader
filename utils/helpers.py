# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        helpers.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import List, Dict, Any

# Arrow builder / writer
def build_arrow_table(events: List[Dict[str, Any]]) -> pa.Table:
    """
    Convert list-of-event dicts into a PyArrow Table with nested list<struct<...>> columns.
    This function detects nested columns automatically based on Python list-of-dict.
    """
    if not events:
        # Return empty table with no columns
        return pa.table({})

    # Collect scalar column names (non-list)
    sample = events[0]
    scalar_cols = {k for k, v in sample.items() if not isinstance(v, np.ndarray)}
    nested_cols = [k for k in sample.keys() if k not in scalar_cols]

    # Build mapping col_name -> pyarrow.Array
    arrow_cols: Dict[str, pa.Array] = {}

    # Scalars: collect column values and let pyarrow infer the type
    for col in sorted(scalar_cols):
        vals = [ev.get(col) for ev in events]
        arrow_cols[col] = pa.array(vals)

    # Nested: each value is a list-of-dicts (or empty list)
    for col in sorted(nested_cols):
        nested_vals = [ev.get(col, []) for ev in events]
        # If lists contain dicts, pa.array will create list<struct<...>> automatically
        arrow_cols[col] = pa.array(nested_vals)

    # Build table preserving insertion order
    return pa.table(arrow_cols)


def write_parquet(table: pa.Table, out_path: Path, compression: str = "zstd") -> None:
    """Write the PyArrow table to Parquet with given compression."""
    # If user wants 'zstd', pyarrow accepts 'zstd' as compression string.
    pq.write_table(table, where=str(out_path), compression=compression, use_dictionary=True)
