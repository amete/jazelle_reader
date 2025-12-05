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

def numpy_struct_to_pyarrow_struct(arr: np.ndarray) -> pa.StructArray:
    """
    Convert a numpy structured array to a PyArrow StructArray.
    Handles nested array fields automatically.

    Args:
        arr: Numpy structured array (e.g., from PHPSUM, PHCHRG parsers)

    Returns:
        PyArrow StructArray where each record is a struct
    """
    fields = []
    arrays = []

    for name in arr.dtype.names:
        field_dtype = arr.dtype[name]
        col_data = arr[name]

        # Check if this is a multi-dimensional field (e.g., shape (n, 6))
        if field_dtype.shape:
            # Convert to list type for nested arrays
            pa_array = pa.array([row.tolist() for row in col_data])
            fields.append(pa.field(name, pa_array.type))
            arrays.append(pa_array)
        else:
            # Scalar field
            pa_array = pa.array(col_data)
            fields.append(pa.field(name, pa_array.type))
            arrays.append(pa_array)

    # Create struct array from fields
    struct_type = pa.struct(fields)
    return pa.StructArray.from_arrays(arrays, fields=fields)


def build_arrow_table(events: List[Dict[str, Any]]) -> pa.Table:
    """
    Convert list-of-event dicts into a PyArrow Table with nested list<struct<...>> columns.

    Handles both:
    - Scalar event-level columns (run, event, time, etc.)
    - Numpy structured arrays (particles, tracks, clusters) -> list<struct<...>>
    """
    if not events:
        # Return empty table with no columns
        return pa.table({})

    # Collect scalar column names (non-list)
    sample = events[0]

    # Categorize columns
    scalar_cols = {k for k, v in sample.items() if not isinstance(v, np.ndarray)}
    nested_cols = [k for k in sample.keys() if k not in scalar_cols]

    # Build mapping col_name -> pyarrow.Array
    arrow_cols: Dict[str, pa.Array] = {}

    # Scalars: collect column values and let pyarrow infer the type
    for col in sorted(scalar_cols):
        vals = [ev.get(col) for ev in events]
        arrow_cols[col] = pa.array(vals)

    # Process numpy structured array columns -> list<struct<...>>
    for col in sorted(nested_cols):
        # For each event, convert the numpy structured array to a list of structs
        list_of_structs = []
        for ev in events:
            arr = ev.get(col)
            if arr is None or len(arr) == 0:
                # Empty list for this event
                list_of_structs.append([])
            else:
                # Convert numpy structured array to list of dicts for PyArrow
                # PyArrow can infer struct schema from list of dicts
                list_of_dicts = []
                for i in range(len(arr)):
                    record = {}
                    for name in arr.dtype.names:
                        val = arr[name][i]
                        # Convert numpy arrays to lists for PyArrow
                        if isinstance(val, np.ndarray):
                            record[name] = val.tolist()
                        else:
                            record[name] = val.item() if hasattr(val, 'item') else val
                    list_of_dicts.append(record)
                list_of_structs.append(list_of_dicts)

        arrow_cols[col] = pa.array(list_of_structs)

    # Build table preserving insertion order
    return pa.table(arrow_cols)


def write_parquet(table: pa.Table, out_path: Path, compression: str = "zstd") -> None:
    """Write the PyArrow table to Parquet with given compression."""
    # If user wants 'zstd', pyarrow accepts 'zstd' as compression string.
    pq.write_table(table, where=str(out_path), compression=compression, use_dictionary=True)
