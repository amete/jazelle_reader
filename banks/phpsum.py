# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phpsum.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import vax
import numpy as np

def parse_phpsum(buffer: DataBuffer, n: int) -> np.ndarray:
    # Define the structure
    dtype = np.dtype([
        ("id",     np.int32),
        ("px",     np.float32),
        ("py",     np.float32),
        ("pz",     np.float32),
        ("x",      np.float32),
        ("y",      np.float32),
        ("z",      np.float32),
        ("charge", np.float32),
        ("status", np.int32)
    ])
    result = np.empty(n, dtype=dtype)

    # Return early if there is nothing to do
    if n == 0:
        return result

    # Read the buffer as raw uint32
    record_size = dtype.itemsize
    element_size = record_size // 4
    arr_uint32 = np.frombuffer(buffer.read(n*record_size), dtype=np.uint32).reshape(n, element_size)

    # Convert all VAX floats in bulk using from_vax32
    ieee_floats = vax.from_vax32(arr_uint32[:,1:8])

    # Now assign the columns and return the result
    result["id"] = arr_uint32[:,0].view(np.int32)
    for i, f in enumerate(["px","py","pz","x","y","z","charge"]):
        result[f] = ieee_floats[:, i]
    result["status"] = arr_uint32[:,8].view(np.int32)

    return result
