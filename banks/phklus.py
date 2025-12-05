# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phklus.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import vax
import numpy as np

def parse_phklus(buffer: DataBuffer, n: int) -> np.ndarray:
    # Define the structure
    dtype = np.dtype([
        ("id",     np.int32),
        ("status", np.int32),
        ("eraw",   np.float32),
        ("cth",    np.float32),
        ("wcth",   np.float32),
        ("phi",    np.float32),
        ("wphi",   np.float32),
        ("elayer", np.float32, (8,)),
        ("nhit2",  np.int32),
        ("cth2",   np.float32),
        ("wcth2",  np.float32),
        ("phi2",   np.float32),
        ("wphi2",  np.float32),
        ("nhit3",  np.int32),
        ("cth3",   np.float32),
        ("wcth3",  np.float32),
        ("phi3",   np.float32),
        ("wphi3",  np.float32)
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
    float_mask = np.ones(25, dtype=bool)
    float_mask[[0, 1, 15, 20]] = False
    ieee_floats = vax.from_vax32(arr_uint32[:, float_mask])

    # Now assign the columns and return the result
    result["id"]     = arr_uint32[:,0].view(np.int32)
    result["status"] = arr_uint32[:,1].view(np.int32)
    for i, f in enumerate(["eraw","cth","wcth","phi","wphi"]):
        result[f] = ieee_floats[:, i]
    result["elayer"] = ieee_floats[:, 5:13].reshape(n, 8)
    result["nhit2"]  = arr_uint32[:, 15].view(np.int32)
    for i, f in enumerate(["cth2","wcth2","phi2","wphi2"]):
        result[f] = ieee_floats[:, i + 13]
    result["nhit3"]  = arr_uint32[:, 20].view(np.int32)
    for i, f in enumerate(["cth3","wcth3","phi3","wphi3"]):
        result[f] = ieee_floats[:, i + 17]

    return result

