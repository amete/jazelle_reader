# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phchrg.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import vax
import numpy as np

def parse_phchrg(buffer: DataBuffer, n: int) -> np.ndarray:
    # Define the structure
    dtype = np.dtype([
        ("id",      np.int32),
        ("hlxpar",  np.float32, (6,)),
        ("dhlxpar", np.float32, (15,)),
        ("bnorm",   np.float32),
        ("impact",  np.float32),
        ("b3nrom",  np.float32),
        ("impact3", np.float32),
        ("charge",  np.int16),
        ("smwstat", np.int16),
        ("status",  np.int32),
        ("tkpar0",  np.float32),
        ("tkpar",   np.float32, (5,)),
        ("dtkpar",  np.float32, (15,)),
        ("length",  np.float32),
        ("chi2dt",  np.float32),
        ("imc",     np.int16),
        ("ndfdt",   np.int16),
        ("nhit",    np.int16),
        ("nhite",   np.int16),
        ("nhitp",   np.int16),
        ("nmisht",  np.int16),
        ("nwrght",  np.int16),
        ("nhitv",   np.int16),
        ("chi2",    np.float32),
        ("chi2v",   np.float32),
        ("vxdhit",  np.int32),
        ("mustat",  np.int16),
        ("estat",   np.int16),
        ("dedx",    np.int32)
    ])
    result = np.empty(n, dtype=dtype)

    # Return early if there is nothing to do
    if n == 0:
        return result

    # Read the buffer as raw
    # This is not super nice since we mix different sizes
    record_size = dtype.itemsize
    element_size = 66
    dtype_raw = np.dtype([
        ("id",      "<i4"),
        ("hlxpar",  "<u4", (6,)),   # VAX floats as uint32
        ("dhlxpar", "<u4", (15,)),  # VAX floats as uint32
        ("bnorm",   "<u4"),
        ("impact",  "<u4"),
        ("b3nrom",  "<u4"),
        ("impact3", "<u4"),
        ("charge",  "<i2"),
        ("smwstat", "<i2"),
        ("status",  "<i4"),
        ("tkpar0",  "<u4"),
        ("tkpar",   "<u4", (5,)),
        ("dtkpar",  "<u4", (15,)),
        ("length",  "<u4"),
        ("chi2dt",  "<u4"),
        ("imc",     "<i2"),
        ("ndfdt",   "<i2"),
        ("nhit",    "<i2"),
        ("nhite",   "<i2"),
        ("nhitp",   "<i2"),
        ("nmisht",  "<i2"),
        ("nwrght",  "<i2"),
        ("nhitv",   "<i2"),
        ("chi2",    "<u4"),
        ("chi2v",   "<u4"),
        ("vxdhit",  "<i4"),
        ("mustat",  "<i2"),
        ("estat",   "<i2"),
        ("dedx",    "<i4")
    ])
    arr_raw = np.frombuffer(buffer.read(n*record_size), dtype=dtype_raw, count=n)

    # Convert all VAX floats in bulk using from_vax32
    vax_fields = ["hlxpar", "dhlxpar", "bnorm", "impact", "b3nrom", "impact3",
                  "tkpar0", "tkpar", "dtkpar", "length", "chi2dt", "chi2", "chi2v"]

    vax_values = []
    for field in vax_fields:
        if arr_raw.dtype[field].shape:  # Array field
            vax_values.append(arr_raw[field].ravel())
        else:  # Scalar field
            vax_values.append(arr_raw[field])

    vax_flat = np.concatenate(vax_values)
    ieee_flat = vax.from_vax32(vax_flat)

    # Now assign the columns and return the result
    int_field = ["id", "chi2v", "smwstat", "status", "imc", "ndfdt", "nhit",
                 "nhite", "nhitp", "nmisht", "nwrght", "nhitv", "vxdhit",
                 "mustat", "estat", "dedx"]
    for field in int_field:
        result[field] = arr_raw[field]

    offset = 0
    result["hlxpar"] = ieee_flat[offset:offset+n*6].reshape(n, 6)
    offset += n*6
    result["dhlxpar"] = ieee_flat[offset:offset+n*15].reshape(n, 15)
    offset += n*15
    result["bnorm"] = ieee_flat[offset:offset+n]
    offset += n
    result["impact"] = ieee_flat[offset:offset+n]
    offset += n
    result["b3nrom"] = ieee_flat[offset:offset+n]
    offset += n
    result["impact3"] = ieee_flat[offset:offset+n]
    offset += n
    result["tkpar0"] = ieee_flat[offset:offset+n]
    offset += n
    result["tkpar"] = ieee_flat[offset:offset+n*5].reshape(n, 5)
    offset += n*5
    result["dtkpar"] = ieee_flat[offset:offset+n*15].reshape(n, 15)
    offset += n*15
    result["length"] = ieee_flat[offset:offset+n]
    offset += n
    result["chi2dt"] = ieee_flat[offset:offset+n]
    offset += n
    result["chi2"] = ieee_flat[offset:offset+n]
    offset += n
    result["chi2v"] = ieee_flat[offset:offset+n]
    offset += n

    return result
