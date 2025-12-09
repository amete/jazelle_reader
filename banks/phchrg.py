# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phchrg.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import vax
import numpy as np
import logging

logger = logging.getLogger(__name__)

class PHCHRG:
    """Parser for PHCHRG bank data with cached dtype definitions."""

    # Class-level constants defined once
    DTYPE = np.dtype([
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

    DTYPE_RAW = np.dtype([
        ("id",      "<i4"),
        ("hlxpar",  "<u4", (6,)),
        ("dhlxpar", "<u4", (15,)),
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

    INT_FIELDS = ["id", "charge", "smwstat", "status", "imc", "ndfdt", "nhit",
                  "nhite", "nhitp", "nmisht", "nwrght", "nhitv", "vxdhit",
                  "mustat", "estat", "dedx"]

    # VAX float fields in order they appear, with their sizes
    VAX_FIELD_INFO = [
        ("hlxpar", 6), ("dhlxpar", 15), ("bnorm", 1), ("impact", 1),
        ("b3nrom", 1), ("impact3", 1), ("tkpar0", 1), ("tkpar", 5),
        ("dtkpar", 15), ("length", 1), ("chi2dt", 1), ("chi2", 1), ("chi2v", 1)
    ]

    def __init__(self):
        """Initialize parser with pre-computed record size."""
        self.record_size = self.DTYPE_RAW.itemsize

    def parse(self, buffer: DataBuffer, n: int) -> np.ndarray:
        """Parse n PHCHRG records from buffer.

        Args:
            buffer: DataBuffer to read from
            n: Number of records to parse

        Returns:
            Structured numpy array with parsed data
            
        Raises:
            ValueError: If buffer has insufficient data for n records
        """
        if n == 0:
            return np.empty(0, dtype=self.DTYPE)

        required_bytes = n * self.record_size
        if buffer.remaining() < required_bytes:
            raise ValueError(
                f"Insufficient buffer data for PHCHRG: need {required_bytes} bytes, "
                f"only {buffer.remaining()} available"
            )

        try:
            # Read raw data
            arr_raw = np.frombuffer(
                buffer.read(required_bytes),
                dtype=self.DTYPE_RAW,
                count=n
            )

            # Collect all VAX values
            vax_arrays = []
            for field, size in self.VAX_FIELD_INFO:
                if size == 1:
                    vax_arrays.append(arr_raw[field])
                else:
                    vax_arrays.append(arr_raw[field].ravel())

            vax_flat = np.concatenate(vax_arrays)
            ieee_flat = vax.from_vax32(vax_flat)

            # Allocate result and fill
            result = np.empty(n, dtype=self.DTYPE)

            # Copy integer fields
            for field in self.INT_FIELDS:
                result[field] = arr_raw[field]

            # Distribute converted floats
            offset = 0
            for field, size in self.VAX_FIELD_INFO:
                count = n * size
                if size == 1:
                    result[field] = ieee_flat[offset:offset+count]
                else:
                    result[field] = ieee_flat[offset:offset+count].reshape(n, size)
                offset += count

            return result
        except Exception as e:
            logger.error(f"Error parsing PHCHRG bank with {n} records: {e}")
            raise RuntimeError(f"Failed to parse PHCHRG bank: {e}") from e
