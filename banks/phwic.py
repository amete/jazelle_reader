# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phwic.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import vax
import numpy as np
import logging

logger = logging.getLogger(__name__)

class PHWIC:
    """Parser for PHWIC bank data with cached dtype definitions."""

    # Class-level constants defined once
    DTYPE = np.dtype([
        ("id",        np.int32),
        ("idstat",    np.int16),
        ("nhit",      np.int16),
        ("nhit45",    np.int16),
        ("npat",      np.int16),
        ("nhitpat",   np.int16),
        ("syshit",    np.int16),
        ("qpinit",    np.float32),
        ("t1",        np.float32),
        ("t2",        np.float32),
        ("t3",        np.float32),
        ("hitmiss",   np.int32),
        ("itrlen",    np.float32),
        ("nlayexp",   np.int16),
        ("nlaybey",   np.int16),
        ("missprob",  np.float32),
        ("phwicid",   np.int32),
        ("nhitshar",  np.int16),
        ("nother",    np.int16),
        ("hitsused",  np.int32),
        ("pref1",     np.float32, (3,)),
        ("pfit",      np.float32, (4,)),
        ("dpfit",     np.float32, (10,)),
        ("chi2",      np.float32),
        ("ndf",       np.int16),
        ("punfit",    np.int16),
        ("matchChi2", np.float32),
        ("matchNdf",  np.int16),
        ("padding",  "V2")
    ])

    DTYPE_RAW = np.dtype([
        ("id",        "<i4"),
        ("idstat",    "<i2"),
        ("nhit",      "<i2"),
        ("nhit45",    "<i2"),
        ("npat",      "<i2"),
        ("nhitpat",   "<i2"),
        ("syshit",    "<i2"),
        ("qpinit",    "<u4"),
        ("t1",        "<u4"),
        ("t2",        "<u4"),
        ("t3",        "<u4"),
        ("hitmiss",   "<i4"),
        ("itrlen",    "<u4"),
        ("nlayexp",   "<i2"),
        ("nlaybey",   "<i2"),
        ("missprob",  "<u4"),
        ("phwicid",   "<i4"),
        ("nhitshar",  "<i2"),
        ("nother",    "<i2"),
        ("hitsused",  "<i4"),
        ("pref1",     "<u4", (3,)),
        ("pfit",      "<u4", (4,)),
        ("dpfit",     "<u4", (10,)),
        ("chi2",      "<u4"),
        ("ndf",       "<i2"),
        ("punfit",    "<i2"),
        ("matchChi2", "<u4"),
        ("matchNdf",  "<i2"),
        ("padding",   "V2")
    ])

    INT_FIELDS = ["id", "idstat", "nhit", "nhit45", "npat", "nhitpat", "syshit",
                  "hitmiss", "nlayexp", "nlaybey", "phwicid", "nhitshar",
                  "nother", "hitsused", "ndf", "punfit", "matchNdf"]

    # VAX float fields in order they appear, with their sizes
    VAX_FIELD_INFO = [
        ("qpinit", 1), ("t1", 1), ("t2", 1), ("t3", 1), ("itrlen", 1),
        ("missprob", 1), ("pref1", 3), ("pfit", 4), ("dpfit", 10),
        ("chi2", 1), ("matchChi2", 1)
    ]

    def __init__(self):
        """Initialize parser with pre-computed record size."""
        self.record_size = self.DTYPE_RAW.itemsize

    def parse(self, buffer: DataBuffer, n: int) -> np.ndarray:
        """Parse n PHWIC records from buffer.

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
                f"Insufficient buffer data for PHWIC: need {required_bytes} bytes, "
                f"only {buffer.remaining()} available"
            )

        try:
            # Read raw data as structured array (uint32-backed for VAX words)
            arr_raw = np.frombuffer(
                buffer.read(required_bytes),
                dtype=self.DTYPE_RAW,
                count=n
            )

            # Compute total number of VAX words we need to convert
            total_vax = sum(size * n for (_field, size) in self.VAX_FIELD_INFO)

            # Preallocate a contiguous uint32 buffer and copy each field's words into it.
            vax_flat = np.empty(total_vax, dtype=np.uint32)
            pos = 0
            for field, size in self.VAX_FIELD_INFO:
                if size == 1:
                    src = arr_raw[field].astype(np.uint32, copy=False)
                    vax_flat[pos:pos + n] = src
                    pos += n
                else:
                    # arr_raw[field] has shape (n, size)
                    src = arr_raw[field].reshape(-1).astype(np.uint32, copy=False)
                    vax_flat[pos:pos + n * size] = src
                    pos += n * size

            # Convert VAX words to IEEE floats in one call
            ieee_flat = vax.from_vax32(vax_flat)

            # Allocate result and fill
            result = np.empty(n, dtype=self.DTYPE)

            # Copy integer fields directly (letting numpy handle dtype casts)
            for field in self.INT_FIELDS:
                result[field] = arr_raw[field]

            # Distribute converted floats from ieee_flat
            offset = 0
            for field, size in self.VAX_FIELD_INFO:
                count = n * size
                if size == 1:
                    result[field] = ieee_flat[offset:offset + count]
                else:
                    result[field] = ieee_flat[offset:offset + count].reshape(n, size)
                offset += count

            return result
        except Exception as e:
            logger.error(f"Error parsing PHWIC bank with {n} records: {e}")
            raise RuntimeError(f"Failed to parse PHWIC bank: {e}") from e