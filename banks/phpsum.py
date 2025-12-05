# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phpsum.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import vax
import numpy as np

class PHPSUM:
    """Parser for PHPSUM bank data with cached dtype definitions."""

    # Class-level constants defined once
    DTYPE = np.dtype([
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

    # Field names for VAX floats (in order)
    VAX_FIELDS = ["px", "py", "pz", "x", "y", "z", "charge"]

    def __init__(self):
        """Initialize parser with pre-computed sizes."""
        self.record_size = self.DTYPE.itemsize
        self.element_size = self.record_size // 4

    def parse(self, buffer: DataBuffer, n: int) -> np.ndarray:
        """Parse n PHPSUM records from buffer.

        Args:
            buffer: DataBuffer to read from
            n: Number of records to parse

        Returns:
            Structured numpy array with parsed data
        """
        if n == 0:
            return np.empty(0, dtype=self.DTYPE)

        # Read raw data as uint32
        arr_uint32 = np.frombuffer(
            buffer.read(n * self.record_size),
            dtype=np.uint32
        ).reshape(n, self.element_size)

        # Convert VAX floats (columns 1-7)
        ieee_floats = vax.from_vax32(arr_uint32[:, 1:8])

        # Allocate result and fill
        result = np.empty(n, dtype=self.DTYPE)
        result["id"] = arr_uint32[:, 0].view(np.int32)

        for i, field in enumerate(self.VAX_FIELDS):
            result[field] = ieee_floats[:, i]

        result["status"] = arr_uint32[:, 8].view(np.int32)

        return result
