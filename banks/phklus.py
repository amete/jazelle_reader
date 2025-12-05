# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phklus.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import vax
import numpy as np

class PHKLUS:
    """Parser for PHKLUS bank data with cached dtype definitions."""

    # Class-level constants defined once
    DTYPE = np.dtype([
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

    # Integer field positions in uint32 array
    INT_FIELDS = [
        (0, "id"),
        (1, "status"),
        (15, "nhit2"),
        (20, "nhit3")
    ]

    # VAX float field info: (start_idx_in_ieee, field_name, size)
    VAX_FIELDS = [
        (0, "eraw", 1),
        (1, "cth", 1),
        (2, "wcth", 1),
        (3, "phi", 1),
        (4, "wphi", 1),
        (5, "elayer", 8),
        (13, "cth2", 1),
        (14, "wcth2", 1),
        (15, "phi2", 1),
        (16, "wphi2", 1),
        (17, "cth3", 1),
        (18, "wcth3", 1),
        (19, "phi3", 1),
        (20, "wphi3", 1)
    ]

    # Pre-compute mask for float columns (exclude positions 0, 1, 15, 20)
    FLOAT_MASK = np.ones(25, dtype=bool)
    FLOAT_MASK[[0, 1, 15, 20]] = False

    def __init__(self):
        """Initialize parser with pre-computed sizes."""
        self.record_size = self.DTYPE.itemsize
        self.element_size = self.record_size // 4

    def parse(self, buffer: DataBuffer, n: int) -> np.ndarray:
        """Parse n PHKLUS records from buffer.

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

        # Convert VAX floats (all non-integer columns)
        ieee_floats = vax.from_vax32(arr_uint32[:, self.FLOAT_MASK])

        # Allocate result and fill
        result = np.empty(n, dtype=self.DTYPE)

        # Fill integer fields
        for pos, field in self.INT_FIELDS:
            result[field] = arr_uint32[:, pos].view(np.int32)

        # Fill float fields
        for idx, field, size in self.VAX_FIELDS:
            if size == 1:
                result[field] = ieee_floats[:, idx]
            else:
                result[field] = ieee_floats[:, idx:idx+size].reshape(n, size)

        return result
