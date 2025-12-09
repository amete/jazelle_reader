# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phkelid.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import vax
import numpy as np
import logging

logger = logging.getLogger(__name__)

class PHKELID:
    """Parser for PHKELID bank data with cached dtype definitions."""

    # Class-level constants defined once
    DTYPE = np.dtype([
        ("id",        np.int32),
        ("idstat",    np.int16),
        ("prob",      np.int16),
        ("phi",       np.float32),
        ("theta",     np.float32),
        ("qp",        np.float32),
        ("dphi",      np.float32),
        ("dtheta",    np.float32),
        ("dqp",       np.float32),
        ("tphi",      np.float32),
        ("ttheta",    np.float32),
        ("isolat",    np.float32),
        ("em1",       np.float32),
        ("em12",      np.float32),
        ("dem12",     np.float32),
        ("had1",      np.float32),
        ("emphi",     np.float32),
        ("emtheta",   np.float32),
        ("phiwid",    np.float32),
        ("thewid",    np.float32),
        ("em1x1",     np.float32),
        ("em2x2a",    np.float32),
        ("em2x2b",    np.float32),
        ("em3x3a",    np.float32),
        ("em3x3b",    np.float32)
    ])

    DTYPE_RAW = np.dtype([
        ("id",        "<i4"),
        ("idstat",    "<i2"),
        ("prob",      "<i2"),
        ("phi",       "<u4"),
        ("theta",     "<u4"),
        ("qp",        "<u4"),
        ("dphi",      "<u4"),
        ("dtheta",    "<u4"),
        ("dqp",       "<u4"),
        ("tphi",      "<u4"),
        ("ttheta",    "<u4"),
        ("isolat",    "<u4"),
        ("em1",       "<u4"),
        ("em12",      "<u4"),
        ("dem12",     "<u4"),
        ("had1",      "<u4"),
        ("emphi",     "<u4"),
        ("emtheta",   "<u4"),
        ("phiwid",    "<u4"),
        ("thewid",    "<u4"),
        ("em1x1",     "<u4"),
        ("em2x2a",    "<u4"),
        ("em2x2b",    "<u4"),
        ("em3x3a",    "<u4"),
        ("em3x3b",    "<u4")
    ])

    INT_FIELDS = ["id", "idstat", "prob"]

    # VAX float fields in order they appear, with their sizes
    VAX_FIELD_INFO = [
        ("phi", 1), ("theta", 1), ("qp", 1), ("dphi", 1), ("dtheta", 1),
        ("dqp", 1), ("tphi", 1), ("ttheta", 1), ("isolat", 1), ("em1", 1),
        ("em12", 1), ("dem12", 1), ("had1", 1), ("emphi", 1), ("emtheta", 1),
        ("phiwid", 1), ("thewid", 1), ("em1x1", 1), ("em2x2a", 1),
        ("em2x2b", 1), ("em3x3a", 1), ("em3x3b", 1)
    ]

    def __init__(self):
        """Initialize parser with pre-computed record size."""
        self.record_size = self.DTYPE_RAW.itemsize

    def parse(self, buffer: DataBuffer, n: int) -> np.ndarray:
        """Parse n PHKELID records from buffer.

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
                f"Insufficient buffer data for PHKELID: need {required_bytes} bytes, "
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
            logger.error(f"Error parsing PHKELID bank with {n} records: {e}")
            raise RuntimeError(f"Failed to parse PHKELID bank: {e}") from e