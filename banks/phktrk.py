# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phktrk.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import numpy as np
import logging

logger = logging.getLogger(__name__)

class PHKTRK:
    """Parser for PHKTRK bank data.
    
    Note: This bank appears to be a placeholder in the original Java implementation
    with no actual data structure defined (read() returns 0 bytes).
    Only the ID field is read.
    """

    DTYPE = np.dtype([
        ("id", np.int32)
    ])

    DTYPE_RAW = np.dtype([
        ("id", "<i4")
    ])

    def __init__(self):
        """Initialize parser with pre-computed record size."""
        self.record_size = self.DTYPE_RAW.itemsize

    def parse(self, buffer: DataBuffer, n: int) -> np.ndarray:
        """Parse n PHKTRK records from buffer.
        
        Args:
            buffer: DataBuffer to read from
            n: Number of records to parse

        Returns:
            Structured numpy array with ID field only
            
        Raises:
            ValueError: If buffer has insufficient data for n records
        """
        if n == 0:
            return np.empty(0, dtype=self.DTYPE)
        
        required_bytes = n * self.record_size
        if buffer.remaining() < required_bytes:
            raise ValueError(
                f"Insufficient buffer data for PHKTRK: need {required_bytes} bytes, "
                f"only {buffer.remaining()} available"
            )

        try:
            # Read raw data as structured array
            arr_raw = np.frombuffer(
                buffer.read(required_bytes),
                dtype=self.DTYPE_RAW,
                count=n
            )

            # Allocate result and copy ID field
            result = np.empty(n, dtype=self.DTYPE)
            result['id'] = arr_raw['id']

            return result
        except Exception as e:
            logger.error(f"Error parsing PHKTRK bank with {n} records: {e}")
            raise RuntimeError(f"Failed to parse PHKTRK bank: {e}") from e