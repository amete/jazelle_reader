# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        data_buffer.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

class DataBuffer:
    """
    Minimal buffer wrapper with offset tracking.
    Holds a memoryview and allows direct offset manipulation.
    """
    
    def __init__(self, data: bytes):
        """Initialize with bytes data."""
        self.buffer = memoryview(data)
        self.offset = 0
        self.size = len(self.buffer)
    
    def read(self, n: int) -> memoryview:
        """Read n bytes and advance offset."""
        view = self.buffer[self.offset:self.offset + n]
        self.offset += n
        return view
    
    def skip(self, n: int):
        """Advance offset by n bytes without reading."""
        self.offset += n
    
    def remaining(self) -> int:
        """Return number of bytes remaining."""
        return self.size - self.offset
    
    def __len__(self) -> int:
        """Return total buffer size."""
        return self.size
