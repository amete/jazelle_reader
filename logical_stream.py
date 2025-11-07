# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        logical_stream.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from physical_stream import PhysicalRecordInputStream
from typing import BinaryIO

class LogicalRecordInputStream(PhysicalRecordInputStream):

    def __init__(self, stream: BinaryIO):
        # Initialize data members
        self.to_be_continued = False

        # Construct the base class
        super().__init__(stream)

    def _read_header(self):
        """Reads both the physical and logical record headers and checks continuation flags."""
        super()._read_header()
        lrlen = self._read_short()
        lrcnt = self._read_short()
        if lrcnt & 0xFFFFFFFC != 0:
            raise IOError("IOSYNCH1")
        continued = (lrcnt & 2) != 0
        if continued != self.to_be_continued:
            raise IOError(f"IOSYNCH2 {continued} {self.to_be_continued}")
        self.to_be_continued = (lrcnt & 1) != 0

    def next_logical_record(self):
        """Skip physical records until the current logical record ends."""
        while self.to_be_continued:
            self.next_physical_record()
        self.next_physical_record()
