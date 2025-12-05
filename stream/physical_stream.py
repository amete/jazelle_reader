# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        physical_stream.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from typing import BinaryIO
import io
import struct

class PhysicalRecordInputStream:

    def __init__(self, stream: BinaryIO):
        # Initialize data members
        self._in = stream
        self.n_bytes = 0
        self.reclen = 0
        self._read_header()

    def _read_short(self) -> int:
        data = self._in.read(2)
        if len(data) < 2:
            raise EOFError("Unexpected end of file while reading short")
        self.n_bytes += 2
        return struct.unpack("<H", data)[0]  # little-endian unsigned short

    def _read_header(self):
        self.n_bytes = 0
        self.reclen = self._read_short()
        if self.reclen < 0:
            raise EOFError("Negative record length encountered")
        _ = self._read_short()  # reserved field

    def read(self, size: int = 1) -> bytes:
        """Read up to 'size' bytes within the current record.
        Automatically cross record boundary if necessary (bulk)."""

        result = []

        while size > 0:
            # See how much data we have in the current record
            remaining = self.reclen - self.n_bytes

            # Rollover (when at the boundry, read the next record)
            if remaining == 0:
                self.next_physical_record()

            # Clamp the read
            to_read = min(size, remaining)

            # Read the actual data
            data = self._in.read(to_read)
            if len(data) != to_read:
                raise EOFError("Unexpected end of file while reading record.")

            # Now take care of accounting
            self.n_bytes += to_read
            size -= to_read
            result.append(data)

        return b"".join(result)

    def next_physical_record(self):
        remaining = self.reclen - self.n_bytes
        if remaining > 0:
            self._in.seek(remaining, io.SEEK_CUR)
        self._read_header()

    def get_n_bytes(self) -> int:
        return self.n_bytes
