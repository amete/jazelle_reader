# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        jazelle_stream.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from datetime import datetime, timedelta
from logical_stream import LogicalRecordInputStream
from typing import BinaryIO
import struct
import utils

JAVA_EPOCH_OFFSET = 3506716800730

class JazelleInputStream(LogicalRecordInputStream):

    def __init__(self, stream: BinaryIO):
        # Construct the base class
        super().__init__(stream)

        # Check the data format
        if self.read_string(8) != "JAZELLE":
            raise ValueError("Input is not in JAZELLE format!")

        # Extract file metadata information
        self._ibmvax = self.read_int()
        self._created = self.read_date()
        self._modified = self.read_date()
        self._nmod = self.read_int()
        self._name = self.read_string(80)

    def read_integer(self, fmt: str) -> int:
        """
        Reads bytes from a file-like object and returns an integer.

        Args:
            f: File-like object to read from.
            fmt: struct format string (e.g., '<h', '<i', '<q').

        Returns:
            The unpacked integer.

        Raises:
            EOFError: If not enough bytes are available.
        """
        size = struct.calcsize(fmt)
        data = self.read(size)
        if len(data) != size:
            raise EOFError(f"Not enough bytes to read format {fmt}")
        return struct.unpack(fmt, data)[0]

    def read_short(self) -> int:
        return self.read_integer('<h')

    def read_int(self) -> int:
        return self.read_integer('<i')

    def read_long(self) -> int:
        return self.read_integer('<q')

    def read_date(self) -> datetime:
        """
        Reads an 8-byte long from the file and converts it to a UTC datetime,
        assuming a Java-like timestamp with a custom epoch offset.
        """
        # Read raw value
        value = self.read_long()

        # Convert to milliseconds
        value //= 10000

        # Adjust for epoch
        value -= JAVA_EPOCH_OFFSET

        # Convert milliseconds to seconds for datetime
        return datetime.utcfromtimestamp(value / 1000)

    def read_float(self) -> float:
        """
        Reads a 4-byte floating value from a binary file-like object
        and converts it to a standard IEEE 32-bit float.
        This could be streamlined w/:
          import vax
          np_ieee_float = vax.from_vax32(vax_float)
        """
        # Read 4 bytes from file as integer
        value = self.read_int()

        # Return early if zero
        if value == 0:
            return 0.0

        # Extract the sign bit (bit 15)
        sign_bit = value & 0x8000

        # Extract exponent bits (bits 7–14)
        exp_bits = value & 0x7f80
        exp_bits -= 2 << 7  # This is 256

        # Extract mantissa: combine low 7 bits of lower word with upper 16 bits
        mantissa_bits = ((value & 0x7f) << 16) | ((value & 0xffff0000) >> 16)
        # Note: The following emulates the operator precedence bug from the original
        #       mantissa_bits = ((value & 0x7f) << (16 + (value & 0xffff0000))) >> 16
        #       However it creates astronomical numbers, so here is a faster approximation
        #       mantissa_bits = (((value & 0x7f) << ((16 + (value & 0xffff0000)) & 0x1F)) & 0xFFFFFFFF) >> 16

        # Combine sign, exponent, and mantissa into 32-bit float representation
        float_bits = (sign_bit << 16) | (exp_bits << 16) | mantissa_bits

        # Ensure it's treated as a 32-bit integer
        float_bits = float_bits & 0xffffffff

        return struct.unpack('!f', struct.pack('!I', float_bits))[0]

    def read_string(self, size, encoding='ascii'):
        """
        Reads a fixed-length string from a binary file.

        Args:
            size: total number of bytes to read
            encoding: text encoding (default: 'ascii')

        Returns:
            Decoded string with trailing whitespace removed.
        """
        data = self.read(size)
        if len(data) < size:
            raise EOFError(f"Expected {size} bytes, got {len(data)}")
        return data.decode(encoding, errors='replace').rstrip()
