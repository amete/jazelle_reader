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
        Reads a 4-byte VAX F-floating value from a binary file-like object
        and converts it to a standard IEEE 32-bit float.
        """
        # Read 4 bytes from file
        data = self.read(4)
        if len(data) != 4:
            raise EOFError("Not enough bytes to read a VAX float")
    
        # VAX F-float is little-endian but words are swapped (middle-endian)
        # Split into two 16-bit words and swap
        w1, w2 = struct.unpack('<HH', data)
        vax_int = (w2 << 16) | w1  # Combine words in proper order
    
        if vax_int == 0:
            return 0.0
    
        # Extract sign (1 bit), exponent (8 bits), fraction/mantissa (23 bits)
        sign = (vax_int >> 31) & 0x1
        exp = (vax_int >> 23) & 0xff
        fraction = vax_int & 0x7fffff
    
        # Adjust exponent from VAX bias (128) to IEEE bias (127)
        ieee_exp = exp - 128 + 127
    
        # Handle underflow / overflow
        if ieee_exp <= 0:
            # Subnormal values
            ieee_exp = 0
        elif ieee_exp >= 0xff:
            # Overflow to infinity
            ieee_exp = 0xff
            fraction = 0
    
        # Construct IEEE 32-bit integer representation
        ieee_bits = (sign << 31) | (ieee_exp << 23) | fraction
    
        # Convert to float
        return struct.unpack('>f', struct.pack('>I', ieee_bits))[0]

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
