# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phmtoc.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import vax
import numpy as np
from typing import Dict, Any

class PHMTOC:
    """Parser for PHMTOC (table of contents) block with cached structure."""

    # Field names in order
    FIELDS = [
        "Version",
        "NMcPart",
        "NPhPSum",
        "NPhChrg",
        "NPhKlus",
        "NPhKTrk",
        "NPhWic",
        "NPhWMC",
        "NPhCrid",
        "NPhPoint",
        "NMCPnt",
        "NPhKMC1",
        "NPhKChrg",
        "NPhBm",
        "NPhEvCl",
        "NMCBeam",
        "NPhKElId",
        "NPhVxOv"
    ]

    # Total size in bytes (1 float + 17 ints = 18 * 4 bytes)
    RECORD_SIZE = 72

    def parse(self, buffer: DataBuffer) -> Dict[str, Any]:
        """Parse the PHMTOC block from buffer.

        Args:
            buffer: DataBuffer to read from

        Returns:
            Dictionary with PHMTOC fields
        """
        # Read all data as uint32
        data = np.frombuffer(buffer.read(self.RECORD_SIZE), dtype=np.uint32)

        # First field is a VAX float (Version)
        version = float(vax.from_vax32(data[0:1])[0])

        # Remaining fields are integers
        int_values = data[1:].view(np.int32)

        # Build result dictionary
        result = {"Version": version}
        for i, field in enumerate(self.FIELDS[1:]):
            result[field] = int(int_values[i])

        return result
