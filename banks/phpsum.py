# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phpsum.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from typing import List, Dict
from stream.jazelle_stream import JazelleInputStream

def parse_phpsum(stream: JazelleInputStream, n: int) -> List[Dict]:
    """Parse N PHPSUM particle entries."""
    out: List[Dict] = []
    for _ in range(n):
        ent = {
            "id":     stream.read_int(),
            "px":     stream.read_float(),
            "py":     stream.read_float(),
            "pz":     stream.read_float(),
            "x":      stream.read_float(),
            "y":      stream.read_float(),
            "z":      stream.read_float(),
            "charge": stream.read_float(),
            "status": stream.read_int()
        }
        out.append(ent)
    return out
