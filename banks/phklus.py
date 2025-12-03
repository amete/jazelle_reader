# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phklus.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from typing import List, Dict
from stream.jazelle_stream import JazelleInputStream

def parse_phklus(stream: JazelleInputStream, n: int) -> List[Dict]:
    """Parse N cluster entries."""
    out: List[Dict] = []
    for _ in range(n):
        ent = {
            "id":     stream.read_int(),
            "status": stream.read_int(),
            "eraw":   stream.read_float(),
            "cth":    stream.read_float(),
            "wcth":   stream.read_float(),
            "phi":    stream.read_float(),
            "wphi":   stream.read_float(),
            "elayer": list(map(lambda _: stream.read_float(), range(8))),
            "nhit2":  stream.read_int(),
            "cth2":   stream.read_float(),
            "wcth2":  stream.read_float(),
            "phi2":   stream.read_float(),
            "whphi2": stream.read_float(),
            "nhit3":  stream.read_int(),
            "cth3":   stream.read_float(),
            "wcth3":  stream.read_float(),
            "phi3":   stream.read_float(),
            "wphi3":  stream.read_float()
        }
        out.append(ent)
    return out
