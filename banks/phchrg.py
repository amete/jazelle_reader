# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phchrg.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from typing import List, Dict
from stream.jazelle_stream import JazelleInputStream

def parse_phchrg(stream: JazelleInputStream, n: int) -> List[Dict]:
    """Parse N track entries."""
    out: List[Dict] = []
    for _ in range(n):
        ent = {
            "id":      stream.read_int(),
            "hlxpar":  list(map(lambda _: stream.read_float(), range(6))),
            "dhlxpar": list(map(lambda _: stream.read_float(), range(15))),
            "bnorm":   stream.read_float(),
            "impact":  stream.read_float(),
            "b3nrom":  stream.read_float(),
            "impact3": stream.read_float(),
            "charge":  stream.read_short(),
            "smwstat": stream.read_short(),
            "status":  stream.read_int(),
            "tkpar0":  stream.read_float(),
            "tkpar":   list(map(lambda _: stream.read_float(), range(5))),
            "dtkpar":  list(map(lambda _: stream.read_float(), range(15))),
            "length":  stream.read_float(),
            "chi2dt":  stream.read_float(),
            "imc":     stream.read_short(), 
            "ndfdt":   stream.read_short(),
            "nhit":    stream.read_short(),
            "nhite":   stream.read_short(),
            "nhitp":   stream.read_short(),
            "nmisht":  stream.read_short(),
            "nwrght":  stream.read_short(),
            "nhitv":   stream.read_short(),
            "chi2":    stream.read_float(),
            "chi2v":   stream.read_float(),
            "vxdhit":  stream.read_int(),
            "mustat":  stream.read_short(),
            "estat":   stream.read_short(),
            "dedx":    stream.read_int()
        }
        out.append(ent)
    return out
