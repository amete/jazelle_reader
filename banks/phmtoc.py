# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phmtoc.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from stream.jazelle_stream import JazelleInputStream
from typing import Any, Dict

def parse_phmtoc(stream: JazelleInputStream) -> Dict[str, Any]:
    """Parse the PHMTOC (table of contents) block and return as dict."""
    return {
        "Version":   stream.read_float(),
        "NMcPart":   stream.read_int(),
        "NPhPSum":   stream.read_int(),
        "NPhChrg":   stream.read_int(),
        "NPhKlus":   stream.read_int(),
        "NPhKTrk":   stream.read_int(),
        "NPhWic":    stream.read_int(),
        "NPhWMC":    stream.read_int(),
        "NPhCrid":   stream.read_int(),
        "NPhPoint":  stream.read_int(),
        "NMCPnt":    stream.read_int(),
        "NPhKMC1":   stream.read_int(),
        "NPhKChrg":  stream.read_int(),
        "NPhBm":     stream.read_int(),
        "NPhEvCl":   stream.read_int(),
        "NMCBeam":   stream.read_int(),
        "NPhKEl_id": stream.read_int(),
        "NPhVxOv":   stream.read_int()
    }
