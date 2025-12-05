# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        event_header.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from stream.jazelle_stream import JazelleInputStream
from typing import Dict, Any

# Event parsing helpers
def parse_event_header(stream: JazelleInputStream) -> Dict[str, Any]:
    """Parse IJEVHD header (event metadata)."""
    return {
        "header":  stream.read_int(),
        "run":     stream.read_int(),
        "event":   stream.read_int(),
        "time":    stream.read_date(),
        "weight":  stream.read_float(),
        "type":    stream.read_int(),
        "trigger": stream.read_int()
    }
