# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        event_header.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from stream.jazelle_stream import JazelleInputStream
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

# Event parsing helpers
def parse_event_header(stream: JazelleInputStream) -> Dict[str, Any]:
    """Parse IJEVHD header (event metadata).
    
    Args:
        stream: JazelleInputStream to read from
        
    Returns:
        Dictionary containing parsed event header fields
        
    Raises:
        EOFError: If insufficient data available in stream
    """
    try:
        return {
            "header":  stream.read_int(),
            "run":     stream.read_int(),
            "event":   stream.read_int(),
            "time":    stream.read_date(),
            "weight":  stream.read_float(),
            "type":    stream.read_int(),
            "trigger": stream.read_int()
        }
    except EOFError as e:
        logger.error(f"Failed to parse event header: insufficient data in stream")
        raise EOFError("Failed to parse event header: stream ended unexpectedly") from e
    except Exception as e:
        logger.error(f"Unexpected error parsing event header: {e}")
        raise ValueError(f"Failed to parse event header: {e}") from e
