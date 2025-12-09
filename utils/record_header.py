# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        record_header.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from stream.jazelle_stream import JazelleInputStream
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

# Record parsing helpers
def parse_record_header(stream: JazelleInputStream) -> Dict[str, Any]:
    """Parse record header.
    
    Args:
        stream: JazelleInputStream to read from
        
    Returns:
        Dictionary containing parsed record header fields
        
    Raises:
        EOFError: If insufficient data available in stream
    """
    try:
        return {
            "recno":    stream.read_int(),
            "t1":       stream.read_int(),
            "t2":       stream.read_int(),
            "target":   stream.read_int(),
            "rectype":  stream.read_string(8),
            "p1":       stream.read_int(),
            "p2":       stream.read_int(),
            "format":   stream.read_string(8),
            "context":  stream.read_string(8),
            "tocrec":   stream.read_int(),
            "datrec":   stream.read_int(),
            "tocsiz":   stream.read_int(),
            "datsiz":   stream.read_int(),
            "tocoff1":  stream.read_int(),
            "tocoff2":  stream.read_int(),
            "tocoff3":  stream.read_int(),
            "datoff":   stream.read_int(),
            "segname":  stream.read_string(8),
            "usrnam":   stream.read_string(8),
            "usroff":   stream.read_int(),
            "lrecflgs": stream.read_int(),
            "spare1":   stream.read_int(),
            "spare2":   stream.read_int()
        }
    except EOFError as e:
        logger.error(f"Failed to parse record header: insufficient data in stream")
        raise EOFError("Failed to parse record header: stream ended unexpectedly") from e
    except Exception as e:
        logger.error(f"Unexpected error parsing record header: {e}")
        raise ValueError(f"Failed to parse record header: {e}") from e
