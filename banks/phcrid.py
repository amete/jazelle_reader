# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        phcrid.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from utils.data_buffer import DataBuffer
import vax
import numpy as np
import logging

logger = logging.getLogger(__name__)

class PHCRID:
    """Parser for PHCRID bank data with variable-length records.

    PHCRID has a complex structure with conditional sections based on control word flags:
    - Fixed header (16 bytes): id, norm (for PIDVEC), rc, geom, trkp, nhits
    - Liquid hypothesis section (4 or 36 bytes, full if bit 0x10000 set in ctlword)
    - Gas hypothesis section (4 or 36 bytes, full if bit 0x20000 set in ctlword)

    Both hypothesis sections are ALWAYS present, but their size depends on the flags.
    The norm value from the header is used internally to compute the combined llik PIDVEC.
    """

    # Fixed header structure (16 bytes)
    HEADER_DTYPE_RAW = np.dtype([
        ("id",   "<i4"),
        ("norm", "<u4"),  # Used internally to compute combined llik PIDVEC
        ("rc",   "<i2"),
        ("geom", "<i2"),
        ("trkp", "<i2"),
        ("nhits", "<i2")
    ])

    # CRIDHYP full structure (36 bytes) - when full=True
    CRIDHYP_FULL_DTYPE_RAW = np.dtype([
        ("llik_e",    "<u4"),
        ("llik_mu",   "<u4"),
        ("llik_pi",   "<u4"),
        ("llik_k",    "<u4"),
        ("llik_p",    "<u4"),
        ("rc",        "<i2"),
        ("nhits",     "<i2"),
        ("besthyp",   "<i4"),
        ("nhexp",     "<i2"),
        ("nhfnd",     "<i2"),
        ("nhbkg",     "<i2"),
        ("mskphot",   "<i2")
    ])

    # CRIDHYP short structure (4 bytes) - when full=False
    CRIDHYP_SHORT_DTYPE_RAW = np.dtype([
        ("rc",     "<i2"),
        ("nhits",  "<i2")
    ])

    def __init__(self):
        """Initialize parser."""
        self.header_size = self.HEADER_DTYPE_RAW.itemsize

    def parse(self, buffer: DataBuffer, n: int) -> list:
        """Parse n PHCRID records from buffer.

        Note: Returns a list of dicts rather than a structured array because
        records have variable length based on control word flags.

        Args:
            buffer: DataBuffer to read from
            n: Number of records to parse

        Returns:
            List of dictionaries with parsed data

        Raises:
            ValueError: If buffer has insufficient data
        """
        if n == 0:
            return []

        records = []

        for i in range(n):
            try:
                # Check we have enough for the header
                if buffer.remaining() < self.header_size:
                    raise ValueError(
                        f"Insufficient buffer data for PHCRID header {i+1}/{n}: "
                        f"need {self.header_size} bytes, only {buffer.remaining()} available"
                    )

                # Read fixed header
                header_bytes = buffer.read(self.header_size)
                header_raw = np.frombuffer(header_bytes, dtype=self.HEADER_DTYPE_RAW, count=1)[0]

                # Parse header fields and extract norm value for PIDVEC combination
                norm = float(vax.from_vax32(np.uint32(header_raw['norm'])))

                record = {
                    'id': int(header_raw['id']),
                    'rc': int(header_raw['rc']),
                    'geom': int(header_raw['geom']),
                    'trkp': int(header_raw['trkp']),
                    'nhits': int(header_raw['nhits'])
                }

                # Check control word flags
                ctlword = record['id']
                liq_present = (ctlword & 0x10000) != 0
                gas_present = (ctlword & 0x20000) != 0

                # Parse liquid hypothesis (ALWAYS present, but may be short or full)
                liq_hyp = self._parse_cridhyp(buffer, full=liq_present)
                record['liq_hyp'] = liq_hyp

                # Parse gas hypothesis (ALWAYS present, but may be short or full)
                gas_hyp = self._parse_cridhyp(buffer, full=gas_present)
                record['gas_hyp'] = gas_hyp

                # Compute combined log-likelihood PIDVEC
                liq_llik = liq_hyp['llik'] if liq_present else None
                gas_llik = gas_hyp['llik'] if gas_present else None
                record['llik'] = self._combine_pidvec(liq_llik, gas_llik, norm)

                records.append(record)

            except Exception as e:
                logger.error(f"Error parsing PHCRID record {i+1}/{n}: {e}")
                raise RuntimeError(f"Failed to parse PHCRID record {i+1}/{n}: {e}") from e

        return records

    def _parse_cridhyp(self, buffer: DataBuffer, full: bool) -> dict:
        """Parse a CRIDHYP hypothesis structure.

        Args:
            buffer: DataBuffer to read from
            full: If True, parse full 36-byte structure; if False, parse short 4-byte structure

        Returns:
            Dictionary with CRIDHYP data
        """
        if full:
            # Full structure: 36 bytes with PIDVEC (5 floats = 20 bytes) + additional fields
            if buffer.remaining() < 36:
                raise ValueError(f"Insufficient data for full CRIDHYP hypothesis (need 36 bytes)")

            cridhyp_bytes = buffer.read(36)
            cridhyp_raw = np.frombuffer(cridhyp_bytes, dtype=self.CRIDHYP_FULL_DTYPE_RAW, count=1)[0]

            # Convert VAX floats for PIDVEC (llik values)
            vax_words = np.array([
                cridhyp_raw['llik_e'],
                cridhyp_raw['llik_mu'],
                cridhyp_raw['llik_pi'],
                cridhyp_raw['llik_k'],
                cridhyp_raw['llik_p']
            ], dtype=np.uint32)

            ieee_floats = vax.from_vax32(vax_words)

            return {
                'llik': {
                    'e': float(ieee_floats[0]),
                    'mu': float(ieee_floats[1]),
                    'pi': float(ieee_floats[2]),
                    'k': float(ieee_floats[3]),
                    'p': float(ieee_floats[4])
                },
                'rc': int(cridhyp_raw['rc']),
                'nhits': int(cridhyp_raw['nhits']),
                'besthyp': int(cridhyp_raw['besthyp']),
                'nhexp': int(cridhyp_raw['nhexp']),
                'nhfnd': int(cridhyp_raw['nhfnd']),
                'nhbkg': int(cridhyp_raw['nhbkg']),
                'mskphot': int(cridhyp_raw['mskphot'])
            }
        else:
            # Short structure: 4 bytes
            if buffer.remaining() < 4:
                raise ValueError(f"Insufficient data for short CRIDHYP hypothesis (need 4 bytes)")

            cridhyp_bytes = buffer.read(4)
            cridhyp_raw = np.frombuffer(cridhyp_bytes, dtype=self.CRIDHYP_SHORT_DTYPE_RAW, count=1)[0]

            return {
                'llik': None,
                'rc': int(cridhyp_raw['rc']),
                'nhits': int(cridhyp_raw['nhits']),
                'besthyp': 0,
                'nhexp': 0,
                'nhfnd': 0,
                'nhbkg': 0,
                'mskphot': 0
            }

    def _combine_pidvec(self, liq_llik: dict, gas_llik: dict, norm: float) -> dict:
        """Combine liquid and gas PIDVEC with normalization.

        This mimics the Java PIDVEC(PIDVEC LIQ, PIDVEC GAS, float norm) constructor:
        - Initialize all fields to norm
        - Add LIQ values if not null
        - Add GAS values if not null

        Args:
            liq_llik: Liquid hypothesis PIDVEC dict or None
            gas_llik: Gas hypothesis PIDVEC dict or None
            norm: Normalization value

        Returns:
            Combined PIDVEC dictionary
        """
        result = {'e': norm, 'mu': norm, 'pi': norm, 'k': norm, 'p': norm}

        if liq_llik is not None:
            for key in result:
                result[key] += liq_llik[key]

        if gas_llik is not None:
            for key in result:
                result[key] += gas_llik[key]

        return result
