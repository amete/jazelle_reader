#!/usr/bin/env python3

# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        convert_minidst.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

"""
convert_minidst.py

Refactored MiniDST -> Parquet pipeline that writes one Parquet row per event.
Each event contains scalar event-level columns and nested list-of-struct
columns for variable-length banks (PHPSUM, PHCHRG, PHKLUS, ...).

Example:
    python convert_minidst.py input.minidst -o /path/to/out -c zstd
"""

# Standard libraries
import argparse
import sys
import logging
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional

# Jazelle Stream
from stream.jazelle_stream import JazelleInputStream

# Bank Parsers
from banks.phmtoc import PHMTOC # Table of Contents
from banks.phpsum import PHPSUM # Particle Summary
from banks.phchrg import PHCHRG # Tracking Information
from banks.phklus import PHKLUS # Cluster Information

# Utils
from utils.data_buffer import DataBuffer
from utils.event_header import parse_event_header
from utils.record_header import parse_record_header
from utils.helpers import build_arrow_table, write_parquet

# Configure logging
logger = logging.getLogger(__name__)

# High-level parser
def read_events_from_stream(fobj: BinaryIO, verbose: bool = False, print_interval: int = 1000) -> List[Dict[str, Any]]:
    """
    Iterate over the JazelleInputStream and return a list of events in the form:
      {
        "run": int,
        "event": int,
        "time": datetime,
        ... scalar columns ...,
        "particles": [ {px,py,pz,...}, ... ],
        "tracks": [ {...}, ... ],
        "clusters": [ {...}, ... ],
        ...
      }
      
    Args:
        fobj: Binary file object to read from
        verbose: Enable verbose output
        print_interval: How often to print progress
        
    Returns:
        List of event dictionaries
    """
    try:
        stream = JazelleInputStream(fobj)
    except ValueError as e:
        logger.error(f"Invalid file format: {e}")
        raise ValueError("Input file is not in valid Jazelle format") from e
    except Exception as e:
        logger.error(f"Failed to initialize stream: {e}")
        raise RuntimeError(f"Failed to initialize stream: {e}") from e

    rec_no = 0
    events: List[Dict[str, Any]] = []
    
    # Track record types for debugging
    header_records = 0
    minidst_records = 0
    other_records: Dict[str, int] = {}  # format -> count

    # Bank Parsers
    phmtoc = PHMTOC()
    phpsum = PHPSUM()
    phchrg = PHCHRG()
    phklus = PHKLUS()

    while True:
        try:
            stream.next_logical_record()
            rec_no += 1

            # Read record header fields
            record = parse_record_header(stream)

            event_info: Optional[Dict[str, Any]] = None

            # Event header (IJEVHD)
            if record["usrnam"] == "IJEVHD":
                header_records += 1
                if stream.get_n_bytes() != record["usroff"]:
                    raise ValueError(
                        f"Inconsistent usroff at record {rec_no}: "
                        f"expected {record['usroff']}, got {stream.get_n_bytes()}"
                    )

                event_info = parse_event_header(stream)

                if rec_no % print_interval == 0:
                    logger.info(f"Record {rec_no}: Run {event_info['run']}, Event {event_info['event']}, Time {event_info['time']}")

            # Event data (MINIDST)
            if record["format"] == "MINIDST":
                minidst_records += 1
                if stream.get_n_bytes() != record["tocoff1"]:
                    raise ValueError(
                        f"Inconsistent tocoff1 at record {rec_no}: "
                        f"expected {record['tocoff1']}, got {stream.get_n_bytes()}"
                    )

                # This serves as a "table of contents" for the MiniDST
                # See: https://www-sld.slac.stanford.edu/sldwww/compress.html
                buffer = DataBuffer(stream.read(72))
                toc = phmtoc.parse(buffer)

                if record["datrec"] > 0:
                    # move to physical record containing data payload
                    stream.next_physical_record()

                if stream.get_n_bytes() != record["datoff"]:
                    raise ValueError(
                        f"Inconsistent datoff at record {rec_no}: "
                        f"expected {record['datoff']}, got {stream.get_n_bytes()}"
                    )

                # Read the entire record
                buffer = DataBuffer(stream.read(record['datsiz']))

                # Things seem to be broken down to the following data banks:
                #
                # MCHEAD
                # MCPART
                # PHPSUM
                # PHCHRG
                # PHKLUS
                # PHWIC
                # PHCRID
                # PHKTRK
                # PHKELID

                # Skip MCHEAD (20 bytes in original)
                buffer.skip(20)

                # Ensure we're looking at data for now...
                if toc["NMcPart"]:
                    raise ValueError(
                        f"Unexpected MC particle data in record {rec_no} "
                        f"(NMcPart={toc['NMcPart']}). MC data not supported."
                    )

                # Parse PHPSUM
                particles = phpsum.parse(buffer, toc['NPhPSum'])

                # Parse PHCHRG
                tracks    = phchrg.parse(buffer, toc['NPhChrg'])

                # Parse PHKLUS
                clusters  = phklus.parse(buffer, toc["NPhKlus"])

                # Build the event row (one dict per event)
                if event_info:
                    event_row: Dict[str, Any] = {
                        # Embed event info (flat scalars)
                        **{k: event_info[k] for k in event_info},
                        # nested banks (np.ndarrays)
                        "particles" : particles,
                        "tracks"    : tracks,
                        "clusters"  : clusters,
                    }
                    events.append(event_row)
                else:
                    logger.warning(f"Found MINIDST record {rec_no} without preceding IJEVHD header")

                # skip/parse any remaining banks here (PHKTRK, PHCRID, etc.)
                pass
            else:
                fmt = record["format"]
                other_records[fmt] = other_records.get(fmt, 0) + 1

        except EOFError:
            # We're done processing...
            logger.info(f"Reached end of file after {rec_no} records")
            break
        except ValueError as e:
            logger.error(f"Validation error in record {rec_no}: {e}")
            raise
        except BufferError as e:
            logger.error(f"Buffer error in record {rec_no}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in record {rec_no}: {e}")
            if verbose:
                import traceback
                logger.error(traceback.format_exc())
            raise

    logger.info(f"Successfully parsed {len(events)} events from {rec_no} total records")
    logger.info(f"Record breakdown: {header_records} headers (IJEVHD), {minidst_records} data (MINIDST), {sum(other_records.values())} other")
    if other_records:
        for fmt, count in sorted(other_records.items()):
            logger.info(f"  - {count} record(s) with format: '{fmt}'")
    if header_records != len(events):
        logger.warning(f"Mismatch: {header_records} header records but only {len(events)} events (unmatched headers or missing MINIDST data)")
    return events


# CLI / main flow
def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Convert MiniDST to nested Parquet")
    parser.add_argument("input", help="Input MiniDST file", nargs="?")
    parser.add_argument("-o", "--output-dir", type=str, default=None, help="Output directory")
    parser.add_argument("-c", "--compression", type=str, default="zstd", help="Parquet compression (snappy, zstd, gzip, etc.)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--print-interval", type=int, default=10000, help="Progress print interval")
    parser.add_argument("--log-level", type=str, default="INFO", 
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    args = parser.parse_args(argv)

    # Configure logging
    log_level = getattr(logging, args.log_level)
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s'
    )

    if not args.input:
        parser.print_help()
        sys.exit(1)

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    if not input_path.is_file():
        logger.error(f"Input path is not a file: {input_path}")
        sys.exit(1)

    # Output directory
    if args.output_dir:
        outdir = Path(args.output_dir)
        try:
            outdir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create output directory {outdir}: {e}")
            sys.exit(1)
    else:
        outdir = input_path.parent

    # Read events
    logger.info(f"Converting Jazelle file: {input_path}")
    try:
        with open(input_path, "rb") as f:
            events = read_events_from_stream(f, verbose=args.verbose, print_interval=args.print_interval)
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Invalid input file format: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to read events from file: {e}")
        sys.exit(1)

    if not events:
        logger.warning("No events were parsed from the input file")
        sys.exit(0)

    # Build Arrow table
    try:
        table = build_arrow_table(events)
        if args.verbose:
            logger.info(f"Arrow table columns: {list(table.schema.names)}")
            logger.debug(f"Table schema: {table.schema}")
    except Exception as e:
        logger.error(f"Failed to build Arrow table: {e}")
        sys.exit(1)

    # Output file
    input_name = input_path.name.replace("$", "_")
    out_file = outdir / f"{input_name}.parquet"

    logger.info(f"Writing Parquet to {out_file} (compression={args.compression})")
    try:
        write_parquet(table, out_file, compression=args.compression)
    except ValueError as e:
        logger.error(f"Invalid compression codec: {e}")
        sys.exit(1)
    except IOError as e:
        logger.error(f"I/O error writing Parquet file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to write Parquet file: {e}")
        sys.exit(1)

    try:
        file_size_mb = out_file.stat().st_size / 1024.0 / 1024.0
        logger.info(f"Saved: {out_file} ({file_size_mb:.2f} MB)")
    except OSError as e:
        logger.error(f"Failed to get output file size: {e}")
        sys.exit(1)

    logger.info("Conversion completed successfully")
    print("\nTo read back in pandas:")
    print(f"  import pandas as pd\n  df = pd.read_parquet('{out_file}')\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Conversion interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
