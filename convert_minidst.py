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
    """
    stream = JazelleInputStream(fobj)

    rec_no = 0
    events: List[Dict[str, Any]] = []

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
                if stream.get_n_bytes() != record["usroff"]:
                    raise ValueError("Inconsistent usroff")

                event_info = parse_event_header(stream)

                if rec_no % print_interval == 0:
                    print(f"Record {rec_no}: Run {event_info['run']}, Event {event_info['event']}, Time {event_info['time']}")

            # Event data (MINIDST)
            if record["format"] == "MINIDST":
                if stream.get_n_bytes() != record["tocoff1"]:
                    raise ValueError("Inconsistent tocoff1")

                # This serves as a "table of contents" for the MiniDST
                # See: https://www-sld.slac.stanford.edu/sldwww/compress.html
                buffer = DataBuffer(stream.read(72))
                toc = phmtoc.parse(buffer)

                if record["datrec"] > 0:
                    # move to physical record containing data payload
                    stream.next_physical_record()

                if stream.get_n_bytes() != record["datoff"]:
                    raise ValueError("Inconsistent datoff")

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
                assert not toc["NMcPart"]

                # Parse PHPSUM
                particles = phpsum.parse(buffer,toc['NPhPSum'])

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

                # skip/parse any remaining banks here (PHKTRK, PHCRID, etc.)
                pass

        except EOFError:
            # We're done processing...
            break
        except OSError as e:
            print(f"\nOS error in record {rec_no}: {e}")
            break
        except Exception as e:
            print(f"\nUnexpected error in record {rec_no}: {e}")
            if verbose:
                import traceback
                traceback.print_exc()
            break

    return events


# CLI / main flow
def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Convert MiniDST to nested Parquet")
    parser.add_argument("input", help="Input MiniDST file", nargs="?")
    parser.add_argument("-o", "--output-dir", type=str, default=None, help="Output directory")
    parser.add_argument("-c", "--compression", type=str, default="zstd", help="Parquet compression (snappy, zstd, gzip, etc.)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--print-interval", type=int, default=10000, help="Progress print interval")
    args = parser.parse_args(argv)

    if not args.input:
        parser.print_help()
        return None

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Output directory
    if args.output_dir:
        outdir = Path(args.output_dir)
        outdir.mkdir(parents=True, exist_ok=True)
    else:
        outdir = input_path.parent

    # Read events
    print(f"Converting Jazelle file: {input_path}")
    with open(input_path, "rb") as f:
        events = read_events_from_stream(f, verbose=args.verbose, print_interval=args.print_interval)

    # Build Arrow table
    table = build_arrow_table(events)
    if args.verbose:
        print(f"Arrow table columns: {list(table.schema.names)}")
        print(table.schema)  # helpful for debugging the nested schema

    # Output file
    input_name = input_path.name.replace("$", "_")
    out_file = outdir / f"{input_name}.parquet"

    print(f"\nWriting Parquet to {out_file} (compression={args.compression}) ...")
    write_parquet(table, out_file, compression=args.compression)

    file_size_mb = out_file.stat().st_size / 1024.0 / 1024.0
    print(f"Saved: {out_file} ({file_size_mb:.2f} MB)")

    print("\nTo read back in pandas:")
    print(f"  import pandas as pd\n  df = pd.read_parquet('{out_file}')\n")


if __name__ == "__main__":
    main()
