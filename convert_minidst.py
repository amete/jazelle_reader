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

Requires:
    - pyarrow

Example:
    python convert_minidst.py input.minidst -o /path/to/out -c zstd
"""

# Standard libraries
import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# PyArrow
import pyarrow as pa
import pyarrow.parquet as pq

# Jazelle Stream
from stream.jazelle_stream import JazelleInputStream

# Bank Parsers
from banks.phmtoc import parse_phmtoc # Table of Contents
from banks.phpsum import parse_phpsum # Particle Summary
from banks.phchrg import parse_phchrg # Tracking Information
from banks.phklus import parse_phklus # Cluster Information

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


# High-level parser
def read_events_from_stream(fobj, verbose: bool = False, print_interval: int = 1000) -> List[Dict[str, Any]]:
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

    while True:
        try:
            stream.next_logical_record()
            rec_no += 1

            # Read TOC/record header fields
            record = {
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
                phmtoc = parse_phmtoc(stream)

                if record["datrec"] > 0:
                    # move to physical record containing data payload
                    stream.next_physical_record()

                if stream.get_n_bytes() != record["datoff"]:
                    raise ValueError("Inconsistent datoff")

                # Here one can read the whole data record
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
                _ = stream.read(20)

                # Ensure we're looking at data for now...
                assert not phmtoc["NMcPart"]

                # Parse PHPSUM
                particles: List[Dict[str, Any]] = []
                nParticles = phmtoc["NPhPSum"]
                if nParticles > 0:
                    particles = parse_phpsum(stream, nParticles)

                # Parse PHCHRG
                tracks: List[Dict[str, Any]] = []
                nTracks = phmtoc["NPhChrg"]
                if nTracks > 0:
                    tracks = parse_phchrg(stream, nTracks)

                # Parse PHKLUS
                clusters: List[Dict[str, Any]] = []
                nClusters = phmtoc["NPhKlus"]
                if nClusters > 0:
                    clusters = parse_phklus(stream, nClusters)

                # Build the event row (one dict per event)
                if event_info:
                    event_row: Dict[str, Any] = {
                        # Embed event info (flat scalars)
                        **{k: event_info[k] for k in event_info},
                        # Embed TOC fields (flat scalars)
                        **{k: phmtoc[k] for k in phmtoc},
                        # nested banks (list-of-structs)
                        "particles": particles,
                        "tracks" : tracks,
                        "clusters" : clusters,
                        # optionally include raw record metadata if you want:
                        # "rec_meta": record
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


# Arrow builder / writer
def build_arrow_table(events: List[Dict[str, Any]]) -> pa.Table:
    """
    Convert list-of-event dicts into a PyArrow Table with nested list<struct<...>> columns.
    This function detects nested columns automatically based on Python list-of-dict.
    """
    if not events:
        # Return empty table with no columns
        return pa.table({})

    # Collect scalar column names (non-list)
    sample = events[0]
    scalar_cols = {k for k, v in sample.items() if not isinstance(v, list)}
    nested_cols = [k for k in sample.keys() if k not in scalar_cols]

    # Build mapping col_name -> pyarrow.Array
    arrow_cols: Dict[str, pa.Array] = {}

    # Scalars: collect column values and let pyarrow infer the type
    for col in sorted(scalar_cols):
        vals = [ev.get(col) for ev in events]
        arrow_cols[col] = pa.array(vals)

    # Nested: each value is a list-of-dicts (or empty list)
    for col in sorted(nested_cols):
        nested_vals = [ev.get(col, []) for ev in events]
        # If lists contain dicts, pa.array will create list<struct<...>> automatically
        arrow_cols[col] = pa.array(nested_vals)

    # Build table preserving insertion order
    return pa.table(arrow_cols)


def write_parquet(table: pa.Table, out_path: Path, compression: str = "zstd") -> None:
    """Write the PyArrow table to Parquet with given compression."""
    # If user wants 'zstd', pyarrow accepts 'zstd' as compression string.
    pq.write_table(table, where=str(out_path), compression=compression, use_dictionary=True)


# CLI / main flow
def main(argv: Optional[List[str]] = None) -> Optional[pa.Table]:
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
    out_file = outdir / f"{input_name}_nested.parquet"

    print(f"\nWriting Parquet to {out_file} (compression={args.compression}) ...")
    write_parquet(table, out_file, compression=args.compression)

    file_size_mb = out_file.stat().st_size / 1024.0 / 1024.0
    print(f"Saved: {out_file} ({file_size_mb:.2f} MB)")

    print("\nTo read back in pandas:")
    print(f"  import pandas as pd\n  df = pd.read_parquet('{out_file}')\n")

    # Return the pyarrow table for programmatic use (or None if called from CLI)
    return table


if __name__ == "__main__":
    main()
