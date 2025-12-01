#!/usr/bin/python3

# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        convert_minidst.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from jazelle_stream import JazelleInputStream
from utils import print_phpsum
import pandas as pd
import sys
import argparse
from pathlib import Path

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Parse SLD MiniDST files and export to Parquet')
    parser.add_argument('input', nargs='?', 
                        default="/global/cfs/projectdirs/m5115/SLD/minidst/qf1065.qf1065$5nrec97v18_mdst_1$7b1",
                        help='Input MiniDST file')
    parser.add_argument('-o', '--output-dir', type=str, default=None,
                        help='Output directory for Parquet files (default: same as input)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print verbose output')
    parser.add_argument('--print-interval', type=int, default=1000,
                        help='Print progress every N records (default: 1000)')
    args = parser.parse_args()
    
    infile = args.input
    verbose = args.verbose
    print_interval = args.print_interval
    
    with open(infile, "rb") as f:
        stream = JazelleInputStream(f)

        # Print some information about the file
        print("="*100)
        print(f"File     : {infile}")
        print(f"Name     : {stream._name}")
        print(f"Created  : {stream._created}")
        print(f"Modified : {stream._modified}")
        print("="*100)

        # --- Iterate over logical records ---
        rec_no = 0
        
        # Collect all data
        all_records = []
        all_phmtoc = []
        all_phpsum = []

        while True:
            try:
                # Advance to next logical record
                stream.next_logical_record()
                rec_no += 1

                # Read fields from the current logical record
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

                # Initialize event_info for this record
                event_info = None

                # Read event information
                if record['usrnam'] == "IJEVHD":
                    if stream.get_n_bytes() != record['usroff']:
                        raise ValueError("Inconsistent usroff")

                    event_info = {
                        "header":  stream.read_int(),
                        "run":     stream.read_int(),
                        "event":   stream.read_int(),
                        "time":    stream.read_date(),
                        "weight":  stream.read_float(),
                        "type":    stream.read_int(),
                        "trigger": stream.read_int()
                    }

                    if rec_no % print_interval == 0:
                        print("*"*100)
                        print(f"Record {rec_no}: Run #{event_info['run']}, "
                              f"Event #{event_info['event']}, "
                              f"Event Time {event_info['time']}")
                        print("*"*100)

                    # Add record metadata with event info
                    record_with_event = {**record, **event_info}
                    all_records.append(record_with_event)

                # Read event data
                if record['format'] == "MINIDST":
                    if stream.get_n_bytes() != record['tocoff1']:
                        raise ValueError("Inconsistent tocoff1")

                    # This serves as a "table of contents" for the MiniDST
                    # See: https://www-sld.slac.stanford.edu/sldwww/compress.html
                    phmtoc = {
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

                    if(record['datrec']>0):
                        stream.next_physical_record()

                    if stream.get_n_bytes() != record['datoff']:
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

                    # Skip MCHEAD
                    _ = stream.read(20)

                    # Ensure we're looking at data for now...
                    assert(not phmtoc['NMcPart'])

                    # Get PHPSUM data
                    phpsum = {
                        "id": [],
                        "px": [],
                        "py": [],
                        "pz": [],
                        "x":  [],
                        "y":  [],
                        "z":  [],
                        "ch": [],
                        "st": [],
                    }

                    for idx in range(phmtoc['NPhPSum']):
                        phpsum["id"].append(stream.read_int())
                        phpsum["px"].append(stream.read_float())
                        phpsum["py"].append(stream.read_float())
                        phpsum["pz"].append(stream.read_float())
                        phpsum["x"].append(stream.read_float())
                        phpsum["y"].append(stream.read_float())
                        phpsum["z"].append(stream.read_float())
                        phpsum["ch"].append(stream.read_float())
                        phpsum["st"].append(stream.read_int())

                    if verbose and rec_no % print_interval == 0:
                        print_phpsum(phpsum)

                    # Add phmtoc with event info
                    if event_info:
                        phmtoc_with_event = {
                            'run': event_info['run'],
                            'event': event_info['event'],
                            'time': event_info['time'],
                            **phmtoc
                        }
                        all_phmtoc.append(phmtoc_with_event)

                        # Add phpsum particles with event info
                        for idx in range(phmtoc['NPhPSum']):
                            phpsum_particle = {
                                'run': event_info['run'],
                                'event': event_info['event'],
                                'time': event_info['time'],
                                'particle_idx': idx,
                                'id': phpsum["id"][idx],
                                'px': phpsum["px"][idx],
                                'py': phpsum["py"][idx],
                                'pz': phpsum["pz"][idx],
                                'x': phpsum["x"][idx],
                                'y': phpsum["y"][idx],
                                'z': phpsum["z"][idx],
                                'ch': phpsum["ch"][idx],
                                'st': phpsum["st"][idx]
                            }
                            all_phpsum.append(phpsum_particle)

                    # Get the rest
                    pass

            except EOFError:
                print(f"\nEOF reached after {rec_no} logical records")
                break
            except OSError as e:
                # Handle IOSYNCH1 / IOSYNCH2
                print(f"\nError in record {rec_no}: {e}")
                break
            except Exception as e:
                print(f"\nUnexpected error in record {rec_no}: {e}")
                if verbose:
                    import traceback
                    traceback.print_exc()
                break
        
        # Create DataFrames from collected data
        print("\nCreating DataFrames...")
        df_records = pd.DataFrame(all_records)
        df_phmtoc = pd.DataFrame(all_phmtoc)
        df_phpsum = pd.DataFrame(all_phpsum)
        
        print(f"\nProcessed {rec_no} logical records")
        print(f"  Records:  {len(df_records)} rows")
        print(f"  PHMTOCs:  {len(df_phmtoc)} rows")
        print(f"  PHPSUM:   {len(df_phpsum)} particles")
        
        if len(df_phpsum) > 0:
            print(f"\nPHPSUM Statistics:")
            print(f"  Runs: {df_phpsum['run'].nunique()}")
            print(f"  Events: {df_phpsum['event'].nunique()}")
            print(f"  Avg particles/event: {len(df_phpsum)/len(df_phmtoc):.1f}")
        
        # Merge all data into a single DataFrame
        print("\nMerging data into single DataFrame...")
        df_combined = df_phpsum.copy()
        
        # Add phmtoc columns (exclude redundant run/event/time)
        phmtoc_cols_to_add = [col for col in df_phmtoc.columns if col not in ['run', 'event', 'time']]
        df_combined = df_combined.merge(
            df_phmtoc[['run', 'event'] + phmtoc_cols_to_add],
            on=['run', 'event'],
            how='left'
        )
        
        # Add record columns (exclude redundant run/event/time and event_info fields)
        record_cols_to_add = [col for col in df_records.columns 
                             if col not in ['run', 'event', 'time', 'header', 'weight', 'type', 'trigger']]
        if len(df_records) > 0:
            df_combined = df_combined.merge(
                df_records[['run', 'event'] + record_cols_to_add],
                on=['run', 'event'],
                how='left'
            )
        
        print(f"Combined DataFrame: {len(df_combined)} rows × {len(df_combined.columns)} columns")
        
        # Determine output directory
        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
        else:
            output_dir = Path(infile).parent
        
        # Generate output filename
        input_name = Path(infile).stem.replace('$', '_')
        
        # Save to single parquet file
        output_file = output_dir / f"{input_name}_combined.parquet"
        
        print(f"\nSaving combined Parquet file...")
        df_combined.to_parquet(output_file, compression='snappy', index=False)
        
        # Calculate file size
        file_size = output_file.stat().st_size / 1024 / 1024
        
        print(f"\nSaved to:")
        print(f"  {output_file} ({file_size:.2f} MB)")
        print(f"\nTo load the data:")
        print(f"  import pandas as pd")
        print(f"  df = pd.read_parquet('{output_file.name}')")
        
        return df_combined

if '__main__' in __name__:
    main()
