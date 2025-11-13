#!/usr/bin/python3

# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        inspect.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

from jazelle_stream import JazelleInputStream
from utils import print_phpsum
import sys

def print_phpsum(phpsum):
    # Header
    print("-" * (10 * len(phpsum)))
    header = "  " + "".join(f"{name:<10}" for name in phpsum.keys())
    print(header)
    print("-" * (10 * len(phpsum)))

    # Rows
    for row in zip(*phpsum.values()):
        line = "  "
        for v in row:
            if isinstance(v, float):
                line += f"{v:<10.3f}"
            else:
                line += f"{v:<10}"
        print(line)

    # Footer
    print("-" * (10 * len(phpsum)))


def main():
    # Input test file name
    infile = sys.argv[1] if len(sys.argv) > 1 else "/global/cfs/projectdirs/m5115/SLD/minidst/qf1065.qf1065$5nrec97v18_mdst_1$7b1"
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

                    if rec_no % 1000 == 0:
                        print("*"*100)
                        print(f"Record {rec_no}: Run #{event_info['run']}, "
                              f"Event #{event_info['event']}, "
                              f"Event Time {event_info['time']}")
                        print("*"*100)

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


                    if rec_no % 1000 == 0:
                        print_phpsum(phpsum)

                    # Get the rest
                    pass

            except EOFError:
                print(f"EOF reached after {rec_no} logical records")
                break
            except OSError as e:
                # Handle IOSYNCH1 / IOSYNCH2
                print(f"Error in record {rec_no}: {e}")
                break 

if '__main__' in __name__:
    main()
