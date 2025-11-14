# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        utils.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

def print_phpsum(phpsum: dict):
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
