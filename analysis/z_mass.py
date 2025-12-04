#!/usr/bin/env python3

# =============================================================================
#  Jazelle Reader — SLD MiniDST Stream Utilities
# =============================================================================
#  File:        z_mass.py
#  Author:      Alaettin Serhan Mete <amete@anl.gov>
# =============================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from scipy.optimize import curve_fit
from multiprocessing import Pool, cpu_count
import argparse

Z_mass = 91.1876  # Z boson mass in GeV

# Vectorized invariant mass
def invariant_mass_vectorized(px1, py1, pz1, px2, py2, pz2):
    E1 = np.sqrt(px1**2 + py1**2 + pz1**2)
    E2 = np.sqrt(px2**2 + py2**2 + pz2**2)
    return np.sqrt((E1 + E2)**2 - ((px1 + px2)**2 + (py1 + py2)**2 + (pz1 + pz2)**2))

# Breit-Wigner + exponential background
def bw_plus_exp(x, m0, gamma, A, B, C):
    bw = A * gamma**2 / ((x - m0)**2 + (gamma/2)**2)
    background = C * np.exp(-B * x)
    return bw + background

# Load all Parquet files
def load_all_files(input_dir=".", pattern="*.parquet"):
    input_dir = Path(input_dir)
    files = list(input_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found in {input_dir} matching {pattern}")

    dfs = []
    for f in files:
        print(f"Loading {f}")
        df = pd.read_parquet(f, columns = ["run","event","particles"])
        dfs.append(df)
    
    df_all = pd.concat(dfs, ignore_index=True)
    print(f"Total events loaded: {len(df_all)}")
    return df_all

# Worker function per event
def process_event(row):
    run = row["run"]
    event = row["event"]
    particles = row.get("particles", None)

    # Skip empty or missing particles
    if particles is None:
        return None

    # Convert to list if it's a NumPy array or Series
    if isinstance(particles, (np.ndarray, pd.Series)):
        particles = particles.tolist()

    if len(particles) == 0:
        return None

    # Convert list of dicts to DataFrame
    df_particles = pd.DataFrame(particles)
    if df_particles.empty:
        return None

    # Separate positive and negative
    df_pos = df_particles[df_particles["charge"] > 0]
    df_neg = df_particles[df_particles["charge"] < 0]
    if df_pos.empty or df_neg.empty:
        return None

    # Cross join
    pairs = df_pos.merge(df_neg, how="cross", suffixes=("_pos", "_neg"))

    # Invariant mass
    pairs["inv_mass"] = invariant_mass_vectorized(
        pairs["px_pos"], pairs["py_pos"], pairs["pz_pos"],
        pairs["px_neg"], pairs["py_neg"], pairs["pz_neg"]
    )
    pairs["mass_diff"] = np.abs(pairs["inv_mass"] - Z_mass)

    best = pairs.loc[pairs["mass_diff"].idxmin()]
    best_dict = best.to_dict()
    best_dict["run"] = run
    best_dict["event"] = event
    return best_dict

# Main workflow
def main(input_dir=".", output_dir=".", output_name="z_candidates", max_chunk_size=1000):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Loading input files...")
    df = load_all_files(input_dir)

    # Parallel processing in chunks
    results = []
    n_cores = cpu_count()
    print(f"Processing events in parallel using {n_cores} cores...")

    for i in range(0, len(df), max_chunk_size):
        chunk = df.iloc[i:i + max_chunk_size]
        event_dicts = chunk.to_dict(orient="records")
        with Pool(n_cores) as pool:
            chunk_results = pool.map(process_event, event_dicts)
        results.extend([r for r in chunk_results if r is not None])

    df_best = pd.DataFrame(results)
    print(f"Events with +- pairs: {len(df_best)}")

    # Columns to keep
    columns_to_keep = [
        "run", "event", "inv_mass", "mass_diff",
        "px_pos", "py_pos", "pz_pos",
        "px_neg", "py_neg", "pz_neg"
    ]
    df_best = df_best[[c for c in columns_to_keep if c in df_best.columns]]

    # Plot invariant mass histogram
    plt.figure(figsize=(8,6))
    counts, bins, _ = plt.hist(df_best["inv_mass"], bins=50, range=(40,140), alpha=0.6, label="SLD Data", edgecolor='black')
    plt.xlabel("Invariant Mass [GeV]")
    plt.ylabel("Counts")
    plt.title("Z Candidate Invariant Mass Distribution")
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend()

    # Fit BW + exp
    bin_centers = 0.5*(bins[1:] + bins[:-1])
    p0 = [Z_mass, 2.5, counts.max(), 0.05, counts.min()]
    try:
        popt, _ = curve_fit(bw_plus_exp, bin_centers, counts, p0=p0)
        x_fit = np.linspace(40, 140, 500)
        y_fit = bw_plus_exp(x_fit, *popt)
        # Combined fit
        plt.plot(x_fit, y_fit, "r-", label=f"BW+Exp Fit: m₀={popt[0]:.2f} GeV, γ={popt[1]:.2f} GeV")
        # Breit-Wigner only
        bw_only = popt[2] * popt[1]**2 / ((x_fit - popt[0])**2 + (popt[1]/2)**2)
        plt.plot(x_fit, bw_only, "b--", label="Breit-Wigner only")
        # Exponential background only
        exp_only = popt[4] * np.exp(-popt[3] * x_fit)
        plt.plot(x_fit, exp_only, "g:", label="Exponential background")
        plt.legend(loc="upper left", fontsize="small", facecolor='white')
        plt.tight_layout()
    except Exception as e:
        print(f"Fit failed: {e}")

    # Save outputs
    output_plot = output_dir / f"{output_name}.png"
    plt.savefig(output_plot, dpi=150)
    plt.close()
    print(f"Plot saved to {output_plot}")

    return df_best

# Run main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Z candidate selection with parallel processing")
    parser.add_argument("--input-dir", type=str, default=".", help="Directory with input Parquet files")
    parser.add_argument("--output-dir", type=str, default=".", help="Directory to save outputs")
    parser.add_argument("--output-name", type=str, default="z_candidates", help="Base name for output files")
    parser.add_argument("--chunk-size", type=int, default=100000, help="Number of events per chunk to limit memory usage")
    args = parser.parse_args()

    df_best = main(input_dir=args.input_dir,
                   output_dir=args.output_dir,
                   output_name=args.output_name,
                   max_chunk_size=args.chunk_size)
