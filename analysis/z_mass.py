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
from scipy.optimize import curve_fit
import argparse

Z_mass = 91.1876  # Z boson mass in GeV

# Vectorized invariant mass
def invariant_mass_vectorized(px1, py1, pz1, px2, py2, pz2):
    E1 = np.sqrt(px1**2 + py1**2 + pz1**2)
    E2 = np.sqrt(px2**2 + py2**2 + pz2**2)
    px_tot = px1 + px2
    py_tot = py1 + py2
    pz_tot = pz1 + pz2
    return np.sqrt((E1 + E2)**2 - (px_tot**2 + py_tot**2 + pz_tot**2))

# Breit-Wigner + Exp function for fitting
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
        df = pd.read_parquet(f)
        dfs.append(df)
    
    df_all = pd.concat(dfs, ignore_index=True)
    print(f"Total events loaded: {len(df_all)}")
    return df_all

# Main workflow
def main(input_dir=".", output_dir=".", output_name="z_candidates"):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Loading input files...")
    df = load_all_files(input_dir)

    print(f"Total unique events: {len(df)}")

    # Explode particles column
    print("Exploding particles...")
    df_exploded = df.explode("particles").reset_index(drop=True)
    particles_df = pd.json_normalize(df_exploded["particles"])
    df_particles = pd.concat([df_exploded.drop(columns=["particles"]), particles_df], axis=1)

    # Separate positives and negatives
    df_pos = df_particles[df_particles["ch"] > 0].copy()
    df_neg = df_particles[df_particles["ch"] < 0].copy()

    # Cartesian product per event
    df_pairs = df_pos.merge(df_neg, on=["run", "event"], suffixes=("_pos", "_neg"))

    # Compute invariant mass
    df_pairs["inv_mass"] = invariant_mass_vectorized(
        df_pairs["px_pos"], df_pairs["py_pos"], df_pairs["pz_pos"],
        df_pairs["px_neg"], df_pairs["py_neg"], df_pairs["pz_neg"]
    )
    df_pairs["mass_diff"] = np.abs(df_pairs["inv_mass"] - Z_mass)

    # Pick best candidate per event
    idx_best = df_pairs.groupby(["run", "event"])["mass_diff"].idxmin()
    df_best = df_pairs.loc[idx_best].reset_index(drop=True)

    # Columns to keep
    columns_to_keep = [
        "run", "event",
        "inv_mass", "mass_diff",
        "px_pos", "py_pos", "pz_pos",
        "px_neg", "py_neg", "pz_neg"
    ]
    df_best = df_best[columns_to_keep]

    print(f"Events with +- pairs: {len(df_best)}")

    # Plot invariant mass histogram
    plt.figure(figsize=(8,6))
    counts, bins, _ = plt.hist(df_best["inv_mass"], bins=50, range=(50, 130), alpha=0.6, label="Data")
    plt.xlabel("Invariant Mass [GeV]")
    plt.ylabel("Counts")
    plt.title("Z Candidate Invariant Mass Distribution")
    plt.legend()

    # Breit-Wigner + Exp fit
    bin_centers = 0.5*(bins[1:] + bins[:-1])
    p0 = [Z_mass, 2.5, counts.max(), 0.05, counts.min()]  # initial guess: m0, gamma, A, B, C
    try:
        popt, _ = curve_fit(bw_plus_exp, bin_centers, counts, p0=p0)
        x_fit = np.linspace(50, 130, 500)
        y_fit = bw_plus_exp(x_fit, *popt)
        plt.plot(x_fit, y_fit, "r-", label=f"BW Fit: m0={popt[0]:.2f}, gamma={popt[1]:.2f}, B={popt[3]:.3f}")
        plt.legend()
    except Exception as e:
        print(f"Fit failed: {e}")

    # Save figure
    output_plot = Path(f"{output_dir}/{output_name}.png")
    plt.savefig(output_plot, dpi=150)
    print(f"Plot saved to {output_plot}")

# Run main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Z candidate selection and BW+Exp fit")
    parser.add_argument("--input-dir", type=str, default=".", help="Directory with input Parquet files")
    parser.add_argument("--output-dir", type=str, default=".", help="Directory to save outputs")
    parser.add_argument("--output-name", type=str, default="z_candidates", help="Base name for output files")
    args = parser.parse_args()

    df_best = main(input_dir=args.input_dir, output_dir=args.output_dir, output_name=args.output_name)
