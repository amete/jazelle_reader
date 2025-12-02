# Jazelle Reader – SLD MiniDST Stream Utilities

A Python package to read SLD MiniDST files in Jazelle format. Based on the original Java implementation: [Jazelle by Tony Johnson](https://github.com/tony-johnson/Jazelle).

## Features

- Conversion from Jazelle/MiniDST to Parquet.
- Z boson mass analysis over Parquet.

## Requirements

- Python 3.12+
- Dependencies: `pyarrow`, `pandas`, `numpy`, `matplotlib`

## Structure

```
.
├── README.md
├── convert_minidst.py  # Main conversion script
├── analysis            # Sample analysis
├── banks               # Bank readers
├── stream              # Stream handlers
└── utils               # Utility functions
```

## Usage

Clone the repository:

```bash
git clone https://github.com/amete/jazelle_reader.git
cd jazelle_reader
```

You can run the conversion as follows (see all options w/ `-h`):

```bash
python3 convert_minidst.py <input_file>
```

This will generate a Parquet file in the current folder, which you can analyze further.
A basic Z boson mass analysis is provided in the `analysis/z_mass.py` script:

![Z Candidates](analysis/z_candidates.png)
