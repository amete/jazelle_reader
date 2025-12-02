# **Jazelle Reader – SLD MiniDST Stream Utilities**

A Python package to read SLD MiniDST files in Jazelle format. Based on the original Java implementation: [Jazelle by Tony Johnson](https://github.com/tony-johnson/Jazelle).

## Features

- Read **physical records** and **logical records** from Jazelle files.
- Extract file metadata such as name, creation, and modification timestamps.
- Convert the selected data into a Parquet file for further analysis. 

## Dependencies

You need to have `pyarrow` installed.

## Usage 

Clone the repository and run basic conversion:

```bash
git clone https://github.com/amete/jazelle_reader.git
cd jazelle_reader
./convert_minidst.py [file]
```

Example output:

```
Record 1000: Run 37435, Event 6317, Time 1997-07-15 04:05:17.805000
Record 2000: Run 37480, Event 291, Time 1997-07-18 19:15:42.931000
Record 3000: Run 37499, Event 1428, Time 1997-07-20 13:13:08.754000
Record 4000: Run 37533, Event 1164, Time 1997-07-21 17:43:42.860000
Record 5000: Run 37542, Event 6006, Time 1997-07-23 00:05:25.844000
Record 6000: Run 37769, Event 5333, Time 1997-07-31 07:03:32.774000
Record 7000: Run 37939, Event 1246, Time 1997-08-09 05:36:38.796000
Record 8000: Run 37957, Event 4583, Time 1997-08-10 07:26:02.069000
Record 9000: Run 37983, Event 6664, Time 1997-08-11 00:58:20.571000

EOF after 9995 logical records

Writing Parquet to outputs/qf1065.qf1065_5nrec97v18_mdst_1_7b1_nested.parquet (compression=snappy) ...
Saved: outputs/qf1065.qf1065_5nrec97v18_mdst_1_7b1_nested.parquet (12.43 MB)
```
