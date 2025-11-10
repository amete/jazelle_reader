# jazelle_reader

**Jazelle Reader – SLD MiniDST Stream Utilities**

A Python package to read SLD MiniDST files in Jazelle format. Based on the original Java implementation: [Jazelle by Tony Johnson](https://github.com/tony-johnson/Jazelle).

## Features

- Read **physical records** and **logical records** from Jazelle files.
- Extract file metadata such as name, creation, and modification timestamps.
- Simple iteration over records for analysis or conversion.

## Usage 

Clone the repository and run inspection:

```bash
git clone <repo-url>
cd jazelle_reader
./inspect.py [file]
```

Example output:

```
====================================================================================================
File     : [...redacted...]
Name     : TAPE0101
Created  : 2001-09-21 18:46:07.126000
Modified : 2003-10-02 16:25:41.789000
====================================================================================================
Record 1000: Run #37435, Event #6317
Record 2000: Run #37480, Event #291
Record 3000: Run #37499, Event #1428
Record 4000: Run #37533, Event #1164
Record 5000: Run #37542, Event #6006
Record 6000: Run #37769, Event #5333
Record 7000: Run #37939, Event #1246
Record 8000: Run #37957, Event #4583
Record 9000: Run #37983, Event #6664
EOF reached after 9995 logical records
```

**TBC**
