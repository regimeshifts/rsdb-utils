# RSDB utils

Utils to process the Regime Shifts DataBase CSV and parquet files.

Functionalities:

  - Import the database from a CSV or parquet file in a pandas DataFrame
  - Save the database in a CSV or parquet file from a pandas DataFrame
  - Check the database (a DataFrame) with the json schema or the Regime Shifts DataBase

Code repository: [https://github.com/regimeshifts/rsdb-utils](https://github.com/regimeshifts/rsdb-utils)

Licence: GNU General Public License v3 (GPLv3)

## Installation

### From Python
In a terminal:
```bash
pip install rsdb-utils 
```

### From R
In the R terminal:
```bash
# use reticulate
library(reticulate)
# create a python virtual environment
virtualenv_create(envname = 'rsdb_venv')
# load the virtual environment
use_virtualenv('rsdb_venv')
# install the rsdb-utils library
reticulate::virtualenv_install('rsdb-utils')
```

## Usage

### Read a database file
Read a database file to import it in a pandas DataFrame:

#### Python
```python
from rsdb_utils import read_rsdb

df = read_rsdb("my_database.csv")
```

#### R
```bash
library(reticulate)
use_virtualenv('rsdb_venv')
read_rsdb <- import('rsdb_utils.read_rsdb')

read_rsdb("my_database.csv")
```

### Read and write a database file

Read a database file in parquet, import it in a pandas DataFrame, and save it in CSV

#### Python
```python
from rsdb_utils import read_rsdb, write_rsdb

df = read_rsdb("my_database.parquet")

write_rsdb(df, "my_database.csv")
```

#### R
```bash
library(reticulate)
use_virtualenv('rsdb_venv')
read_rsdb <- import('rsdb_utils.read_rsdb')
write_rsdb <- import('rsdb_utils.write_rsdb')

df <- read_rsdb("my_database.csv")

write_rsdb(df, "my_database.csv")
```

### Check a database

Check a database based on the JSON schema of the Regime Shifts Database:

#### Python
```python
from rsdb_utils import read_rsdb, write_rsdb, check_rsdb

df = read_rsdb("my_database.parquet")

df = check_rsdb(df)

# save the database with the errors info
write_rsdb(df, "my_database.csv")
```

#### R
```bash
library(reticulate)
use_virtualenv('rsdb_venv')
read_rsdb <- import('rsdb_utils.read_rsdb')
write_rsdb <- import('rsdb_utils.write_rsdb')
check_rsdb <- import('rsdb_utils.check_rsdb')

df <- read_rsdb("my_database.csv")

df <- check_rsdb("my_database.csv")

write_rsdb(df, "my_database.csv")
```


## Development environment setup

### Python local installation

It's advised to create a virtual environment to install the required packages during development.
You can then "install" this package (rsdb-utils) as a local package to make it easier to import
within this local virtual environment.

```bash
# create a virtual environment
python3 -m venv .venv
# load the virtual environment
source .venv/bin/activate
# install the requirements
pip install -r requirements.txt
# install rsdb-utils as a local package
pip install -e .
```

### R local installation

You can install the package from source with reticulate in R.
Use the instructions above in `Installation from R` and swap the line
`reticulate::virtualenv_install("rsdb-utils")` by:
```R
reticulate::virtualenv_install('/home/romain/Documents/SRC/RSDB/rsdb-utils')
```
Note that the editable installation ('pip install -e') doesn't work with reticulate, which
means that you will need to reinstall the package each time you will modify the Python source code.

### Tests

The tests are run with `pytest`, install it (within the virtual environment) with:

```bash
pip install pytest 
```

And run the tests:

```bash
pytest tests/tests.py
```

### Build

In the development virtual environment, install:
```bash
pip install build twine
```

Then build the package:
```bash
python3 -m build
```

Configure your pypi access token and publish the package version:
```bash
twine upload dist/rsdb_utils-0.1.tar.gz dist/rsdb_utils-0.1-py3-none-any.whl
```
Once published, a version cannot be re-uploaded.

Romain THOMAS 2024  
Stockholm Resilience Centre
