# Romain THOMAS 2024
# Stockholm Resilience Centre
# GNU General Public License v3 (GPLv3)

import json
import warnings

import pandas as pd
import numpy as np

from tqdm import tqdm

import jsonref  # dereferencing of JSON Reference
from jsonschema import Draft202012Validator

# replace with API call, package data or GitHub http download and cache
schema_path = "../rsdb-schema/case_study_schema.json"

# Load the JSON schema
with open(schema_path) as f:
    # rs_cs_schema = json.load(f)
    rs_cs_schema = f.read()
    rs_cs_schema = jsonref.loads(rs_cs_schema)
first_level_fields_in_schema = list(rs_cs_schema['properties'].keys())
# check if the schema is valid
Draft202012Validator.check_schema(rs_cs_schema)
# create the JSON schema validator
rs_cs_validator = Draft202012Validator(rs_cs_schema)


def jsonize_cell(val):
    """
    Convert each cell from a pythonic type (list or dictionary) into a JSON string.

    :param val: the value of the cell.
    :type val: None | str | int | float | list | dict
    :return: the value in a JSON compatible format.
    :rtype: None | str | int | float
    """
    if isinstance(val, np.ndarray):
        val = list(val)
    if isinstance(val, (list, dict)):
        return json.dumps(val)
    else:
        return val


def pythonize_cell_from_csv(val):
    """
    Convert each cell from a JSON string into a pythonic type (list or dictionary). Works to read CSV files.

    :param val: the value of the cell.
    :type val: None | str | int | float
    :return: the value in a pythonic type.
    :rtype: None | str | int | float | list | dict
    """
    try:
        if pd.isna(val):
            return None
        elif isinstance(val, str) and (
                (val[0] == "[" and val[-1] == "]") or
                (val[0] == "{" and val[-1] == "}")):
            return json.loads(val)
        else:
            return val
    except json.JSONDecodeError:
        warnings.warn("Cell inferred as JSON type but error while loading JSON content, please check for syntax "
                      f"errors.\nCell value: {val}",
                      RuntimeWarning,
                      stacklevel=11)
        return val


def pythonize_cell_from_parquet(val):
    """
    Convert each cell from a parquet object dtype into a pythonic type (list or dictionary). Works to read parquet
    files.

    :param val: the value of the cell.
    :type val: None | str | int | float | list | np.ndarray | dict
    :return: the value in a pythonic format.
    :rtype: None | str | int | float | list | dict
    """
    if isinstance(val, np.ndarray):
        return val.tolist()
    elif pd.isna(val):
        return None
    else:
        return val


def read_csv_rsdb(file_path: str) -> pd.DataFrame:
    """
    Read a CSV database file into a pandas dataframe and infer the cell types based on the JSON syntax.

    :param file_path: the path of the CSV file.
    :type file_path: str
    :return: The database in a pandas DataFrame.
    :rtype: pd.DataFrame
    """
    rsdb_df = pd.read_csv(file_path)
    rsdb_df = rsdb_df.map(pythonize_cell_from_csv)
    return rsdb_df


def read_parquet_rsdb(file_path: str) -> pd.DataFrame:
    """
    Read a parquet database file into a pandas dataframe and infer the cell types based on the parquet object dtypes.

    :param file_path: the path of the parquet file.
    :type file_path: str
    :return: The database in a pandas DataFrame.
    :rtype: pd.DataFrame
    """
    rsdb_df = pd.read_parquet(file_path)
    rsdb_df = rsdb_df.map(pythonize_cell_from_parquet)
    return rsdb_df


def write_csv_rsdb(rsdb_df: pd.DataFrame, file_path: str):
    """
    From a DataFrame, convert the pythonic types (lists and dictionaries) to JSON compatible strings and save in a CSV
    file.

    :param rsdb_df: The database in a pandas DataFrame.
    :type rsdb_df: pd.DataFrame
    :param file_path: the path of the CSV file.
    :type file_path: str
    """
    rsdb_df = rsdb_df.map(jsonize_cell)
    rsdb_df.to_csv(file_path)


def write_parquet_rsdb(rsdb_df: pd.DataFrame, file_path: str):
    """
    Save a DataFrame database in a parquet file.

    :param rsdb_df: The database in a pandas DataFrame.
    :type rsdb_df: pd.DataFrame
    :param file_path: the path of the parquet file.
    :type file_path: str
    """
    rsdb_df.to_parquet(file_path)


def read_rsdb(file_path: str) -> pd.DataFrame:
    """
    Read a database file into a pandas dataframe and infer the cell types. Automatically detect the database format (CSV
    or parquet).

    :param file_path: the path of the CSV file.
    :type file_path: str
    :return: The database in a pandas DataFrame.
    :rtype: pd.DataFrame
    """
    if file_path.lower().endswith(".parquet"):
        rsdb_df = read_parquet_rsdb(file_path)
    elif file_path.lower().endswith(".csv"):
        rsdb_df = read_csv_rsdb(file_path)
    else:
        raise ValueError("Filename must end with .parquet or .csv")
    return rsdb_df


def write_rsdb(rsdb_df: pd.DataFrame, file_path: str):
    """
    Save a DataFrame database in a parquet file. Automatically detect the database format (CSV or parquet).

    :param rsdb_df: The database in a pandas DataFrame.
    :type rsdb_df: pd.DataFrame
    :param file_path: the path of the file.
    :type file_path: str
    """
    if file_path.lower().endswith(".parquet"):
        write_parquet_rsdb(rsdb_df, file_path)
    elif file_path.lower().endswith(".csv"):
        write_csv_rsdb(rsdb_df, file_path)
    else:
        raise ValueError("Filename must end with .parquet or .csv")


def check_rsdb(rsdb_df: pd.DataFrame) -> pd.DataFrame:
    """
    Check that the database DataFrame (rsdb_df) complies to the RSDB JSON schema.
    Columns not specified in schema are ignored.
    The column "schema_errors" contains the errors of each row.
    The column "nb_schema_errors" contains the number of errors in each row.

    :param rsdb_df: DataBase of Regime Shifts
    :type rsdb_df: pd.DataFrame
    :return: DataBase of Regime Shifts with the "schema_errors" and "nb_schema_errors" columns
    :rtype: pd.DataFrame
    """
    rsdb_df['schema_errors'] = None
    rsdb_df['nb_schema_errors'] = None
    rsdb_records = rsdb_df.to_dict('records')
    nb_errors = 0
    for i, case in tqdm(enumerate(rsdb_records), colour="#00ffff"):
        if not rs_cs_validator.is_valid(case):
            nb_errors_in_cs = 0
            error_string = ""
            for error in rs_cs_validator.iter_errors(case):
                nb_errors_in_cs += 1
                # error_string = f"### {i} ####################\n{error}"
                if error_string != "":
                    error_string += "\n"
                error_string = f"####################\n{error}"
                warnings.warn(
                    f"\n### Row {i} ########################################\n"
                    f"DataBase doesn't complies to the JSON schema:\n{error}",
                    RuntimeWarning,
                    stacklevel=2)
            if nb_errors_in_cs > 0:
                case['schema_errors'] = error_string
                case['nb_schema_errors'] = nb_errors_in_cs
                nb_errors += nb_errors_in_cs
    print(f"{nb_errors} errors when validating cases against the schema") # TODO: Use logging lib?

    rsdb_df = pd.DataFrame.from_records(rsdb_records)

    return rsdb_df
