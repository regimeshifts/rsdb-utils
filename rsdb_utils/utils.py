# Romain THOMAS 2024
# Stockholm Resilience Centre
# GNU General Public License v3 (GPLv3)

import json
import copy
import warnings
import pkgutil

import pandas as pd
import numpy as np

from tqdm import tqdm

import jsonref  # dereferencing of JSON Reference
from jsonschema import Draft202012Validator

# Load the JSON schema from the package data with all the references (defs)
schema_path = "rsdb-schema/case_study_schema.json"
rs_cs_schema = jsonref.loads(pkgutil.get_data(__name__, schema_path).decode('utf-8'))

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
        # ensure_ascii=False ensure to preserve UTF-8 encoding
        return json.dumps(val, ensure_ascii=False)
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
    rsdb_df = pd.read_csv(file_path, index_col=False)
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
    rsdb_df.to_csv(file_path, index=False)


def write_parquet_rsdb(rsdb_df: pd.DataFrame, file_path: str):
    """
    Save a DataFrame database in a parquet file.

    :param rsdb_df: The database in a pandas DataFrame.
    :type rsdb_df: pd.DataFrame
    :param file_path: the path of the parquet file.
    :type file_path: str
    """
    rsdb_df.to_parquet(file_path, index=False)


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
    print(f"{nb_errors} errors when validating cases against the schema")  # TODO: Use logging lib?

    rsdb_df = pd.DataFrame.from_records(rsdb_records)

    return rsdb_df


def generate_enums_dataframe():
    def get_schema_without_allof():
        schema = copy.deepcopy(rs_cs_schema['properties'])

        def replace_allof(item_to_replace):
            """
            Remove the allOf attributes in an item. Works recursively.
            """
            # replace allof
            if 'allOf' in item_to_replace.keys():
                for allof_key in item_to_replace['allOf'][0].keys():
                    item_to_replace[allof_key] = item_to_replace['allOf'][0][allof_key]
                del item_to_replace['allOf']
            # recursive call
            for value in item_to_replace.values():
                if isinstance(value, dict):
                    value = replace_allof(value)
            return item_to_replace

        # we could directly use replace_allof on the whole schema but this
        # for loop allows us to nicely display debug
        for root_key in schema.keys():
            root_item = schema[root_key]
            # print(item)
            root_item = replace_allof(root_item)
            # print(item)
            # print("")
        return schema

    def check_type_of_json_item(type_object, type_to_check):
        if not isinstance(type_object, list):
            type_object = [type_object]
        if type_to_check in type_object:
            return True
        else:
            return False

    def create_enum_option_dict(val):
        """
        Create the dict with the field names
        """
        option = '{'
        for prop_key in val.keys():
            if check_type_of_json_item(val[prop_key]['type'], 'number'):
                option += '"' + str(prop_key) + '": null, '
            else:
                option += '"' + str(prop_key) + '": "", '
        option = option[:-2]
        option += '}'
        return option

    schema_without_allof = get_schema_without_allof()
    enum_options = {}
    for key in schema_without_allof.keys():
        item = schema_without_allof[key]
        # print(item)
        if check_type_of_json_item(item['type'], "string") \
                or check_type_of_json_item(item['type'],"integer") \
                or check_type_of_json_item(item['type'], "number"):
            if 'enum' in item:
                enum_options[key] = item['enum']
            else:
                enum_options[key] = None
        elif check_type_of_json_item(item['type'], "array"):
            # list of simple string
            if check_type_of_json_item(item['items']['type'], "string"):
                if 'enum' in item['items']:
                    enum_options[key] = item['items']['enum']
                    enum_options[key] = ['[null]' if option is None else '["' + option + '"]' for option in
                                         enum_options[key]]
                else:
                    enum_options[key] = None
            # list of objects
            else:
                # count the number of enums in the properties
                nb_enums = 0
                for prop_val in item['items']['properties'].values():
                    if 'enum' in prop_val:
                        nb_enums += 1
                if nb_enums == 0:
                    enum_options[key] = '[' + create_enum_option_dict(item['items']['properties']) + ']'
                elif nb_enums == 1:
                    try:
                        options = item['items']['properties']['value']['enum']
                    except Exception as e:
                        print(e)
                        raise ValueError(
                            "Only columns of regime shift type format are supported for the enums in list of objects")
                    enum_options[key] = []
                    for option in options:
                        if option == "Proposed & new type":
                            enum_options[key].append('[{"value": "Proposed & new type", "other": ""}]')
                        elif option is None:
                            enum_options[key].append('[{"value": null}]')
                        else:
                            enum_options[key].append('[{"value": "' + option + '"}]')
                else:
                    raise ValueError("Multiple enums in the same field are not supported")

    enum_options_df = pd.DataFrame({key: pd.Series(val) for key, val in enum_options.items()})

    return enum_options_df
