# Romain THOMAS 2024
# Stockholm Resilience Centre
# GNU General Public License v3 (GPLv3)

import os

import pytest

from rsdb_utils import read_rsdb, write_rsdb, check_rsdb

rsdb_test_dataset_csv_file = "rsdb_test_dataset.csv"
rsdb_test_dataset_csv_path = os.path.join(os.path.dirname(__file__), rsdb_test_dataset_csv_file)
rsdb_test_dataset_to_write_csv_path = os.path.join(os.path.dirname(__file__), "tmp_python_test_file-"+rsdb_test_dataset_csv_file)

rsdb_test_dataset_parquet_file = "rsdb_test_dataset.parquet"
rsdb_test_dataset_parquet_path = os.path.join(os.path.dirname(__file__), rsdb_test_dataset_parquet_file)
rsdb_test_dataset_to_write_parquet_path = os.path.join(os.path.dirname(__file__), "tmp_python_test_file-"+rsdb_test_dataset_parquet_file)

rsdb_test_dataset_with_errors_csv_file = "rsdb_test_dataset_with_errors.csv"
rsdb_test_dataset_with_errors_csv_path = os.path.join(os.path.dirname(__file__), rsdb_test_dataset_with_errors_csv_file)

rsdb_test_dataset_with_errors_checked_csv_file = "rsdb_test_dataset_with_errors_checked.csv"
rsdb_test_dataset_with_errors_checked_csv_path = os.path.join(os.path.dirname(__file__), rsdb_test_dataset_with_errors_checked_csv_file)


@pytest.mark.parametrize("file_path, col, row, value, value_type", [
    (rsdb_test_dataset_csv_path, "case_study_name", 0, "Balinese rice production", str),
    (rsdb_test_dataset_parquet_path, "case_study_name", 0, "Balinese rice production", str),
    (rsdb_test_dataset_csv_path, "main_contributors", 1, [{"name": "Johanna Yletyinen", "orcid": None}], list),
    (rsdb_test_dataset_parquet_path, "main_contributors", 1, [{"name": "Johanna Yletyinen", "orcid": None}], list),
    (rsdb_test_dataset_csv_path, "location_region", 2, "The Baltic Sea", str),
    (rsdb_test_dataset_parquet_path, "location_region", 2, "The Baltic Sea", str),
])
def test_read_rsdb(file_path, col, row, value, value_type):
    df = read_rsdb(file_path)
    assert df.at[row, col] == value
    assert isinstance(df.at[row, col], value_type)


def test_write_rsdb():
    df = read_rsdb(rsdb_test_dataset_parquet_path)

    write_rsdb(df, rsdb_test_dataset_to_write_parquet_path)
    os.remove(rsdb_test_dataset_to_write_parquet_path)

    write_rsdb(df, rsdb_test_dataset_to_write_csv_path)
    os.remove(rsdb_test_dataset_to_write_csv_path)


def test_check_rsdb_parquet_no_error():
    df = read_rsdb(rsdb_test_dataset_parquet_path)

    check_rsdb(df)


def test_check_rsdb_csv_with_errors():
    df = read_rsdb(rsdb_test_dataset_with_errors_csv_path)
    df_checked = read_rsdb(rsdb_test_dataset_with_errors_checked_csv_path)

    with pytest.warns(RuntimeWarning, match=r"DataBase doesn't complies to the JSON schema:\n*.") as record:
        df = check_rsdb(df)

    assert len(record) == 2
    assert df['nb_schema_errors'].sum() == 2
    assert df.at[2, 'schema_errors'] == df_checked.at[2, 'schema_errors']
    assert df.at[3, 'schema_errors'] == df_checked.at[3, 'schema_errors']
    assert df.at[3, 'nb_schema_errors'] == df_checked.at[3, 'nb_schema_errors']
    assert df.at[4, 'schema_errors'] == df_checked.at[4, 'schema_errors']

    # to re-generate test file to validate:
    # write_rsdb(df, rsdb_test_dataset_with_errors_checked_csv_path)
