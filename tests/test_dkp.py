import os
import pytest
from scripts.dkp import DKP
from pathlib import PosixPath
from _pytest.logging import LogCaptureFixture

os.environ['XL_IDP_PATH_DATACORE'] = "/home/timur/sambashare/DataCore"
dir_path: str = f"{os.environ['XL_IDP_PATH_DATACORE']}/dkp"


@pytest.fixture
def dkp_instance() -> DKP:
    """
    A fixture that provides a DKP instance.

    The instance is created with a filename pointing to the file
    "ДКП_ЮФО_ПП_2024_ОП_v3 от 27.09.2023.xlsx" in the "done" folder and a folder
    pointing to the "json" folder.

    :return: An instance of the DKP class.
    """
    return DKP(filename=f"{dir_path}/done/ДКП_ЮФО_ПП_2024_ОП_v3 от 27.09.2023.xlsx", folder=f"{dir_path}/json")


def test_is_digit(dkp_instance: DKP) -> None:
    """
    Tests the `_is_digit` method.

    This test verifies that the method correctly determines whether a string can be
    converted to a float. It checks that the method returns True for strings that
    represent numbers and False for strings that do not.

    :param dkp_instance: An instance of the DKP class.
    :return: None
    """
    assert dkp_instance._is_digit("123.45") is True
    assert dkp_instance._is_digit("abc") is False
    assert dkp_instance._is_digit("abc123") is False
    assert dkp_instance._is_digit("12 34") is True


def test_merge_two_dicts(dkp_instance: DKP) -> None:
    """
    Tests the `_merge_two_dicts` method.

    This test verifies that the method correctly merges two dictionaries into a new dictionary.
    It checks that the method returns a dictionary with the expected keys and values.

    :param dkp_instance: A DKP object.
    :return: None
    """
    dict1: dict = {"key1": "value1"}
    dict2: dict = {"key2": "value2"}
    merged: dict = dkp_instance._merge_two_dicts(dict1, dict2)
    assert merged == {"key1": "value1", "key2": "value2"}


def test_remove_symbols_in_columns(dkp_instance: DKP) -> None:
    """
    Tests the `_remove_symbols_in_columns` method.

    This test verifies that the method correctly removes extra spaces and newline characters
    from a string. It checks that the method returns a string with the expected value.

    :param dkp_instance: A DKP object.
    :return: None
    """
    result: str = dkp_instance._remove_symbols_in_columns("  test   string\n")
    assert result == "test string"


def test_convert_value(dkp_instance: DKP) -> None:
    """
    Tests the `_convert_value` method.

    This test verifies that the method correctly removes extra spaces and newline characters
    from a string. It checks that the method returns a string with the expected value.

    :param dkp_instance: A DKP object.
    :return: None
    """
    assert dkp_instance._convert_value("123.45") == 123.45
    assert dkp_instance._convert_value("12 3.45") == 123.45
    assert dkp_instance._convert_value("12 34") == 1234
    assert dkp_instance._convert_value("1234") == 1234
    assert dkp_instance._convert_value("abc") == "abc"
    assert dkp_instance._convert_value("abc123") == "abc123"
    assert dkp_instance._convert_value("Да") is True
    assert dkp_instance._convert_value("да") is True
    assert dkp_instance._convert_value("yes") is True
    assert dkp_instance._convert_value("Нет") is False
    assert dkp_instance._convert_value("нет") is False
    assert dkp_instance._convert_value("no") is False
    assert dkp_instance._convert_value(None) is None


def test_extract_metadata_from_filename(dkp_instance: DKP) -> None:
    """
    Tests the `extract_metadata_from_filename` method.

    This test verifies that the method correctly extracts metadata from a filename.
    It checks that the method returns a dictionary with the expected department and year.

    :param dkp_instance: A DKP object.
    :return: None
    """
    metadata: dict = dkp_instance.extract_metadata_from_filename()
    assert metadata["department"] == "ДКП ЮФО"
    assert metadata["year"] == 2024


def test_get_probability_of_header(dkp_instance: DKP) -> None:
    """
    Tests the `_get_probability_of_header` method.

    This test verifies that the method correctly calculates the probability of a given row
    being a header. The test checks that the method returns a value greater than 87
    when given a row with the expected column names.

    :param dkp_instance: A DKP object.
    :return: None
    """
    row: list = [
        'организация', 'клиент', 'стратегич. проект', 'груз',
        'направление', 'бассейн', 'принадлежность ктк', 'разм'
    ]
    list_columns: list = dkp_instance._get_list_columns()
    probability: float = dkp_instance._get_probability_of_header(row, list_columns)
    assert probability >= 87


def test_check_errors_in_columns(dkp_instance: DKP, caplog: LogCaptureFixture) -> None:
    """
    Tests the `check_errors_in_columns` method for handling empty columns.

    This test verifies that the method logs an error message and raises a
    SystemExit exception when there are empty columns in the input dictionary.
    It checks that the appropriate error message is logged and the exit code
    is set to 2.

    :param dkp_instance: An instance of the DKP class.
    :param caplog: A pytest fixture to capture log records.
    :return: None
    """
    columns: dict = {"client": None, "description": 1}
    with pytest.raises(SystemExit) as excinfo:
        dkp_instance.check_errors_in_columns(columns, "Test error")
    assert "Test error. Empty columns - ['client']" in caplog.text
    assert excinfo.value.code == 2


def test_write_to_json(dkp_instance: DKP, tmp_path: PosixPath) -> None:
    """
    Tests the `write_to_json` method.

    This test verifies that the method correctly writes the data to a JSON file
    in the specified folder and that the file contains the expected data.

    :param dkp_instance: An instance of the DKP class.
    :param tmp_path: A temporary path to write the JSON file.
    :return: None
    """
    data: list = [{"client": "Test Client", "description": "Test Description"}]
    dkp_instance.folder = tmp_path
    dkp_instance.write_to_json(data)
    output_file: PosixPath = tmp_path / os.path.basename(f"{dkp_instance.filename}.json")
    assert output_file.exists()
    with open(output_file, "r", encoding="utf-8") as f:
        content: str = f.read()
    assert '"client": "Test Client"' in content


def setup_column_positions(dkp_instance, columns_positions):
    """
    Sets up the column positions in the DKP instance.

    This function iterates over the provided dictionary of column positions
    and assigns each column's position to the DKP instance's
    `dict_columns_position` attribute.

    :param dkp_instance: An instance of the DKP class.
    :param columns_positions: A dictionary with column names as keys and their
                              respective positions as values.
    :return: None
    """
    for column, position in columns_positions.items():
        dkp_instance.dict_columns_position[column] = position


def validate_record_fields(record: dict, expected_values: dict) -> None:
    """
    Validates that the given record contains the expected values in its fields.
    :param record: The record to validate.
    :param expected_values: The expected values for each field in the record.
    :return:
    """
    for key, expected_value in expected_values.items():
        assert record[key] == expected_value, f"Mismatch for '{key}': expected {expected_value}, got {record[key]}"


def test_get_content_in_table(dkp_instance: DKP) -> None:
    """
    Tests the `get_content_in_table` method.

    This test verifies that the method correctly extracts the fields from the table
    and converts the date to the correct format.

    :param dkp_instance: An instance of the DKP class.
    :return: None
    """
    metadata: dict = dkp_instance.extract_metadata_from_filename()
    row: list = ["1", "РУСКОН ООО", "Новые клиенты", "Без проекта", "Китай", "импорт", "АЧБ", None, "20"]
    columns_positions: dict = {
        "client": 2,
        "project": 3,
        "cargo": 4,
        "direction": 5,
        "bay": 6,
        "owner": 7,
        "container_size": 8,
    }
    expected_values: dict = {
        "client": "Новые клиенты",
        "project": "Без проекта",
        "cargo": "Китай",
        "direction": "импорт",
        "bay": "АЧБ",
        "container_size": 20,
        "month_string": "Январь",
        "date": "2024-01-01",
    }
    setup_column_positions(dkp_instance, columns_positions)
    record: dict = dkp_instance.get_content_in_table(0, 1, "Январь", row, metadata)
    validate_record_fields(record, expected_values)
