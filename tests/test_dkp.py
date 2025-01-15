import os
import json
import pytest
from scripts.dkp import DKP
from pathlib import PosixPath
from typing import Optional, Union
from _pytest.logging import LogCaptureFixture

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
    # Тестирование базовых чисел
    assert dkp_instance._is_digit("123.45") is True
    assert dkp_instance._is_digit("-123.45") is True
    assert dkp_instance._is_digit("+123.45") is True

    # Тестирование научной нотации
    assert dkp_instance._is_digit("1e-10") is True
    assert dkp_instance._is_digit("1.23e+4") is True
    assert dkp_instance._is_digit("1E6") is True

    # Тестирование чисел с запятыми и пробелами
    assert dkp_instance._is_digit("1,234.56") is True
    assert dkp_instance._is_digit("12 34") is True
    assert dkp_instance._is_digit(" 123.45 ") is True

    # Тестирование некорректных входных данных
    assert dkp_instance._is_digit("abc") is False
    assert dkp_instance._is_digit("abc123") is False
    assert dkp_instance._is_digit("12.34.56") is False
    assert dkp_instance._is_digit("12..34") is False
    assert dkp_instance._is_digit("") is False


@pytest.mark.parametrize("dict1, dict2, expected", [
    ({"key1": "value1"}, {"key2": "value2"}, {"key1": "value1", "key2": "value2"}),
    ({"a": 1, "b": 2}, {"c": 3}, {"a": 1, "b": 2, "c": 3}),
    ({"name": "Alice"}, {"age": 30}, {"name": "Alice", "age": 30})
])
def test_merge_two_dicts(dkp_instance: DKP, dict1: dict, dict2: dict, expected: dict) -> None:
    """
    Tests the `_merge_two_dicts` method.

    This test verifies that the method correctly merges two dictionaries into a new dictionary.
    It checks that the method returns a dictionary with the expected keys and values.

    :param dkp_instance: A DKP object.
    :return: None
    """
    merged: dict = dkp_instance._merge_two_dicts(dict1, dict2)
    assert merged == expected


@pytest.mark.parametrize("input_str, expected_result", [
    ("  test   string\n", "test string"),
    ("\n  hello world \n", "hello world"),
    (" \t some text \t ", "some text"),
    ("no extra spaces or newlines", "no extra spaces or newlines"),
    ("   \n  multiple  \n  spaces  ", "multiple spaces"),
    ("\n\n  \t only spaces and newlines  \n\n", "only spaces and newlines")
])
def test_remove_symbols_in_columns(dkp_instance: DKP, input_str: str, expected_result: str) -> None:
    """
    Tests the `_remove_symbols_in_columns` method.

    This test verifies that the method correctly removes extra spaces and newline characters
    from a string. It checks that the method returns a string with the expected value.

    :param dkp_instance: A DKP object.
    :param input_str: The input string to process.
    :param expected_result: The expected result after removing extra spaces and newlines.
    :return: None
    """
    result: str = dkp_instance._remove_symbols_in_columns(input_str)
    assert result == expected_result


@pytest.mark.parametrize("input_value, expected_output", [
    ("123.45", 123.45),
    ("12 3.45", 123.45),
    ("12 34", 1234),
    ("1234", 1234),
    ("abc", "abc"),
    ("abc123", "abc123"),
    ("Да", True),
    ("да", True),
    ("yes", True),
    ("Нет", False),
    ("нет", False),
    ("no", False),
    (None, None)
])
def test_convert_value(
    dkp_instance: DKP,
    input_value: Optional[str],
    expected_output: Union[float, str, bool, None]
) -> None:
    """
    Tests the `_convert_value` method.

    This test verifies that the method correctly converts values to their expected types:
    strings, integers, booleans, or returns `None`.

    :param dkp_instance: A DKP object.
    :return: None
    """
    result = dkp_instance._convert_value(input_value)
    assert result == expected_output


@pytest.mark.parametrize("basename_filename, expected", [
    ("ДКП_ДВ_ПП_2025_v6_11.10.xlsx", {"department": "ДКП ДВ", "year": 2025}),
    ("ДКП_Сибирь_ПП_2025_v4.xlsx", {"department": "ДКП Сибирь", "year": 2025}),
    ("ДКП_Урал_ПП_2025 v6.xlsx", {"department": "ДКП Урал", "year": 2025}),
    ("ДКП_Центр Восток_ПП_2025_v.3.xlsx", {"department": "ДКП Центр Восток", "year": 2025}),
    ("ДКП ЮФО_ПП_2024_импорт_v7 от 27.09.2023.xlsx", {"department": "ДКП ЮФО", "year": 2024}),
    ("ДКП_ЮФО_ПП_2024_ОП_v3 от 27.09.2023.xlsx", {"department": "ДКП ЮФО", "year": 2024}),
    ("ДКП_ЮФО_ПП_2024_экспорт_FC от 13.10.2023.xlsx", {"department": "ДКП ЮФО", "year": 2024}),
    ("СПБ_ДКП СЗФО_ПП_2024.v.10.xlsx", {"department": "ДКП СЗФО", "year": 2024})
])
def test_extract_metadata_from_filename(dkp_instance: DKP, basename_filename: str, expected: dict) -> None:
    """
    Tests the `extract_metadata_from_filename` method.

    This test verifies that the method correctly extracts metadata from a filename.
    It checks that the method returns a dictionary with the expected department and year.

    :param dkp_instance: A DKP object.
    :return: None
    """
    dkp_instance.basename_filename = basename_filename
    metadata: dict = dkp_instance.extract_metadata_from_filename()
    assert metadata == expected


@pytest.mark.parametrize("row, expected_min_count", [
    ([
        'организация', 'клиент', 'стратегич. проект', 'груз',
        'направление', 'бассейн', 'принадлежность ктк', 'разм'
    ], 7),
    ([
        'организация', 'клиент', 'описание', 'стратегич. проект', 'груз',
        'направление', 'бассейн', 'принадлежность ктк', 'разм'
    ], 8),
])
def test_get_probability_of_header(dkp_instance: DKP, row: list, expected_min_count: int) -> None:
    """
    Tests the `_get_probability_of_header` method.

    This test verifies that the method correctly calculates the probability of a given row
    being a header. The test checks that the method returns a value greater than 87
    when given a row with the expected column names.

    :param dkp_instance: A DKP object.
    :param row: The row to check for header probability.
    :param expected_min_count: The expected minimum count of matching columns for a header.
    :return: None
    """
    list_columns: list = dkp_instance._get_list_columns()
    count_match_header: float = dkp_instance._get_count_match_of_header(row, list_columns)
    assert count_match_header >= expected_min_count


@pytest.mark.parametrize("columns, error_message, expected_log", [
    ({"client": None, "description": 1}, "Test error", "Test error. Empty columns - ['client']"),
    (
        {"client": None, "description": None},
        "Empty columns error",
        "Empty columns error. Empty columns - ['client', 'description']"
    )
])
def test_check_errors_in_columns(
    dkp_instance: DKP,
    caplog: LogCaptureFixture,
    columns: dict,
    error_message: str,
    expected_log: str
) -> None:
    """
    Tests the `check_errors_in_columns` method for handling empty columns.

    This test verifies that the method logs an error message and raises a
    SystemExit exception when there are empty columns in the input dictionary.
    It checks that the appropriate error message is logged and the exit code
    is set to 2.

    :param dkp_instance: An instance of the DKP class.
    :param caplog: A pytest fixture to capture log records.
    :param columns: The dictionary of columns to test.
    :param error_message: The error message to test.
    :param expected_log: The expected log output.
    :return: None
    """
    with pytest.raises(SystemExit) as excinfo:
        dkp_instance.check_errors_in_columns(columns, error_message)

    # Проверка на то, что сообщение об ошибке содержится в логе
    assert expected_log in caplog.text

    # Проверка на то, что код выхода правильный
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
        parsed_content = json.loads(f.read())

    # Убедитесь, что это список с одним элементом
    assert isinstance(parsed_content, list)
    assert len(parsed_content) == 1

    # Убедитесь, что структура и содержимое данных соответствуют входным данным
    assert parsed_content == data

    # При необходимости проверьте конкретные поля
    first_entry = parsed_content[0]
    assert first_entry["client"] == "Test Client"
    assert first_entry["description"] == "Test Description"


def setup_column_positions(dkp_instance: DKP, columns_positions: dict) -> None:
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
    record: dict = dkp_instance.get_content_in_table(1, "Январь", row, metadata)
    validate_record_fields(record, expected_values)
