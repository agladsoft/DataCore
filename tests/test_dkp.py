import os
import json
import pytest
from scripts.dkp import DKP
from pathlib import PosixPath
from typing import Optional, Union
from _pytest.logging import LogCaptureFixture

dir_path: str = f"{os.environ['XL_IDP_PATH_DATACORE']}/dkp"
headers_columns: list = [
    "клиент",
    "описание",
    "стратегич. проект",
    "груз",
    "направление",
    "бассейн",
    "принадлежность ктк",
    "разм"
]
headers_blocks: list = [
    "НАТУРАЛЬНЫЕ ПОКАЗАТЕЛИ, ктк",
    "КОММЕНТАРИЙ К УСЛУГЕ",
    "СОИСПОЛНИТЕЛЬ",
    "ПРИЗНАК ВОЗМЕЩАЕМЫХ (76)",
    "НАТУРАЛЬНЫЕ ПОКАЗАТЕЛИ, TEUS"
]
reference_dkp: list = [
    ('Наименования столбцов', 'Столбцы основной таблицы', 'клиент', 'client'),
    ('Наименования столбцов', 'Столбцы основной таблицы', 'описание', 'description'),
    ('Наименования столбцов', 'Столбцы основной таблицы', 'стратегич. проект', 'project'),
    ('Наименования столбцов', 'Столбцы основной таблицы', 'груз', 'cargo'),
    ('Наименования столбцов', 'Столбцы основной таблицы', 'направление', 'direction'),
    ('Наименования столбцов', 'Столбцы основной таблицы', 'бассейн', 'bay'),
    ('Наименования столбцов', 'Столбцы основной таблицы', 'принадлежность ктк', 'owner'),
    ('Наименования столбцов', 'Столбцы основной таблицы', 'разм', 'container_size'),
    ('Наименования блоков', 'Блоки для основной таблицы', 'НАТУРАЛЬНЫЕ ПОКАЗАТЕЛИ, ктк', 'natural_indicators_ktk'),
    ('Наименования блоков', 'Блоки для основной таблицы', 'КОММЕНТАРИЙ К УСЛУГЕ', 'service'),
    ('Наименования блоков', 'Блоки для основной таблицы', 'СОИСПОЛНИТЕЛЬ', 'co_executor'),
    ('Наименования блоков', 'Блоки для основной таблицы', 'ПРИЗНАК ВОЗМЕЩАЕМЫХ (76)', 'reimbursable_sign_76'),
    ('Наименования блоков', 'Блоки для основной таблицы', 'НАТУРАЛЬНЫЕ ПОКАЗАТЕЛИ, TEUS', 'natural_indicators_teus'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'янв', 'natural_indicators_ktk_jan'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'фев', 'natural_indicators_ktk_feb'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'мар', 'natural_indicators_ktk_mar'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'апр', 'natural_indicators_ktk_apr'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'май', 'natural_indicators_ktk_may'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'июн', 'natural_indicators_ktk_jun'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'июл', 'natural_indicators_ktk_jul'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'авг', 'natural_indicators_ktk_aug'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'сен', 'natural_indicators_ktk_sep'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'окт', 'natural_indicators_ktk_oct'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'ноя', 'natural_indicators_ktk_nov'),
    ('natural_indicators_ktk', 'Столбцы таблиц в блоках', 'дек', 'natural_indicators_ktk_dec'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'янв', 'natural_indicators_teus_jan'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'фев', 'natural_indicators_teus_feb'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'мар', 'natural_indicators_teus_mar'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'апр', 'natural_indicators_teus_apr'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'май', 'natural_indicators_teus_may'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'июн', 'natural_indicators_teus_jun'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'июл', 'natural_indicators_teus_jul'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'авг', 'natural_indicators_teus_aug'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'сен', 'natural_indicators_teus_sep'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'окт', 'natural_indicators_teus_oct'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'ноя', 'natural_indicators_teus_nov'),
    ('natural_indicators_teus', 'Столбцы таблиц в блоках', 'дек', 'natural_indicators_teus_dec'),
    ('Наименования листов', 'ПЛАН ПРОДАЖ', 'ПЛАН_ПРОДАЖ', 'ПЛАН_ПРОДАЖ'),
    ('Наименования листов', 'ПЛАН ПРОДАЖ', 'ПЛАН-ПРОДАЖ', 'ПЛАН_ПРОДАЖ'),
    ('Наименования листов', 'ПЛАН ПРОДАЖ', 'ПЛАН ПРОДАЖ', 'ПЛАН_ПРОДАЖ'),
    ('Наименования в файле', 'ДКП', 'ДКП_ДВ', 'ДКП ДВ'),
    ('Наименования в файле', 'ДКП', 'Дкп_Дв', 'ДКП ДВ'),
    ('Наименования в файле', 'ДКП', 'дкп_дв', 'ДКП ДВ'),
    ('Наименования в файле', 'ДКП', 'ДКП ДВ', 'ДКП ДВ'),
    ('Наименования в файле', 'ДКП', 'ДКП_СЗФО', 'ДКП СЗФО'),
    ('Наименования в файле', 'ДКП', 'Дкп_Сзфо', 'ДКП СЗФО'),
    ('Наименования в файле', 'ДКП', 'дкп_сзфо', 'ДКП СЗФО'),
    ('Наименования в файле', 'ДКП', 'ДКП СЗФО', 'ДКП СЗФО'),
    ('Наименования в файле', 'ДКП', 'ДКП_ЮФО', 'ДКП ЮФО'),
    ('Наименования в файле', 'ДКП', 'Дкп_Юфо', 'ДКП ЮФО'),
    ('Наименования в файле', 'ДКП', 'дкп_юфо', 'ДКП ЮФО'),
    ('Наименования в файле', 'ДКП', 'ДКП ЮФО', 'ДКП ЮФО'),
    ('Наименования в файле', 'ДКП', 'ДКП_Сибирь', 'ДКП Сибирь'),
    ('Наименования в файле', 'ДКП', 'Дкп_Сибирь', 'ДКП Сибирь'),
    ('Наименования в файле', 'ДКП', 'дкп_сибирь', 'ДКП Сибирь'),
    ('Наименования в файле', 'ДКП', 'ДКП Сибирь', 'ДКП Сибирь'),
    ('Наименования в файле', 'ДКП', 'ДКП_Урал', 'ДКП Урал'),
    ('Наименования в файле', 'ДКП', 'Дкп_Урал', 'ДКП Урал'),
    ('Наименования в файле', 'ДКП', 'дкп_урал', 'ДКП Урал'),
    ('Наименования в файле', 'ДКП', 'ДКП Урал', 'ДКП Урал'),
    ('Наименования в файле', 'ДКП', 'ДКП_Центр Восток', 'ДКП Центр Восток'),
    ('Наименования в файле', 'ДКП', 'Дкп_Центр Восток', 'ДКП Центр Восток'),
    ('Наименования в файле', 'ДКП', 'дкп_центр восток', 'ДКП Центр Восток'),
    ('Наименования в файле', 'ДКП', 'ДКП Центр Восток', 'ДКП Центр Восток'),
    ('Наименования в файле', 'ДКП', 'ДКП_Центр', 'ДКП Центр'),
    ('Наименования в файле', 'ДКП', 'Дкп_Центр', 'ДКП Центр'),
    ('Наименования в файле', 'ДКП', 'дкп_центр', 'ДКП Центр'),
    ('Наименования в файле', 'ДКП', 'ДКП Центр', 'ДКП Центр')
]


# Фикстура для подмены метода get_reference
@pytest.fixture
def mock_get_reference(mocker):
    mocker.patch("scripts.dkp.DKP.get_reference", return_value=reference_dkp)


@pytest.fixture
def dkp_instance(mock_get_reference) -> DKP:
    """
    A fixture that provides a DKP instance.

    The instance is created with a filename pointing to the file
    "ДКП_ЮФО_ПП_2024_ОП_v3 от 27.09.2023.xlsx" in the "done" folder and a folder
    pointing to the "json" folder.

    :return: An instance of the DKP class.
    """
    return DKP(
        filename=f"{dir_path}/done/ДКП_ЮФО_ПП_2024_ОП_v3 от 27.09.2023.xlsx",
        folder=f"{dir_path}/json",
    )


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


def test_get_list_columns(dkp_instance: DKP) -> None:
    """
    Tests the `_get_list_columns` method.

    This test verifies that the method correctly returns a list of all possible column names.
    It checks that the length of the list is greater than or equal to 8.

    :param dkp_instance: A DKP object.
    :return: None
    """
    list_columns = dkp_instance._get_list_columns()
    assert list_columns == headers_columns


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


@pytest.mark.parametrize("row, block_position, headers, dict_columns_position, expected_result", [
    (headers_columns, [0, len(headers_columns)], "columns_names", {
        "client": None,
        "description": None,
        "project": None,
        "cargo": None,
        "direction": None,
        "bay": None,
        "owner": None,
        "container_size": None
    }, {
        "client": 0,
        "description": 1,
        "project": 2,
        "cargo": 3,
        "direction": 4,
        "bay": 5,
        "owner": 6,
        "container_size": 7
    }),
    (headers_blocks, [0, len(headers_blocks)], "block_names", {
        "natural_indicators_ktk": None,
        "service": None,
        "co_executor": None,
        "reimbursable_sign_76": None,
        "natural_indicators_teus": None
    }, {
        "natural_indicators_ktk": 0,
        "service": 1,
        "co_executor": 2,
        "reimbursable_sign_76": 3,
        "natural_indicators_teus": 4
    })
])
def test_get_columns_position(
    dkp_instance: DKP,
    row: list,
    block_position: list,
    headers: str,
    dict_columns_position: dict,
    expected_result: dict
) -> None:
    """
    Tests the `get_columns_position` method.

    This test verifies that the method correctly returns a list of all possible column names.
    It checks that the length of the list is greater than or equal to 8.

    :param dkp_instance: A DKP object.
    :return: None
    """
    headers = getattr(dkp_instance, headers)
    dkp_instance.get_columns_position(row, block_position, headers, dict_columns_position)
    assert dict_columns_position == expected_result


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
    data: list = [{
        "client": "Test Client",
        "description": "Test Description",
        "project": "Test Project"
    }]
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
    assert first_entry["project"] == "Test Project"


def validate_record_fields(record: dict, expected_values: dict) -> None:
    """
    Validates that the given record contains the expected values in its fields.
    :param record: The record to validate.
    :param expected_values: The expected values for each field in the record.
    :return:
    """
    for key, expected_value in expected_values.items():
        assert record[key] == expected_value, f"Mismatch for '{key}': expected {expected_value}, got {record[key]}"


@pytest.mark.parametrize(
    "row, columns_positions, month_string, expected_values", [
        (
            ["1", "РУСКОН ООО", "Новые клиенты", "Без проекта", "Китай", "импорт", "АЧБ", None, "20"],
            {
                "client": 2,
                "project": 3,
                "cargo": 4,
                "direction": 5,
                "bay": 6,
                "owner": 7,
                "container_size": 8,
            },
            "Январь",
            {
                "client": "Новые клиенты",
                "project": "Без проекта",
                "cargo": "Китай",
                "direction": "импорт",
                "bay": "АЧБ",
                "container_size": 20,
                "month_string": "Январь",
                "date": "2024-01-01",
            },
        ),
    ],
)
def test_get_content_in_table(
    dkp_instance: DKP,
    row: list,
    columns_positions: dict,
    month_string: str,
    expected_values: dict
) -> None:
    """
    Tests the `get_content_in_table` method.

    :param dkp_instance: An instance of the DKP class.
    :param row: A list representing a row of data.
    :param columns_positions: A dictionary mapping column names to their positions.
    :param month_string: The month as a string.
    :param expected_values: The expected values in the resulting record.
    :return: None
    """
    metadata: dict = dkp_instance.extract_metadata_from_filename()
    for column, position in columns_positions.items():
        dkp_instance.dict_columns_position[column] = position

    record: dict = dkp_instance.get_content_in_table(1, month_string, row, metadata)
    validate_record_fields(record, expected_values)
