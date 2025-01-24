import re
import sys
import json
import numpy as np
import pandas as pd
from re import Match
from pandas import DataFrame
from datetime import datetime
from scripts.settings_dkp import *
from scripts.app_logger import get_logger
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.query import Sequence
from typing import List, Dict, Optional, Union, Hashable

logger: get_logger = get_logger(os.path.basename(__file__).replace(".py", ""))


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


class DKP(object):
    def __init__(self, filename: str, folder: str):
        self.filename: str = filename
        self.basename_filename: str = os.path.basename(filename)
        self.folder: str = folder
        reference_dkp: Sequence = self._get_reference()
        self.columns_names: dict = self._group_columns(
            reference=reference_dkp,
            group_index=3,
            column_index=2,
            filter_key=0,
            filter_value="Наименования столбцов"
        )

        self.block_names: dict = self._group_columns(
            reference=reference_dkp,
            group_index=3,
            column_index=2,
            filter_key=0,
            filter_value="Наименования блоков"
        )

        self.block_table_columns: dict = self._group_nested_columns(
            reference=reference_dkp,
            block_index=0,
            group_index=3,
            column_index=2,
            filter_key=1,
            filter_value="Столбцы таблиц в блоках",
        )
        self.sheets_name: list = [column[2] for column in reference_dkp if column[0] == "Наименования листов"]
        self.dkp_names: dict = {column[2]: column[3] for column in reference_dkp if column[0] == "Наименования в файле"}
        self.floating_columns: list = [
            "description",
            *[
                sub_key
                for field, sub_keys in self.block_table_columns.items()
                if field not in NOT_COUNT_BLOCK
                for sub_key in sub_keys.keys()
                if sub_key.endswith(FILTER_SUFFIXES)
            ]
        ]
        self.dict_columns_position: Dict[str, Optional[int]] = {
            **{key: None for key in self.columns_names},
            **{
                sub_key: None
                for field, sub_keys in self.block_table_columns.items()
                if field not in NOT_COUNT_BLOCK
                for sub_key in sub_keys.keys()
            }
        }
        self.dict_block_position: Dict[str, Optional[int]] = {
            "natural_indicators_ktk": None,
            "co_executor_rate_per_unite": None,
            "unit_margin_income": None,
            "service": None,
            "co_executor": None,
            "reimbursable_sign_76": None,
            "natural_indicators_teus": None,
            "profit_plan": None,
            "costs_plan": None
        }

    @staticmethod
    def _group_columns(
        reference: Sequence,
        group_index: int,
        column_index: int,
        filter_key: int = None,
        filter_value: str = None
    ):
        """
        Groups columns of a given `reference` by a given `group_index` and then by a given `column_index`.

        If `filter_key` and `filter_value` are provided, it filters the rows of `reference` that have the value
        `filter_value` in the column with index `filter_key`.

        :param reference: The sequence of rows to group.
        :param group_index: The index of the column to group the rows by.
        :param column_index: The index of the column to group by after grouping by `group_index`.
        :param filter_key: The index of the column to filter the rows by.
        :param filter_value: The value of the column with index `filter_key` to filter the rows by.
        :return: A dictionary of dictionaries, where the keys of the outer dictionary are the values of the column
                 with index `group_index`, and the keys of the inner dictionaries are the values of the column with
                 index `column_index`, and the values of the inner dictionaries are tuples of the values of the column
                 with index `column_index`.
        """
        result: dict = {}
        for row in reference:
            if filter_key is None or row[filter_key] == filter_value:
                if row[group_index] in result:
                    result[row[group_index]] = result[row[group_index]] + (row[column_index],)
                else:
                    result[row[group_index]] = (row[column_index],)
        return result

    @staticmethod
    def _group_nested_columns(
        reference: Sequence,
        block_index: int,
        group_index: int,
        column_index: int,
        filter_key: int,
        filter_value: str
    ):
        """
        Groups nested columns of a given `reference` by specified indices.

        This function processes a sequence of rows and groups them hierarchically based on the values at
        specified indices. It first groups the rows by `block_index` and within each block, it groups by `group_index`.
        It filters the rows using a specified `filter_key` and `filter_value`, only including rows
        that match the filter criteria.

        :param reference: The sequence of rows to group.
        :param block_index: The index of the column to group the blocks by.
        :param group_index: The index of the column to group within each block.
        :param column_index: The index of the column whose values are to be collected in the groups.
        :param filter_key: The index of the column to filter the rows by.
        :param filter_value: The value that the column at `filter_key` must equal for a row to be included.
        :return: A nested dictionary where the outer keys correspond to unique values of the column at `block_index`,
                 the inner keys correspond to unique values of the column at `group_index`, and the inner values
                 are tuples of values from the column at `column_index`.
        """
        result: dict = {}
        for row in reference:
            if row[filter_key] == filter_value:
                block_key: str = row[block_index]
                table_key: str = row[group_index]
                if block_key not in result:
                    result[block_key] = {}
                if table_key in result[block_key]:
                    result[block_key][table_key] = result[block_key][table_key] + (row[column_index],)
                else:
                    result[block_key][table_key] = (row[column_index],)
        return result

    @staticmethod
    def _get_reference() -> Sequence:
        client: Client = get_client(
            host=get_my_env_var('HOST'),
            database=get_my_env_var('DATABASE'),
            username=get_my_env_var('USERNAME_DB'),
            password=get_my_env_var('PASSWORD')
        )
        return client.query("SELECT * FROM reference_dkp").result_rows

    @staticmethod
    def _clean_number(value: str) -> Union[float, int]:
        """
        Clean a given string by removing all characters that are not digits, decimal points, or minus signs.
        Then convert the cleaned string to a float if it contains a decimal point, or an int otherwise.
        :param value: The string to clean and convert.
        :return: The cleaned and converted value, as a float or an int.
        """
        cleaned = re.sub(NUMBER_CLEANING_PATTERN, '', value)
        if '.' in cleaned or 'e' in cleaned.lower():
            return float(cleaned)
        else:
            return int(cleaned)

    def _is_digit(self, x: str) -> bool:
        """
        Checks if a given string can be converted to a float.
        :param x: The string to check.
        :return: True if x can be converted to a float, False otherwise.
        """
        try:
            self._clean_number(x)
            return True
        except ValueError:
            return False

    @staticmethod
    def _merge_two_dicts(x: Dict, y: Dict) -> dict:
        """
        Merge two dictionaries, x and y, into a new dictionary, z.
        The values in y will overwrite the values in x if there are any keys in common.

        :param x: The first dictionary to merge.
        :param y: The second dictionary to merge.
        :return: A new dictionary with the merged values.
        """
        z: Dict = x.copy()  # start with keys and values of x
        z.update(y)  # modifies z with keys and values of y
        return z

    def _get_list_columns(self) -> List[str]:
        """
        Returns a list of all possible column names.

        This function takes all the values from COLUMN_NAMES (which is a dictionary of lists)
        and flattens them into a single list. This list is then returned.

        :return: A list of column names.
        """
        list_columns: list = []
        for keys in list(self.columns_names.values()):
            list_columns.extend(iter(keys))
        return list_columns

    @staticmethod
    def _remove_symbols_in_columns(row: Optional[str]) -> str:
        """
        Removes extra spaces and newline characters from a string.

        This method takes a string, removes any extra spaces and newline characters,
        and returns the cleaned string. If the input is not a string, it returns the input as is.

        :param row: The string from which symbols are to be removed.
        :return: The cleaned string with extra spaces and newline characters removed.
        """
        if row and isinstance(row, str):
            row: str = re.sub(r'\s+', ' ', row).strip()
        return row

    def _get_count_match_of_header(self, row: list, list_columns: list) -> int:
        """
        Counts the number of columns in a row that match with a list of column names.

        This method takes a row and a list of column names and returns the count of columns
        in the row that match with the column names. It first removes extra spaces and newline
        characters from the row, and then counts the number of columns that match with the
        list of column names.

        :param row: The row in which to count the columns.
        :param list_columns: The list of column names to match with.
        :return: The count of columns in the row that match with the list of column names.
        """
        row: list = list(map(self._remove_symbols_in_columns, row))
        count: int = sum(element in list_columns for element in row)
        return count

    def get_columns_position(self, row: list, block_position: list, headers: dict, dict_columns_position) -> None:
        """
        Finds the position of all columns in a row.

        This method takes a row, a range of columns (block_position), a dictionary of headers,
        and a dictionary to store the positions of the columns.
        It removes any extra spaces and newline characters from the strings in the row,
        and then iterates over the strings in the row.
        For each string, it checks if it matches any of the strings in the headers dictionary,
        and if it does, it stores the index of the string in the row in the dict_columns_position dictionary.
        The index is stored with the English name of the column as the key.

        :param row: The row to find the columns in.
        :param block_position: A list containing the start and end index of the block of columns to search in.
        :param headers: A dictionary of headers, where the keys are the English names of the columns,
                        and the values are lists of strings that may appear in the row as the header.
        :param dict_columns_position: A dictionary to store the positions of the columns in.
        :return: None
        """
        start_index, end_index = block_position
        row: list = list(map(self._remove_symbols_in_columns, row))
        for index, col in enumerate(row):
            for eng_column, columns in headers.items():
                for column_eng in columns:
                    if col == column_eng and start_index <= index < end_index:
                        dict_columns_position[eng_column] = index

    def check_errors_in_columns(self, dict_columns: dict, message: str) -> None:
        """
        Checks if there are any empty columns in the given dictionary.

        This method takes a dictionary of columns, a message, and an error code.
        It checks if there are any empty columns in the dictionary (i.e., columns with a value of None).
        If there are, it logs an error message with the error code, prints the error code to stderr,
        sends a message to Telegram with the error code and the names of the empty columns,
        and then exits with the error code.

        :param dict_columns: A dictionary of columns, where the keys are the English names of the columns,
                             and the values are the positions of the columns in the row.
        :param message: The message to log and print in case of an error.
        :return: None
        """
        if empty_columns := [key for key, value in dict_columns.items() if value is None]:
            logger.error(f"{message}. Empty columns - {empty_columns}")
            print("2", file=sys.stderr)
            telegram(
                f"Error code 2: {message}! Не были найдены следующие поля - {empty_columns}! "
                f"Файл: {self.basename_filename}"
            )
            sys.exit(2)

    def check_errors_in_header(self, row: list, max_df_columns: int) -> None:
        """
        Checks if there are any empty columns in the header of the given row.

        This method takes a row of the Excel file and checks if there are any empty columns in the header.
        If there are, it logs an error message with the error code 2, prints the error code to stderr,
        sends a message to Telegram with the error code and the names of the empty columns,
        and then exits with the error code 2.

        It also checks if there are any repeated columns in the header and if there are,
        it logs an error message with the error code 2, prints the error code to stderr,
        sends a message to Telegram with the error code and the names of the repeated columns,
        and then exits with the error code 2.

        :param row: The row of the Excel file to check.
        :param max_df_columns: The maximum number of columns in the DataFrame.
        :return: None
        """
        self.check_errors_in_columns(
            dict_columns=self.dict_block_position,
            message="Блоки текста отсутствуют в файле или изменены"
        )
        self.get_columns_position(row, [0, max_df_columns], self.columns_names, self.dict_columns_position)

        items: list = list(self.dict_block_position.items())
        dict_block_position_ranges = {
            current_key: [start_index, next_index]
            for (current_key, start_index), (_, next_index) in zip(items, items[1:] + [(None, len(row))])
        }

        for col, block_position in dict_block_position_ranges.items():
            if repeated_column := self.block_table_columns.get(col):
                self.get_columns_position(row, block_position, repeated_column, self.dict_columns_position)

        dict_columns_position: dict = self.dict_columns_position.copy()
        for delete_column in self.floating_columns:
            del dict_columns_position[delete_column]

        self.check_errors_in_columns(
            dict_columns=dict_columns_position,
            message="Столбцы отсутствуют в файле или изменены"
        )

    def _is_table_starting(self, row: list) -> bool:
        """
        Checks if the table is starting in the given row.

        This method takes a row of the Excel file and checks if the table is starting in that row.
        It does this by trying to get the value of the column "client" in the row.
        If the column is not found, it returns False.
        If the column is found, it returns the value of the column.

        :param row: The row of the Excel file to check.
        :return: The value of the column "client" if the table is starting in the row, False otherwise.
        """
        try:
            return row[self.dict_columns_position["client"]]
        except TypeError:
            return False

    def write_to_json(self, list_data: List[dict]) -> None:
        """
        Writes the given list of dictionaries to a JSON file.

        This method takes a list of dictionaries and writes them to a JSON file.
        The file is written to the same folder as the given Excel file.
        The filename is the same as the Excel file, but with a `.json` extension instead of `.xls`.
        If the list of dictionaries is empty, it logs an error message with the error code 4,
        prints the error code to stderr, sends a message to Telegram with the error code and the name of the file,
        and then exits with the error code.

        :param list_data: The list of dictionaries to write to the JSON file.
        :return: None
        """
        if not list_data:
            logger.error("Error code 4: length list equals 0!")
            print("4", file=sys.stderr)
            telegram(f"Error code 4: В Файле отсутствуют данные! Файл: {self.basename_filename}")
            sys.exit(4)
        output_file_path = os.path.join(self.folder, f'{self.basename_filename}.json')
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(list_data, f, ensure_ascii=False, indent=4, cls=JsonEncoder)

    def _extract_value(self, rows: list, column: str) -> Optional[str]:
        """
        Extracts a value from a specified column in a list of rows.

        This method attempts to retrieve a value from the specified column
        in the provided list of rows. It checks the position of the column
        within the rows using a dictionary of column positions. If the
        position is invalid or exceeds the length of the rows, it returns
        None. Otherwise, it returns the value from the specified position,
        stripping any leading or trailing whitespace if the value is not None.

        :param rows: The list of rows to extract the value from.
        :param column: The name of the column to extract the value from.
        :return: The extracted value as a string with whitespace removed,
        or None if the position is invalid or the value is None.
        """
        position: int = self.dict_columns_position.get(column)
        if position is None or position >= len(rows):
            return None
        value: str = rows[position]
        return value.strip() if value else None

    def _convert_value(self, value: Optional[str]) -> Union[str, float, int, bool, None]:
        """
        Converts the given value to a number if possible.

        This method takes a string and tries to convert it to a number.
        If the string is not a number, it returns the original string.
        If the string is a number, it returns a float if the number has a decimal point,
        or an int otherwise.

        If the string is None, it returns None.

        If the string is "yes" or "да", it returns True.
        If the string is "no" or "нет", it returns False.

        :param value: The string to convert to a number.
        :return: The converted value, or the original value if it could not be converted.
        """
        if value is None:
            return None

        lower_value: str = value.lower()
        if lower_value in {"да", "yes"}:
            return True
        elif lower_value in {"нет", "no"}:
            return False

        if not self._is_digit(value):
            return value

        try:
            return self._clean_number(value)
        except (ValueError, TypeError) as e:
            logger.warning(
                f"Value conversion failed. "
                f"Original value: '{value}' (type: {type(value).__name__}). "
                f"Error details: {str(e)}. Returning original value."
            )
            return value

    @staticmethod
    def _check_numeric(value_with_field_name: tuple) -> Union[int, float, None]:
        """
        Checks if the given value is numeric and returns it as a number.

        If the value is None, it raises a ValueError.
        If the value is not a number, it raises a ValueError with a message
        indicating that the value is non-numeric.

        :param value_with_field_name: The value and field name to check and convert.
        :return: The value as a number, or raises a ValueError.
        """
        value, field_name = value_with_field_name
        if field_name is None:
            return None
        elif value is None:
            raise ValueError(f"Поле '{field_name}' содержит значение: None")
        try:
            return float(value)  # Преобразуем значение в число
        except (ValueError, TypeError) as e:
            raise ValueError(f"Поле '{field_name}' содержит нечисловое значение: {value}") from e

    def parse_value(self, rows: list, column: str) -> Union[str, float, int, bool, None]:
        """
        Extracts and converts a value from the given row.

        This method takes a list of rows and a column name,
        extracts the value from the row using the column name,
        and converts the value to a number if possible.

        The conversion is done by the `_convert_value` method.

        :param rows: The list of rows to extract the value from.
        :param column: The column name to extract the value from.
        :return: The extracted and converted value, or None if the value is not found or cannot be converted.
        """
        raw_value: Optional[str] = self._extract_value(rows, column)
        return self._convert_value(raw_value)

    def get_content_in_table(
        self,
        index_month: int,
        month_string: str,
        row: list,
        metadata: dict
    ) -> dict:
        """
        Extracts and processes data from a row in a table, returning a dictionary of parsed values.

        This function takes a row from a table and extracts various data points such as client details,
        project information, and financial metrics. It uses a helper function `parse_value` to clean and
        convert the data appropriately. The parsed data is returned as a dictionary along with additional
        metadata.

        :param index_month: The numeric representation of the month.
        :param month_string: The string representation of the month.
        :param row: The list of values in the current row.
        :param metadata: Additional metadata extracted earlier in the process.
        :return: A dictionary containing parsed and processed data from the row.
        """
        parsed_record: dict = {
            "client": self.parse_value(row, "client"),
            "description": self.parse_value(row, "description"),
            "project": self.parse_value(row, "project"),
            "cargo": self.parse_value(row, "cargo"),
            "direction": self.parse_value(row, "direction"),
            "bay": self.parse_value(row, "bay"),
            "owner": self.parse_value(row, "owner"),
            "container_size": self.parse_value(row, "container_size"),
            "month": index_month,
            "month_string": month_string,
            "date": f"{metadata['year']}-{index_month:02d}-01",

            "container_count": next((
                self.parse_value(row, key)
                for key, val in self.block_table_columns["natural_indicators_ktk"].items()
                if month_string in val
            ), None),
            "teu": next((
                self.parse_value(row, key)
                for key, val in self.block_table_columns["natural_indicators_teus"].items()
                if month_string in val
            ), None),
            "profit_plan_thousand_rub": self._check_numeric(
                next((
                    (self.parse_value(row, key), key)
                    for key, val in self.block_table_columns["profit_plan"].items()
                    if month_string in val
                ), (None, None))
            ),
            "costs_plan_thousand_rub": self._check_numeric(
                next((
                    (self.parse_value(row, key), key)
                    for key, val in self.block_table_columns["costs_plan"].items()
                    if month_string in val
                ), (None, None))
            ),

            "original_file_name": self.basename_filename,
            "original_file_parsed_on": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        profit_plan: float = parsed_record.get("profit_plan_thousand_rub") or 0
        costs_plan: float = parsed_record.get("costs_plan_thousand_rub") or 0
        parsed_record["margin_plan_thousand_rub"] = profit_plan - costs_plan

        fields_check: list = ["client", "container_size"]
        for block_key in [key for key in self.block_table_columns if key not in NOT_COUNT_BLOCK]:
            for field in self.block_table_columns[block_key]:
                for column in fields_check:
                    if column in field:
                        val1: Union[str, float, int, bool, None] = self.parse_value(row, field) or None
                        val2: Union[str, float, int, bool, None] = parsed_record[column] or None
                        if val1 != val2:
                            raise ValueError(f"{column}: '{val2}' not equal to {field}: '{val1}'")
                parsed_record[field] = self.parse_value(row, field)

        return self._merge_two_dicts(metadata, parsed_record)

    def extract_metadata_from_filename(self) -> dict:
        """
        Extract metadata from filename.

        This method takes the filename of the given Excel file and extracts metadata from it.
        It matches the department and year from the filename and logs an error with the error code 10
        if the department is not found and an error with the error code 1 if the year is not found.
        Then it sends a message to Telegram with the error code and the filename
        if an error occurs and exits with the error code.

        :return: A dictionary with the extracted metadata.
        """
        logger.info(f'File - {self.basename_filename}. Datetime - {datetime.now()}')
        # Match department
        dkp_pattern: str = '|'.join(map(re.escape, self.dkp_names.keys()))
        department_match: Match = re.search(rf'{dkp_pattern}', self.basename_filename)
        if not department_match:
            self.send_error(
                message='Error code 10: Department не указан в файле! Файл:', error_code=10
            )
        metadata: dict = {'department': self.dkp_names[f"{department_match.group(0)}"]}
        # Match year
        year_match: Match = re.search(r'\d{4}', self.basename_filename)
        if not year_match:
            self.send_error(
                message='Error code 1: Год не указан в файле! Файл:', error_code=1
            )
        metadata['year'] = int(year_match.group(0))

        return metadata

    def send_error(self, message: str, error_code: int) -> None:
        """
        Sends an error message to the logger and Telegram, then exits with the given error code.

        This method takes a message and an error code, appends the current file's basename to the message,
        logs the error message, sends it to Telegram, and exits the program with the specified error code.

        :param message: The error message to log and send.
        :param error_code: The error code to exit with.
        :return: None
        """
        error_message: str = f"{message} {self.basename_filename}"
        logger.error(error_message)
        telegram(error_message)
        sys.exit(error_code)

    def parse_sheet(self, df: pd.DataFrame, max_df_columns: int, count_match_header: int = 7) -> None:
        """
        Parse a sheet of Excel file.

        This method takes a pandas DataFrame, representing a sheet of the Excel file,
        and parses it, extracting metadata from the filename,
        identifying the header and the table, and extracting content from the table.
        It then writes the extracted content to a JSON file.

        If an error occurs during processing, it logs an error message with the error code 5,
        sends a message to Telegram with the error code and the filename,
        and then exits with the error code 5.

        :param df: The pandas DataFrame representing the sheet of the Excel file.
        :param max_df_columns: The maximum number of columns in the DataFrame.
        :param count_match_header: The coefficient to determine if a row is a header or not.
        :return: None
        """
        list_data: list = []
        metadata: dict = self.extract_metadata_from_filename()
        list_columns: List[str] = self._get_list_columns()
        index: Union[int, Hashable]
        for index, row in df.iterrows():
            row = list(row.to_dict().values())
            if self._get_count_match_of_header(row, list_columns) >= count_match_header:
                self.check_errors_in_header(row, max_df_columns)
            elif not self.dict_columns_position["client"]:
                self.get_columns_position(row, [0, len(row)], self.block_names, self.dict_block_position)
            elif self._is_table_starting(row):
                try:
                    list_data.extend(
                        self.get_content_in_table(index_month, month_string, row, metadata)
                        for index_month, month_string in enumerate(MONTH_NAMES, start=1)
                    )
                except (IndexError, ValueError, TypeError, AttributeError) as exception:
                    telegram(
                        f"Error code 5: Ошибка возникла в строке {index + 1}! "
                        f"Файл: {self.basename_filename}. Exception - {exception}"
                    )
                    logger.error(f"Error code 5: error processing in row {index + 1}! Exception - {exception}")
                    print(f"5_in_row_{index + 1}", file=sys.stderr)
                    sys.exit(5)
        self.write_to_json(list_data)

    def main(self) -> None:
        """
        The main method of the class.

        This method reads the Excel file given by the filename, extracts the needed sheets,
        parses the sheet, and writes the extracted data to a JSON file.

        If an error occurs during processing, it logs an error message,
        sends a message to Telegram with the error message,
        and exits with the error code 6.
        :return: None
        """
        try:
            sheets: list = pd.ExcelFile(self.filename).sheet_names
            logger.info(f"Sheets is {sheets}")
            needed_sheets: list = [sheet for sheet in sheets if sheet in self.sheets_name]
            if len(needed_sheets) > 3:
                raise ValueError(f"Нужных листов из SHEETS_NAME больше нужного: {needed_sheets}")
            replace_dict: dict = {np.nan: None, "NaT": None}
            dfs: List[DataFrame] = [
                pd.read_excel(self.filename, sheet_name=sheet, dtype=str, header=None)
                .dropna(how="all")
                .replace(replace_dict)
                for sheet in needed_sheets
            ]
            dfs.sort(key=lambda df: df.shape[1], reverse=True)  # Сортируем DataFrames по количеству столбцов (убывание)
            merged_df: DataFrame = pd.concat(dfs, axis=1).replace(replace_dict)
            merged_df.columns = range(merged_df.shape[1])  # Индексация столбцов для последовательности
            self.parse_sheet(merged_df, dfs[0].shape[1])
        except Exception as exception:
            logger.error(f"Ошибка при чтении файла {self.basename_filename}: {exception}")
            telegram(f'Error code 6: Ошибка при обработке файла! Файл: {self.basename_filename}! Ошибка: {exception}')
            print("unknown", file=sys.stderr)
            sys.exit(6)


if __name__ == "__main__":
    logger.info(f"{os.path.basename(sys.argv[1])} has started processing")
    dkp: DKP = DKP(os.path.abspath(sys.argv[1]), sys.argv[2])
    dkp.main()
    logger.info(f"{os.path.basename(sys.argv[1])} has finished processing")
