import os
import re
import sys
import json
import contextlib
import numpy as np
import pandas as pd
from typing import Optional
from pandas import DataFrame
from datetime import datetime

headers_eng: dict = {
    "цфо 2 ур": "department",
    "организация": "organization",
    "клиент": "client",
    "размерность": "container_size",
    "проект": "project",
    "груз": "cargo",
    "направление": "direction",
    "бассейн": "basin",
    "принадлежность ктк": "container_owned",
    "дата": "date",
    "ктк": "container_count",
    "TEU": "teu"
}

dict_types: dict = {
    'размерность': int,
    'ктк': int,
    "TEU": int
}

terminals: list = ['ORLOVKA', 'MANP']
date_formats: tuple = ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S")


class TerminalsPlansOrlovkaManp(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    def get_terminal_in_filename(self, df: DataFrame):
        """
        Getting the terminal at the beginning of the file.
        """
        # Преобразуем терминалы в верхний регистр и создаем регулярное выражение
        terminal_pattern = re.compile(rf"({'|'.join(terminals).upper()})", re.IGNORECASE)

        # Получаем имя файла в верхнем регистре
        file_name = os.path.basename(self.input_file_path).upper()

        # Проверяем на соответствие
        terminal_match = terminal_pattern.search(file_name)
        if terminal_match is None:
            raise AssertionError('Terminal not in file name!')
        df['terminal'] = terminal_match[0].lower()

    @staticmethod
    def convert_format_date(date: str) -> Optional[str]:
        """
        Convert to a date type.
        """
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())
        return None

    def change_type_and_values(self, df: DataFrame) -> None:
        """
        Change data types or changing values.
        """
        with contextlib.suppress(Exception):
            df['date'] = df['date'].apply(lambda x: self.convert_format_date(str(x)))

    def add_new_columns(self, df: DataFrame) -> None:
        """
        Add new columns.
        """
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def write_to_json(self, parsed_data: list) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(self.input_file_path)
        output_file_path: str = os.path.join(self.output_folder, f'{basename}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        df: DataFrame = pd.read_excel(self.input_file_path, dtype=dict_types)
        df = df.dropna(axis=0, how='all')
        df = df.rename(columns=headers_eng)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        self.get_terminal_in_filename(df)
        self.add_new_columns(df)
        self.change_type_and_values(df)
        df = df.replace({np.nan: None, "NaT": None})
        self.write_to_json(df.to_dict('records'))


if __name__ == '__main__':
    table: TerminalsPlansOrlovkaManp = TerminalsPlansOrlovkaManp(sys.argv[1], sys.argv[2])
    table.main()
