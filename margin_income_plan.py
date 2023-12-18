import os
import sys
import json
import contextlib
import numpy as np
import pandas as pd
from typing import Optional
from pandas import DataFrame
from datetime import datetime

headers_eng: dict = {
    "Месяц": "month",
    "Год": "year",
    "ЦФО": "department",
    "Выручка, тыс. руб.": "income_thousand_rub",
    "МД, тыс. руб.": "margin_income_thousand_rub",
    "Расходы, тыс. руб.": "expenses_thousand_rub"
}

dict_types: dict = {
    'Месяц': int,
    "Год": int,
    "Выручка, тыс. руб.": float,
    "МД, тыс. руб.": float,
    "Расходы, тыс. руб.": float,

}


date_formats: tuple = ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S")


class MarginIncomePlan(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def convert_format_date(date: str) -> Optional[str]:
        """
        Convert to a date type.
        """
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())
        return None

    @staticmethod
    def change_type_and_values(df: DataFrame) -> None:
        """
        Change data types or changing values.
        """
        with contextlib.suppress(Exception):
            df['income_thousand_rub'] = df['income_thousand_rub'].round(2)
            df['margin_income_thousand_rub'] = df['margin_income_thousand_rub'].round(2)
            df['expenses_thousand_rub'] = df['expenses_thousand_rub'].round(2)

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
        self.add_new_columns(df)
        self.change_type_and_values(df)
        df = df.replace({np.nan: None, "NaT": None})
        self.write_to_json(df.to_dict('records'))


if __name__ == '__main__':
    table: MarginIncomePlan = MarginIncomePlan(sys.argv[1], sys.argv[2])
    table.main()
