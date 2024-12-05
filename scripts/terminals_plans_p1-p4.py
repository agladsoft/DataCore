import os
import sys
import json
import contextlib
import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime

headers_eng: dict = {
    "ЦФО расходов": "department",
    "внешний клиент": "client",
    "груз": "cargo",
    "направление": "direction",
    "разм": "container_size",
    "ставка": "rate",
    "услуга": "service",
    "технология": "technology",
    "СОИСПОЛНИТЕЛЬ": "co-executors",
    "сектор": "sector",
    "источник": "source",
    "участок": "section",
    "месяц": "month",
    "кол-во конт": "container_count",
    "Сумма, тыс.руб.": "amount_thousand_rubles"
}

dict_types: dict = {
    "кол-во конт": int,
    "Сумма, тыс.руб.": float,
    "ставка": float
}

month_mapping: dict = {
    'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6,
    'июл': 7, 'авг': 8, 'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12
}


date_formats: tuple = ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S")


class MarginIncomePlan(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def change_type_and_values(df: DataFrame) -> None:
        """
        Change data types or changing values.
        """
        with contextlib.suppress(Exception):
            df['rate'] = df['rate'].round(2)
            df['month'] = df['month'].map(month_mapping)
            df['amount_thousand_rubles'] = df['amount_thousand_rubles'].round(2)

    def add_new_columns(self, df: DataFrame) -> None:
        """
        Add new columns.
        """
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        df['teu'] = None  # Инициализируем столбец
        df.loc[df['container_size'] == 20, 'teu'] = df['container_count'] * 1
        df.loc[df['container_size'] == 40, 'teu'] = df['container_count'] * 2

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
