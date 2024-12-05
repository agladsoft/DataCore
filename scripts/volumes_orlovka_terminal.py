import sys
import json
import contextlib
import numpy as np
import pandas as pd
from __init__ import *
from typing import Optional
from pandas import DataFrame
from datetime import datetime

headers_eng: dict = {
    "Контейнеры гружёные":
        {
            "№ ктк": "container_number",
            "Тип ктк": "container_size",
            "Дата оформления": "date_registration",
            "Сумма ПРР гружёный": "amount_cargo_handling_loaded"
        },
    "Контейнеры порожние":
        {
            "№ ктк": "container_number",
            "Тип ктк": "container_size",
            "Дата прибытия": "date_arrival",
            "Дата вывоза": "date_departure",
            "кол-во дней хранения": "storage_days",
            "стоимость хранения": "storage_cost",
            "Ставка ПРР приём": "rate_cargo_handlin_arrival",
            "Ставка ПРР выдача": "rate_cargo_handlin_export",
            "Примечание": "comment"
        },
    "Контейнеры прибытие":
        {
            "№ ктк": "container_number",
            "Тип ктк": "container_size",
            "Дата прибытия": "date_arrival"
        },
    "Контейнеры убытие":
        {
            "№ ктк": "container_number",
            "Тип ктк": "container_size",
            "Дата убытия": "date_departure"
        },
    "Платформы":
        {
            "№ платформы": "platform_number",
            "Тип платформы": "platform_type",
            "Дата прибытия": "date_arrival",
            "Дата убытия": "date_departure",
            "кол-во дней хранения": "storage_days",
            "стоимость хранения": "storage_cost",
            "Примечание": "comment"
        },
}

dict_types: dict = {
    'Тип ктк': int,
    "Тип платформы": int,
    "кол-во дней хранения": int,
    "стоимость хранения": int,
    "Ставка ПРР приём": int,
    "Ставка ПРР выдача": int
}

logger: logging.getLogger = get_logger(f"data_extractor {str(datetime.now().date())}")
date_formats: tuple = ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S")


class VolumesOrlovkaTerminal(object):
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

    def change_type_and_values(self, df: DataFrame) -> None:
        """
        Change data types or changing values.
        """
        # Список столбцов для обработки
        date_columns = ['date_registration', 'date_arrival', 'date_departure']
        for column in date_columns:
            if column in df.columns:
                with contextlib.suppress(Exception):
                    df[column] = df[column].apply(lambda x: self.convert_format_date(str(x)))

    def add_new_columns(self, df: DataFrame) -> None:
        """
        Add new columns.
        """
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def write_to_json(self, parsed_data: list, sheet: str) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(self.input_file_path)
        output_file_path: str = os.path.join(self.output_folder, f'{basename}_{sheet}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        logger.info(f"{os.path.basename(self.input_file_path)} has started processing")
        try:
            sheets = pd.ExcelFile(self.input_file_path).sheet_names
            logger.info(f"Sheets is {sheets}")
            for sheet in sheets:
                if headers_eng.get(sheet):
                    df: DataFrame = pd.read_excel(self.input_file_path, sheet_name=sheet, dtype=dict_types)
                    df = df.dropna(axis=0, how='all')
                    df = df.rename(columns=headers_eng.get(sheet))
                    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                    self.add_new_columns(df)
                    self.change_type_and_values(df)
                    df = df.replace({np.nan: None, "NaT": None})
                    self.write_to_json(df.to_dict('records'), sheet)
        except Exception as ex:
            logger.error(f"Ошибка при чтении файла {self.input_file_path}: {ex}")
        logger.info(f"{os.path.basename(self.input_file_path)} has finished processing")


if __name__ == '__main__':
    table: VolumesOrlovkaTerminal = VolumesOrlovkaTerminal(sys.argv[1], sys.argv[2])
    table.main()
