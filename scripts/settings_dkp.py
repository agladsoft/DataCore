import os
import requests
from requests import Response
from dotenv import load_dotenv
from notifiers import get_notifier

load_dotenv()


def get_my_env_var(var_name: str) -> str:
    """
    Retrieves the value of the given environment variable.
    :param var_name: The name of the environment variable to retrieve.
    :return: The value of the given environment variable.
    :raises MissingEnvironmentVariable: If the given environment variable does not exist.
    """
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


class MissingEnvironmentVariable(Exception):
    pass


NOT_COUNT_BLOCK: list = ["natural_indicators_ktk", "natural_indicators_teus"]
FILTER_SUFFIXES: tuple = ('_separation', '_fee', '_value_money', '_tax')

DATE_FORMATS: list = [
    "%m/%d/%y",
    "%d.%m.%Y",
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y",
    "%d%b%Y"
]

MONTH_NAMES: list = ["янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"]
NUMBER_CLEANING_PATTERN: str = r'\s|,'


def send_email_notifiers(message: str, subject: str = "Уведомление от системы DataCore"):
    """
    Отправка email через Mail.ru
    """
    try:
        email = get_notifier('email')
        email.notify(
            to=get_my_env_var('RECIPIENT_EMAIL'),
            subject=subject,
            message=message,
            from_=get_my_env_var('EMAIL_USER'),
            host='smtp.mail.ru',
            port=587,
            username=get_my_env_var('EMAIL_USER'),
            password=get_my_env_var('EMAIL_PASSWORD'),
            tls=True
        )
        print(f"Email успешно отправлен на {get_my_env_var('RECIPIENT_EMAIL')}")
    except Exception as e:
        print(f"Ошибка при отправке email: {e}")


def telegram(message) -> int:
    url: str = f"https://api.telegram.org/bot{get_my_env_var('TOKEN_TELEGRAM')}/sendMessage"
    params: dict = {
        "chat_id": f"{get_my_env_var('CHAT_ID')}/{get_my_env_var('TOPIC')}",
        "text": message,
        "reply_to_message_id": get_my_env_var('ID')
    }
    response: Response = requests.get(url, params=params)
    send_email_notifiers(params.get('text'))
    return response.status_code
