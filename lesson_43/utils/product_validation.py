import pandas as pd
import logging
import requests
from datetime import datetime
from airflow.models import Variable
from pydantic import BaseModel, ValidationError, conint, constr, confloat

# --- Конфигурация логгера и переменных Telegram ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TELEGRAM_TOKEN = Variable.get("telegram_token")
TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")

# --- Отправка сообщений в Telegram ---
def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        response = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': message})
        if not response.ok:
            logger.error(f"[Telegram] Ошибка: {response.status_code} {response.text}")
    except Exception as e:
        logger.error(f"[Telegram] Исключение при отправке: {e}")

# --- Логирование и уведомления ---
def log_and_notify(message: str, level: str = "info"):
    tag = {"info": "[v]", "error": "[x]", "warning": "[!]"}[level]
    full_message = f"{tag} {message}"
    getattr(logger, level)(full_message)
    send_telegram_message(full_message)

# --- Pydantic-модели ---
class ProductProductModel(BaseModel):
    product_id: conint(gt=0)
    product_name: constr(strip_whitespace=True, min_length=1)

class OrdersProductModel(BaseModel):
    order_id: conint(gt=0)
    order_date: datetime

class OrderItemsProductModel(BaseModel):
    order_id: conint(gt=0)
    product_id: conint(gt=0)
    quantity: conint(gt=0)
    price: confloat(gt=0)

# --- Pydantic-валидация DataFrame ---
def validate_with_pydantic(df: pd.DataFrame, model_class: BaseModel):
    errors = []
    for i, row in df.iterrows():
        try:
            model_class(**row.to_dict())
        except ValidationError as e:
            errors.append(f"Строка {i + 1}: {e}")
    if errors:
        raise ValueError("Ошибки валидации:\n" + "\n".join(errors))

# --- Валидация CSV-файла: пустота, дубликаты, пропуски, схема ---
def validate_csv_file(table_name: str, **kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids=f'wait_for_{table_name}')
    df = pd.read_csv(file_path)

    if df.empty:
        log_and_notify(f"{table_name}: файл пустой", "error")
        raise ValueError("Пустой файл")

    if df.duplicated().any():
        log_and_notify(f"{table_name}: дубликаты", "error")
        raise ValueError("Дубликаты")

    if df.isnull().any().any():
        log_and_notify(f"{table_name}: пропуски", "error")
        raise ValueError("Пропуски")

    model_map = {
        'products_product': ProductProductModel,
        'orders_product': OrdersProductModel,
        'order_items_product': OrderItemsProductModel
    }

    model_class = model_map.get(table_name)
    if not model_class:
        log_and_notify(f"{table_name}: не найдена модель", "error")
        raise ValueError("Нет модели")

    validate_with_pydantic(df, model_class)
    log_and_notify(f"{table_name}: валидация пройдена ({len(df)} строк)")
