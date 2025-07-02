from pydantic import BaseModel, Field

# === User Models ===
# Базовая модель пользователя, содержит общую информацию
class UserBase(BaseModel):
    user_id: str = Field(..., max_length=20)  # Обязательное поле: строка не длиннее 20 символов
    name: str        # Имя пользователя
    surname: str     # Фамилия пользователя
    age: int         # Возраст
    email: str       # Электронная почта
    phone: str       # Телефон

# Наследник UserBase — может использоваться, например, для API, базы данных и т.п.
class User(UserBase):
    pass  # Пока ничего не добавляем, просто наследуем всё от UserBase

# Модель банковской карты, связанной с пользователем
class Card(BaseModel):
    card_number: str  # Номер карты
    user_id: str      # Идентификатор пользователя, которому принадлежит карта
