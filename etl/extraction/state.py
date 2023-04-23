import abc
import datetime
import json
from typing import Any, Optional

from pydantic import BaseModel


class StateModel(BaseModel):
    person: Optional[datetime.datetime] = datetime.datetime.min
    genre: Optional[datetime.datetime] = datetime.datetime.min
    filmwork: Optional[datetime.datetime] = datetime.datetime.min


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_raw_state(self) -> dict[str, Any]:
        """Получить состояние из хранилища."""

    def retrieve_state(self) -> StateModel:
        raw_state = self.retrieve_raw_state()
        return StateModel(**raw_state)


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, "w") as file:
            json.dump(state, file, default=str)

    def retrieve_raw_state(self) -> dict[str, Any]:
        try:
            with open(self.file_path, 'r') as file:
                return json.load(file)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            return {}


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        state = self.storage.retrieve_raw_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        whole_state = self.storage.retrieve_state()
        return getattr(whole_state, key)
