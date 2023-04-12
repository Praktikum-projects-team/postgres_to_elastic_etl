import abc
import datetime
import json
# from pathlib import Path
from typing import Any

# from redis import Redis


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
    def retrieve_state(self) -> dict[str, Any]:
        """Получить состояние из хранилища."""


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

    def retrieve_state(self) -> dict[str, Any]:
        try:
            with open(self.file_path, 'r') as file:
                return json.load(file)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            # Path(self.file_path).touch()
            return {}


# class RedisStorage(BaseStorage):
#     def __init__(self, redis_adapter: Redis):
#         self.redis_adapter = redis_adapter
#
#     def save_state(self, state: Dict[str, Any]) -> None:
#         ...
#         # with open(self.file_path, "w") as file:
#         #     json.dump(state, file)
#
#     def retrieve_state(self) -> Dict[str, Any]:
#         ...
#         # if not os.path.exists(self.file_path):
#         # try:
#         #     with open(self.file_path, 'r') as file:
#         #         return json.load(file)
#         # except FileNotFoundError:
#         #     # Path(self.file_path).touch()
#         #     return {}


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        state = self.storage.retrieve_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        state = self.storage.retrieve_state()
        return state.get(key, None)

    def get_last_state(self) -> datetime.datetime:
        """Получить состояние по определённому ключу."""
        state = self.storage.retrieve_state()
        state = state.get('modified', None)
        return datetime.datetime.strptime(state, '%Y-%m-%d %H:%M:%S.%f%z') if state else datetime.datetime.min
