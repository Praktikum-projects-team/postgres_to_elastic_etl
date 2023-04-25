import abc
import datetime
import json
from typing import Any, Optional

from pydantic import BaseModel


class StateModel(BaseModel):
    movies_index_modified_person: Optional[datetime.datetime] = datetime.datetime.min
    movies_index_modified_genre: Optional[datetime.datetime] = datetime.datetime.min
    movies_index_modified_filmwork: Optional[datetime.datetime] = datetime.datetime.min

    person_index_modified: Optional[datetime.datetime] = datetime.datetime.min
    genre_index_modified: Optional[datetime.datetime] = datetime.datetime.min


class BaseStorage(abc.ABC):
    """Abstract state storage."""

    @abc.abstractmethod
    def save_state(self, state: dict[str, Any]) -> None:
        """Save state to the storage."""

    @abc.abstractmethod
    def retrieve_raw_state(self) -> dict[str, Any]:
        """Get state from the storage."""

    def retrieve_state(self) -> StateModel:
        raw_state = self.retrieve_raw_state()
        return StateModel(**raw_state)


class JsonFileStorage(BaseStorage):
    """State storage using local file.

    file_format: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: dict[str, Any]) -> None:
        with open(self.file_path, "w") as file:
            json.dump(state, file, default=str)

    def retrieve_raw_state(self) -> dict[str, Any]:
        try:
            with open(self.file_path, 'r') as file:
                return json.load(file)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            return {}


class State:
    """."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Set state for given key."""
        state = self.storage.retrieve_raw_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Get state by given key."""
        whole_state = self.storage.retrieve_state()
        return getattr(whole_state, key)
