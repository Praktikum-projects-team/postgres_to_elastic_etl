import datetime
import logging
from typing import Generator

import backoff
import psycopg2
from psycopg2.extras import RealDictCursor

from config import PostgresConfig
from extraction.sql_statements import fw_statement, person_fw_statement, genre_fw_statement, person_statement
from extraction.state import State


class PersonReader:
    def __init__(self, connection_params: PostgresConfig, batch_size: int, state: State):
        self.connection_params = connection_params
        self.batch_size = batch_size
        self.state = state

    def get_last_datetime(self) -> datetime.datetime:
        return self.state.get_state(key='person_index_modified')

    def set_last_datetime(self, last_datetime: str):
        self.state.set_state(key='person_index_modified', value=last_datetime)

    def _read_all_modified(self, cursor: RealDictCursor) -> Generator:
        logging.info('person reading started')
        cursor.execute(person_statement, [self.get_last_datetime()])
        while data := cursor.fetchmany():
            modified_datetimes = [person.pop('modified') for person in data]
            yield data
            self.set_last_datetime(modified_datetimes[-1])
        logging.info('person reading finished')

    def _gen_data(self, connection) -> Generator:
        try:
            with connection.cursor() as cursor:
                cursor.arraysize = self.batch_size
                yield self._read_all_modified(cursor)
        finally:
            connection.close()

    @backoff.on_exception(
        backoff.expo,
        psycopg2.OperationalError,
        max_time=300,
    )
    def read_data(self) -> Generator:
        with psycopg2.connect(**(self.connection_params.dict()), cursor_factory=RealDictCursor) as connection:
            return self._gen_data(connection=connection)
