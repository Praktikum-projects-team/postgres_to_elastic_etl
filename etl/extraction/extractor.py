import abc
import collections
import datetime
import logging
from typing import Generator

import backoff
import psycopg2
from psycopg2.extras import RealDictCursor

from config import PostgresConfig
from extraction.sql_statements import fw_statement, person_fw_statement, genre_fw_statement, person_statement
from extraction.state import State


class PostgresReader(abc.ABC):
    """class for postgres data extraction"""
    def __init__(self, connection_params: PostgresConfig, batch_size: int, state: State):
        self.connection_params = connection_params
        self.batch_size = batch_size
        self.state = state

    @abc.abstractmethod
    def _read_all_modified(self, cursor: RealDictCursor) -> Generator:
        """read batches of modified records"""
        raise NotImplementedError

    def _gen_data(self, connection):
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
    def read_data(self) -> collections.abc.Generator:
        with psycopg2.connect(**(self.connection_params.dict()), cursor_factory=RealDictCursor) as connection:
            return self._gen_data(connection=connection)


class FilmworkReader(PostgresReader):

    def get_last_datetime(self, table: str) -> datetime.datetime:
        return self.state.get_state(key=table)

    def set_last_datetime(self, table: str, last_datetime: str):
        self.state.set_state(key=table, value=last_datetime)

    def _read_modified_filmworks(self, cursor: RealDictCursor):
        """read modified data from film_work table"""

        logging.info('filmworks reading started')
        cursor.execute(fw_statement, [self.get_last_datetime('filmwork')])
        while data := cursor.fetchmany():
            yield data
            self.set_last_datetime('filmwork', data[-1]['modified'])
        logging.info('filmworks reading finished')

    def _read_modified_secondary_table(self, cursor: RealDictCursor, table: str, fw_statement: str):
        """read modified data from tables related to film_work table"""

        logging.info(f'{table} reading started')
        while True:
            entity_statement = f'''
                SELECT id, modified
                FROM content.{table}
                WHERE modified > %s
                ORDER BY modified
                LIMIT 100;         
            '''
            cursor.execute(entity_statement, [self.get_last_datetime(table)])
            entities = cursor.fetchall()

            if not entities:
                break

            new_state = entities[-1]['modified']

            entity_ids = [entity['id'] for entity in entities]
            cursor.execute(fw_statement, [entity_ids, self.get_last_datetime('filmwork')])
            while data := cursor.fetchmany():  # как default использует cursor.arraysize, который я задаю перед вызовом
                yield data

            self.set_last_datetime(table, new_state)
        logging.info(f'{table} reading finished')

    def _read_all_modified(self, cursor: RealDictCursor):
        yield from self._read_modified_secondary_table(cursor, table='person', fw_statement=person_fw_statement)
        yield from self._read_modified_secondary_table(cursor, table='genre', fw_statement=genre_fw_statement)
        yield from self._read_modified_filmworks(cursor)


class PersonReader(PostgresReader):
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
