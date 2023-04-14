import datetime
import logging
from contextlib import closing

import backoff
import psycopg2
from psycopg2.extras import RealDictCursor

from config import PostgresConfig
from sql_statements import fw_statement, person_fw_statement, genre_fw_statement
from state import State


class PostgresReader:
    def __init__(self, connection_params: PostgresConfig, batch_size: int, state: State):
        self.connection_params = connection_params
        self.batch_size = batch_size
        self.state = state

    def get_last_datetime(self, table: str) -> datetime.datetime:
        return self.state.get_state(key=table)

    def set_last_datetime(self, table: str, last_datetime: str):
        self.state.set_state(key=table, value=last_datetime)

    def _read_modified_filmworks(self, cursor: RealDictCursor):
        logging.info('filmworks reading started')
        cursor.execute(fw_statement, [self.get_last_datetime('filmwork')])
        while data := cursor.fetchmany():
            yield data
            self.set_last_datetime('filmwork', data[-1]['modified'])
        logging.info('filmworks reading finished')

    def _read_modified_person_filmworks(self, cursor: RealDictCursor):
        logging.info('persons reading started')
        while True:
            person_statement = '''
                SELECT id, modified
                FROM content.person
                WHERE modified > %s
                ORDER BY modified
                LIMIT 100;         
            '''
            cursor.execute(person_statement, [self.get_last_datetime('person')])
            persons = cursor.fetchall()
            print(persons)
            if not persons:
                break
            new_state = persons[-1]['modified']
            persons_ids = [person['id'] for person in persons]


            cursor.execute(person_fw_statement, [persons_ids, self.get_last_datetime('filmwork')])
            while data := cursor.fetchmany():
                yield data

            self.set_last_datetime('person', new_state)
        logging.info('persons reading finished')

    def _read_modified_secondary_table(self, cursor: RealDictCursor, table: str, fw_statement: str):
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
            while data := cursor.fetchmany():
                yield data

            self.set_last_datetime(table, new_state)
        logging.info(f'{table} reading finished')

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_time=300)
    def read_data(self):
        with closing(psycopg2.connect(**(self.connection_params.dict()), cursor_factory=RealDictCursor)) as connection:
            cursor = connection.cursor()
            cursor.arraysize = self.batch_size
            yield self._read_modified_secondary_table(cursor, table='person', fw_statement=person_fw_statement)
            yield self._read_modified_secondary_table(cursor, table='genre', fw_statement=genre_fw_statement)
            yield self._read_modified_filmworks(cursor)


if __name__ == '__main__':
    from state import JsonFileStorage

    storage = JsonFileStorage('state_file.json')
    state = State(storage)
    pg_read = PostgresReader(connection_params=PostgresConfig(), state=state, batch_size=3)
    for table_data in pg_read.read_data():
        for batch in table_data:
            print('batch!!!')
            for row in batch:
                print(row)