import logging
import os
from contextlib import closing

import backoff
import psycopg2
# from psycopg2.extensions import connection as _connection
from psycopg2.extras import RealDictCursor

from config import PostgresConfig
from state import State, JsonFileStorage


logging.basicConfig(level=logging.INFO)


storage = JsonFileStorage('state_file.json')
state = State(storage)


class PostgresReader:
    def __init__(self, connection_params: PostgresConfig, batch_size: int, state: State):
        self.connection_params = connection_params
        self.batch_size = batch_size
        self.state = state

    def get_last_datetime(self, table):
        return self.state.get_state(key=table)

    def set_last_datetime(self, table, last_datetime):
        self.state.set_state(key=table, value=last_datetime)

    def _read_modified_filmworks(self, cursor):
        logging.info('filmworks reading started')
        statement = '''
            SELECT
            fw.id as id, 
            fw.title, 
            fw.description, 
            fw.rating as rating, 
            COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'role', pfw.role,
                       'id', p.id,
                       'name', p.full_name
                   )
               ) FILTER (WHERE p.id is not null),
               '[]'
            ) as persons,
            array_agg(DISTINCT g.name) as genres,
            fw.modified as modified
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.modified > %s
            GROUP BY fw.id
            ORDER BY fw.modified;
        '''
        cursor.execute(statement, [self.get_last_datetime('filmwork')])
        while data := cursor.fetchmany():
            yield data
            self.set_last_datetime('filmwork', data[-1]['modified'])
        logging.info('filmworks reading finished')

    def _get_persons_ids(self, cursor):
        person_statement = '''
            SELECT id, modified
            FROM content.person
            WHERE modified > %s
            ORDER BY modified
        '''
        cursor.execute(person_statement, [self.get_last_datetime('person')])
        while persons := cursor.fetchmany():
            print(persons)
            yield [person['id'] for person in persons]
            self.set_last_datetime('person', persons[-1]['modified'])

    def _read_modified_person_filmworks(self, cursor):
        logging.info('persons reading started')
        filmworks_statement = '''
                SELECT
                fw.id as id, 
                fw.title, 
                fw.description, 
                fw.rating as rating, 
                COALESCE (
                   json_agg(
                       DISTINCT jsonb_build_object(
                           'role', pfw.role,
                           'id', p.id,
                           'name', p.full_name
                       )
                   ) FILTER (WHERE p.id is not null),
                   '[]'
                ) as persons,
                array_agg(DISTINCT g.name) as genres,
                fw.modified as modified
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                JOIN content.person p1 ON p1.id = pfw.person_id and p1.id = any(%s::uuid[])
                LEFT JOIN content.person p ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                WHERE fw.modified < %s
                GROUP BY fw.id
                ORDER BY fw.modified;
            '''
        for persons_ids in self._get_persons_ids(cursor):
            cursor.execute(filmworks_statement, [persons_ids, self.get_last_datetime('filmwork')])
            while data := cursor.fetchmany():
                print(data)
                yield data

            # while person_ids := self._get_persons_ids(cursor):
            #     yield self._get_filmworks_by_person_ids(cursor, person_ids)

        logging.info('persons reading finished')

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_time=300)
    def read_data(self):
        with closing(psycopg2.connect(**(self.connection_params.dict()), cursor_factory=RealDictCursor)) as connection:
            cursor = connection.cursor()
            cursor.arraysize = self.batch_size
            yield self._read_modified_person_filmworks(cursor)
            yield self._read_modified_filmworks(cursor)




if __name__ == '__main__':
    pg_read = PostgresReader(connection_params=PostgresConfig(), state=state, batch_size=3)
    for table_data in pg_read.read_data():
        for batch in table_data:
            print('batch!!!')
            for row in batch:
                print(row)

