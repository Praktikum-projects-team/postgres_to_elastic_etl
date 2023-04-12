import logging
import os
from contextlib import closing

import backoff
import psycopg2
# from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from config import PostgresConfig
from main import state

# logging.basicConfig(level=logging.INFO)


class PostgresReader:
    def __init__(self, connection_params: PostgresConfig, batch_size, state):
        self.connection_params = connection_params
        self.batch_size = batch_size
        self.state = state

    def get_last_datetime(self):
        return self.state.get_last_state()

    def set_last_datetime(self, last_datetime):
        self.state.set_state('modified', last_datetime)

    @staticmethod
    def get_sql_statement():
        statement = '''
            SELECT
            fw.id as fw_id, 
            fw.title, 
            fw.description, 
            fw.rating, 
            g.name as genre,
            pfw.role person_role, 
            p.id person_id, 
            p.full_name person_name,
            GREATEST(fw.modified, p.modified, g.modified) as modified
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE GREATEST(fw.modified, p.modified, g.modified) > %s
            ORDER BY modified;
        '''
        return statement

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_time=300)
    def read_data(self):

        with closing(psycopg2.connect(**(self.connection_params.dict()), cursor_factory=DictCursor)) as connection:
            cursor = connection.cursor()
            print(self.get_last_datetime())
            cursor.execute(self.get_sql_statement(), [self.get_last_datetime()])
            while data := cursor.fetchmany(self.batch_size):
                yield data
                self.set_last_datetime(data[-1]['modified'])


pg_read = PostgresReader(connection_params=PostgresConfig(), state=state, batch_size=100)
for batch in pg_read.read_data():
    print('batch!!!')
    for row in batch:
        print(dict(row))

