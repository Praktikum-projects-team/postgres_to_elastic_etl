from dotenv import load_dotenv
from pydantic import (
    BaseModel,
    BaseSettings,
    PyObject,
    RedisDsn,
    PostgresDsn,
    AmqpDsn,
    Field,
)

load_dotenv()


class PostgresConfig(BaseSettings):
    host: str = Field(..., env='DB_HOST')
    user: str = Field(..., env='DB_USER')
    password: str = Field(..., env='DB_PASSWORD')
    database: str = Field(..., env='DB_NAME')
    port: int = Field(..., env='DB_PORT')


# class Settings(BaseSettings):
    # auth_key: str
    # api_key: str = Field(..., env='my_api_key')

    # redis_dsn: RedisDsn = 'redis://user:pass@localhost:6379/1'
    # pg_dsn: PostgresDsn = 'postgres://user:pass@localhost:5432/foobar'
    # amqp_dsn: AmqpDsn = 'amqp://user:pass@localhost:5672/'

    # special_function: PyObject = 'math.cos'

    # # to override domains:
    # # export my_prefix_domains='["foo.com", "bar.com"]'
    # domains: set[str] = set()
    #
    # # to override more_settings:
    # # export my_prefix_more_settings='{"foo": "x", "apple": 1}'
    # more_settings: SubModel = SubModel()

    # class Config:
    #     env_prefix = 'my_prefix_'  # defaults to no prefix, i.e. ""
    #     fields = {
    #         'auth_key': {
    #             'env': 'my_auth_key',
    #         },
    #         'redis_dsn': {
    #             'env': ['service_redis_dsn', 'redis_url']
    #         }
    #     }




#
# import logging
# import sqlite3
# from contextlib import closing
# from dataclasses import fields, astuple
#
# import psycopg2
# from psycopg2.extensions import connection as _connection
# from psycopg2.extras import DictCursor
#
# from sqlite_to_postgres.config import CHUNK_SIZE, sqlite_db_path, pg_dsl
# from sqlite_to_postgres.sqlite_conn_context import conn_context
# from sqlite_to_postgres.table_dataclasses.dataclass_builder import DataclassBuilder
# from sqlite_to_postgres.table_dataclasses.tables import TABLE_MODELS
#
# logging.basicConfig(level=logging.INFO)

#
#
# def write_to_postgresql(connection: _connection, table: str, model_type: type, data: list):
#     cursor = connection.cursor()
#     if not data:
#         return
#     field_list = [f.name for f in fields(model_type)]
#     args = [astuple(row) for row in data]
#     args_template = '(' + ', '.join(['%s']*len(args[0])) + ')'
#     args = ','.join(cursor.mogrify(args_template, item).decode() for item in args)
#
#     insert_statement = f'''
#         insert into content.{table}({','.join(field_list)})
#         values {args}
#         on conflict(id) do nothing;
#         '''
#     cursor.execute(insert_statement)
#
#
#
# if __name__ == '__main__':
#     with conn_context(sqlite_db_path) as sqlite_conn, closing(psycopg2.connect(**pg_dsl, cursor_factory=DictCursor)) as pg_conn:
#         load_from_sqlite(sqlite_conn=sqlite_conn, pg_conn=pg_conn)

