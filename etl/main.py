from config import PostgresConfig
from extractor import PostgresReader, state
from transformer import transform


def run_etl():
    pg_read = PostgresReader(connection_params=PostgresConfig(), state=state, batch_size=3)
    for table_data in pg_read.read_data():
        for batch in table_data:
            print('batch!!!')
            transformed_data = transform(batch)
            print(transformed_data)

run_etl()