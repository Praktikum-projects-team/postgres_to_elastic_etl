from config import PostgresConfig
from es_loader import ElasticsearchLoader
from extractor import PostgresReader, state
from transformer import transform
import os


def run_etl(pg_reader: PostgresReader, es_loader: ElasticsearchLoader):
    for table_data in pg_reader.read_data():
        for batch in table_data:
            transformed_data = transform(batch)
            es_loader.load_data(transformed_data)


if __name__ == '__main__':
    pg_reader = PostgresReader(connection_params=PostgresConfig(), state=state, batch_size=3)
    es_loader = ElasticsearchLoader(os.environ.get('ES_HOST'))
    while True:
        run_etl()
        sleep()