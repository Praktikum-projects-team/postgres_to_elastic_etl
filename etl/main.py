import logging
from time import sleep

from config import PostgresConfig, MainConfig
from extraction.state import State, JsonFileStorage
from loading.es_loader import ElasticsearchLoader
from extraction.extractor import PostgresReader
from transformer import transform


def run_etl(pg_reader: PostgresReader, es_loader: ElasticsearchLoader):
    for table_data in pg_reader.read_data():
        for batch in table_data:
            transformed_data = transform(batch)
            es_loader.load_data(transformed_data)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    config = MainConfig()

    storage = JsonFileStorage(config.state_file)
    state = State(storage)
    reader = PostgresReader(connection_params=PostgresConfig(), state=state, batch_size=config.batch_size)
    loader = ElasticsearchLoader(config.es_host)

    while True:
        logging.info('etl started')
        run_etl(pg_reader=reader, es_loader=loader)
        logging.info('etl finished')
        sleep(config.etl_run_interval)
