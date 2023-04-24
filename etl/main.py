import logging
from time import sleep

from config import PostgresConfig, MainConfig
from pg_extraction.state import State, JsonFileStorage
from es_loading.es_loader import ElasticsearchLoader
from pg_extraction.pg_extractor import FilmworkReader, PersonReader
from transformer import transform_filmwork


def run_etl(
        pg_reader: FilmworkReader,
        es_loader: ElasticsearchLoader,
        es_index: str,
        transformer: callable = None,
):
    for table_data in pg_reader.read_data():
        for batch in table_data:
            if transformer:
                batch = transformer(batch)
            es_loader.load_data(es_index, batch)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    config = MainConfig()

    storage = JsonFileStorage(config.state_file)
    state = State(storage)

    fw_reader = FilmworkReader(connection_params=PostgresConfig(), state=state, batch_size=config.batch_size)
    person_reader = PersonReader(connection_params=PostgresConfig(), state=state, batch_size=config.batch_size)

    loader = ElasticsearchLoader(config.es_host)

    while True:
        logging.info('filmwork etl started')
        run_etl(pg_reader=fw_reader, es_loader=loader, transformer=transform_filmwork, es_index='movies')
        logging.info('filmwork etl finished')

        logging.info('person etl started')
        run_etl(pg_reader=person_reader, es_loader=loader, es_index='person')
        logging.info('person etl finished')

        sleep(config.etl_run_interval)
