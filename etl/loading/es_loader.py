import logging

import backoff
import elastic_transport
from elasticsearch import Elasticsearch, helpers

from loading.es_index import movies_mappings, movies_settings


class ElasticsearchLoader:

    @backoff.on_exception(backoff.expo, ConnectionError, max_time=300)
    def __init__(self, es_host: str):
        self.index_name = 'movies'
        self.es = Elasticsearch(es_host, verify_certs=False)

    def _create_index_if_not_exists(self):
        if self.es.indices.exists(index=self.index_name):
            return
        logging.info('index does not exist, creating...')
        self.es.indices.create(index=self.index_name, mappings=movies_mappings, settings=movies_settings)
        logging.info('index created')

    @backoff.on_exception(backoff.expo, (ConnectionError, elastic_transport.ConnectionError), max_time=300)
    def load_data(self, data: dict):
        self._create_index_if_not_exists()
        actions = [{'_index': self.index_name, '_id': doc['id'], '_source': doc} for doc in data]
        rows_count, errors = helpers.bulk(self.es, actions=actions)
        if errors:
            logging.error('loading data error:', extra={'actions': actions, 'errors': errors})
