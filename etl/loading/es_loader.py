import logging

import backoff
import elastic_transport
from elasticsearch import Elasticsearch, helpers

from loading.es_index import index_mappings, common_index_settings


class ElasticsearchLoader:

    @backoff.on_exception(backoff.expo, ConnectionError, max_time=300)
    def __init__(self, es_host: str):
        self.es = Elasticsearch(es_host, verify_certs=False)

    def _create_index_if_not_exists(self, index_name):
        if self.es.indices.exists(index=index_name):
            return
        logging.info('index does not exist, creating...')
        self.es.indices.create(index=index_name, mappings=index_mappings[index_name], settings=common_index_settings)
        logging.info('index created')

    @backoff.on_exception(backoff.expo, (ConnectionError, elastic_transport.ConnectionError), max_time=300)
    def load_data(self, index_name: str, data: list[dict]):
        self._create_index_if_not_exists(index_name)
        actions = [{'_index': index_name, '_id': doc['id'], '_source': doc} for doc in data]
        rows_count, errors = helpers.bulk(self.es, actions=actions)
        if errors:
            logging.error('loading data error:', extra={'index': index_name, 'actions': actions, 'errors': errors})
