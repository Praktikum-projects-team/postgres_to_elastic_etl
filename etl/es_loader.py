import logging

import backoff
from elasticsearch import Elasticsearch, helpers

from es_index import movies_mappings, movies_settings


class ElasticsearchLoader:
    def __init__(self, es_host: str):
        self.index_name = 'movies'
        self.es = Elasticsearch(es_host, verify_certs=False)

    def create_index_if_not_exists(self):
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, mappings=movies_mappings, settings=movies_settings)

    @backoff.on_exception(backoff.expo, ConnectionError, max_time=300)
    def load_data(self, data: dict):
        actions = [{'_index': self.index_name, '_id': doc['id'], '_source': doc} for doc in data]
        rows_count, errors = helpers.bulk(self.es, actions=actions)
        if errors:
            logging.error('loading data error:', extra={'actions': actions, 'errors': errors})

