import logging

import backoff
from elasticsearch import Elasticsearch, helpers


class ElasticsearchLoader:
    def __init__(self, es_host: str):
        self.es = Elasticsearch(es_host, verify_certs=False)

    @backoff.on_exception(backoff.expo, ConnectionError, max_time=300)
    def load_data(self, data: dict):
        actions = [{'_index': 'movies', '_id': doc['id'], '_source': doc} for doc in data]
        rows_count, errors = helpers.bulk(self.es, actions=actions)
        if errors:
            logging.error('loading data error:', extra={'actions': actions, 'errors': errors})

