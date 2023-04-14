import logging

import backoff
from elasticsearch import Elasticsearch, helpers


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    _es.bulk()


class ElasticsearchLoader:
    def __init__(self, es_host: str):
        self.es = Elasticsearch(es_host, verify_certs=False)

    @backoff.on_exception(backoff.expo, ConnectionError, max_time=300)
    def load_data(self, data: dict) -> tuple[int, list]:
        query = [{'_index': 'movies', '_id': doc['id'], '_source': doc} for doc in data]
        rows_count, errors = helpers.bulk(self.es, query)
        if errors:
            logging.error('loading data error:', extra={'query': query, 'errors': errors})
        return rows_count, errors