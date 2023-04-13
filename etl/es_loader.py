import logging

import backoff
from elasticsearch import Elasticsearch, helpers


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    _es.bulk()


class ElasticsearchLoader:
    def __init__(self, es_host: str, es_user: str, es_password: str):
        self.es = Elasticsearch(es_host, basic_auth=(es_user, es_password), verify_certs=False)

    @staticmethod
    @backoff.on_exception(backoff.expo, ConnectionError, max_time=300)
    def load_data(es: Elasticsearch, data: dict) -> tuple[int, list]:
        query = [{'_index': 'movies', '_id': doc.id, '_source': doc} for doc in data]
        rows_count, errors = helpers.bulk(es, query)
        if errors:
            logging.error('loading data error:', extra={'query': query, 'errors': errors})
        return rows_count, errors