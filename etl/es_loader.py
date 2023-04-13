import backoff
from elasticsearch import Elasticsearch


@backoff.on_exception(backoff.expo, Exception, max_time=300)
def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    _es.bulk()