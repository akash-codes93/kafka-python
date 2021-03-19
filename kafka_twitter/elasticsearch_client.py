import re
import json

from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.helpers import streaming_bulk


class ClsElasticSearch:

    def __init__(self, bonsai):
        self.bonsai = bonsai
        self.es = self.get_connection()

    @property
    def es_header(self):

        auth = re.search('https\:\/\/(.*)\@', self.bonsai).group(1).split(':')
        host = self.bonsai.replace('https://%s:%s@' % (auth[0], auth[1]), '')

        # optional port
        match = re.search('(:\d+)', host)
        if match:
            p = match.group(0)
            host = host.replace(p, '')
            port = int(p.split(':')[1])
        else:
            port = 443

        # Connect to cluster over SSL using auth for best security:
        es_header = [{
            'host': host,
            'port': port,
            'use_ssl': True,
            'http_auth': (auth[0], auth[1])
        }]

        return es_header

    def get_connection(self):
        return Elasticsearch(self.es_header)

    def ping(self):
        return self.es.ping()

    def close(self):
        self.es.close()

    def insert(self, index, document):
        try:
            self.es.create(index, document["id"], body=json.dumps({"text": document["text"]}))
            print("Document inserted")
        except ElasticsearchException as e:
            print("Error occurred while inserting, error: ", str(e))

    def bulk_insert(self, index, _generator):
        successes = 0
        try:
            for ok, action in streaming_bulk(
                    client=self.es, index=index, actions=_generator(),
            ):

                successes += ok
        except ElasticsearchException as e:
            print("Error occurred while inserting, error: ", str(e))
        finally:
            print("Indexed %d documents" % successes)


if __name__ == '__main__':
    from config import BONSAI_URL

    elasticSearch = ClsElasticSearch(BONSAI_URL)
    # print(elasticSearch.ping())
    elasticSearch.insert("twitter", {"id": 1100, "text": "Inserted via code"})
    elasticSearch.close()
