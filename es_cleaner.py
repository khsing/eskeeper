#!/usr/bin/env python

import sys
import json
import httplib

class ElasticSearch(object):
    def __init__(self, host="localhost:9200"):
        super(ElasticSearch, self).__init__()
        if ':' in host:
            host, port = host.split(':')
        else:
            port = 9200
        self.host = host
        self.port = int(port)
    
    def query(self, method="GET",url="/",payload=None):
        conn = httplib.HTTPConnection(self.host,self.port)
        conn.request(method,url,payload)
        resp = conn.getresponse()
        return json.load(resp)
    
    def status(self):
        return self.query(url="/_status")
    
    def get_indices(self):
        return self.status()['indices']
    
    def delete_indices(self, indice):
        ret = self.query(method='DELETE', url='/%s'%indice)
        if 'ok' in ret:
            return ret['ok']
        return False
    

def delete_old_logstash_indices(es, days=20):
    indices = es.get_indices()
    logstash_indices_keys = [i for i in indices.keys() if i.startswith('logstash-')]
    logstash_indices_keys.sort()
    for i in logstash_indices_keys[:-days]:
        print 'cleaning indices', i, es.delete_indices(i)
    
def main():
    es = ElasticSearch(host=sys.argv[1])
    delete_old_logstash_indices(es)
    
if __name__ == '__main__':
    main()
    
    