#!/usr/bin/python
# -*- coding: utf-8 -*-

import pyes

import IPy
import re
import datetime
import time
import json
import string

class WhoisQuery():


    def __init__(self,  es, es_index, es_type):
        self.es = pyes.ES(es)
        self.index = es_index
        self.es_type = es_type

    def validate(self, s):
        return filter(lambda x: x in string.ascii_letters +string.digits+':'+'.'+'*'+ '+', s)

    def list_to_str(self,l):
        if len(l) == 0:
            return ""
        return reduce(lambda x, y: "%s\r\n\r\n%s"%(x,y), map(self.printable_entry,  l))

    def printable_entry(self,  val):
       v = val['_source']
       return "%s,"% (json.dumps(v))

    def whois_query(self, query):
        query = self.validate(query)
        queries = []
        qqq = query.split('+')
        for quer in qqq:
            qq = quer.split(':')
            field = '_all'
            if len(qq) > 1:
                field = qq[0]
                query = qq[1]
                q = pyes.query.TextQuery(field, query)
                queries.append(q)
        bq = pyes.query.BoolQuery(must=queries)
        rez = self.es.search(query=bq, indices=self.index, doc_types=self.es_type)
        to_return = "Total %s matches\r\n%s" % (rez.count(), self.list_to_str(rez._results['hits']['hits']))


        return to_return



if __name__ == "__main__":
    import os
    import IPy
    import sys
    import ConfigParser
    config = ConfigParser.RawConfigParser()
    config.read("../../etc/whois-server.conf")
    query_maker = WhoisQuery(int(config.get('whois_server','redis_db')), config.get('whois_server','prepend_to_keys'))

    def usage():
        print "arin_query.py query"
        exit(1)

    if len(sys.argv) < 2:
        usage()

    query = sys.argv[1]
    ip = None
    try:
        ip = IPy.IP(query)
    except:
        pass


    if ip:
        print(query_maker.whois_ip(ip))
    else:
       print(query_maker.whois_asn(query))
