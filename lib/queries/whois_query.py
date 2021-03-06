#!/usr/bin/python
# -*- coding: utf-8 -*-

import pyes

#import IPy
import re
import datetime
import time
import json
import time
import string
from dateutil import parser
import pytz
from pytz import timezone

class WhoisQuery():


    def __init__(self,  es, es_index, es_type):
        self.es = pyes.ES(es)
        self.index = es_index
        self.es_type = es_type
    def checkindex(self, ind):
	print "check %s" % ind
        
        statuses = self.es.indices.status(indices=[ind])
        if len(statuses['indices']) == 0:
            print "index is closed"
            try:
                self.es.indices.open_index(ind)
                time.sleep(20)
            except Exception, e:
                pass
    
                    
    def validate(self, s):
        return filter(lambda x: x in string.ascii_letters +string.digits+' '+'-'+':'+'.'+'*'+ '+' + '[' + ']' + '|' + '@' + '_', s)

    def list_to_str(self,l):
        if len(l) == 0:
            return ""
        return reduce(lambda x, y: "%s\r\n%s"%(x,y), map(self.printable_entry,  l))

    def tm_local(self, d):
        rez = d
        try:
            dt_d = parser.parse(d)
            local_tz = timezone("Asia/Taipei")
            rez = dt_d.astimezone(local_tz).strftime("%Y-%m-%dT%H:%M:%S.000 %z")
        except Exception, e:
            pass
        return rez


    def printable_entry(self,  val):
       v = val['_source']
       
       
       return "%s\r\n%s"%(reduce(lambda x,y: "%s, %s"% (x,y), v.keys()), reduce (lambda x, y: "%s, %s" % (x, self.tm_local(y)), v.values()))
       #return "%s,"% (json.dumps(v))

    def whois_query(self, query):
        start_num = 0
        size_num = 100 # defaults
        query = self.validate(query)
        queries = []
        day = '%.4i%.2i%.2i%.2i'% (datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day, datetime.datetime.now().hour)
        if query.find('@') != -1:
            day = query.split('@', 1)
            query = day[1]
            day = day[0]
        qqq = query.split('+')
        for quer in qqq:
            qq = quer.split(':', 1)
            field = '_all'
            if len(qq) > 1:
                field = qq[0]
                query = qq[1]
		q = None
                if field == "count":
                    size_num = int(query)
                    continue
                elif field == "start":
                    start_num = int(query)
                    continue
		elif query.find('[') != -1:
			from_d = query[1:query.find('|')]
			to_d = query[query.find('|') + 1:query.find(']')]
	
                        try:                                                                         
                            dt_from_d = parser.parse(from_d)
                            dt_to_d = parser.parse(to_d)
                            local_tz = timezone("Asia/Taipei")
                            local_from_d = local_tz.localize(dt_from_d)
                            local_to_d = local_tz.localize(dt_to_d)
                            from_d = local_from_d.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                            to_d = local_to_d.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                        except Exception, e:
                            print e
                            pass

			q = pyes.query.RangeQuery(qrange=pyes.query.ESRange(field, to_value=to_d,  from_value=from_d))
		else:	
			q = pyes.query.TextQuery(field, query)
                queries.append(q)
        to_return = ""
        try:         
            self.checkindex("%s-%s"% (self.index, day))

            bq = pyes.query.BoolQuery(must=queries)
            rez = self.es.search(pyes.query.Search(query=bq, start=start_num, size=size_num), indices="%s-%s"% (self.index, day), doc_types=self.es_type)
            to_return = "Result, Total %s matches\r\n%s" % (rez.count(), self.list_to_str(rez._results['hits']['hits']))
        except Exception, e:
            to_return = "Result, Error: %s\r\n" % str(e)


        return to_return



if __name__ == "__main__":
    import os
    import sys
    import ConfigParser
    config = ConfigParser.RawConfigParser()
    config.read("../../etc/whois-server.conf")
    query_maker = WhoisQuery('localhost:9200', 'searchindex', 'searchindex-type')

    def usage():
        print "arin_query.py query"
        exit(1)

    if len(sys.argv) < 2:
        usage()

    query = sys.argv[1]
    print(query_maker.whois_query(query))
