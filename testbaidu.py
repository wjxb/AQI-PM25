#! /usr/bin/python
# -*- coding: utf-8 -*-
import cookielib,urllib2
from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError
from couchbase.views.params import Query
import  json

baiduUrlAddressInCity ='http://api.map.baidu.com/geocoder/v2/?ak=zMCmT2jFBggL0fBT3MVIwSkj&ip=&output=json'

def getcoordinate(baiduUrl,address,City):
    url = baiduUrl+'&address='+address+'&City='+City
    req = urllib2.Request(url);
    req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.')
    req.add_header('Content-Type', 'application/x-www-form-urlencoded');
    req.add_header('Cache-Control', 'no-cache');
    req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
    resp = urllib2.urlopen(req);
    respInfo = resp.read();
    # obj = json.loads(respInfo)
    # print respInfo
    # print obj['result']['location']['lng']
    return respInfo

coordinate = getcoordinate(baiduUrlAddressInCity,'滨州市','')
coordinateObj = json.loads(coordinate)
print coordinateObj['result']['location']['lat']
print coordinateObj['result']['location']['lng']
