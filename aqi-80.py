#! /usr/bin/python
# -*- coding: utf-8 -*-
import cookielib, urllib,urllib2
from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError
import  json
import sys
reload(sys)  
sys.setdefaultencoding('utf8')
client = Couchbase.connect(bucket='lastestAQIData',  host='10.10.10.70' ,port=8091,password='111111')
historyClient = Couchbase.connect(bucket='historyAQIData',  host='10.10.10.70' ,port=8091,password='111111')
historyStationClient = Couchbase.connect(bucket='historyStationAQIData',  host='10.10.10.70',port=8091,password='111111')
def aqiJson(url='http://www.pm25.in/api/querys/aqi_ranking.json',urllogin='http://www.pm25.in/api/querys/pm2_5.json?token=5j1znBVAsnSf5xQyNQyq'):
	baiduSpaceEntryUrl = urllogin
	cj = cookielib.CookieJar();
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj));
	urllib2.install_opener(opener);
	resp = urllib2.urlopen(baiduSpaceEntryUrl);
	for index, cookie in enumerate(cj):
		print '[',index, ']',cookie;
	apiUrl = url;
	req = urllib2.Request(apiUrl);
	req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36');
	req.add_header('Content-Type', 'application/x-www-form-urlencoded');
	req.add_header('Cache-Control', 'no-cache');
	req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
	resp = urllib2.urlopen(req);
	respInfo = resp.info();
	return resp.read();


def insertCityJson(data):
	jsonObj = json.loads(data) 
	for x in range(len(jsonObj)):
		key = jsonObj[x]["area"]
		value= jsonObj[x]
		client.set(key,value)	    
		
def insertLastData(data): #user API 1.13
	
	jsonObj = json.loads(data)
	time_point =  jsonObj[0]["time_point"]
	time_point_db=''
	try:
		time_point_db = client.get("last_time_point").value.strip() #if excetpin =>no data in lastestAQI
	except CouchbaseError:
		client.set("last_time_point",time_point) #set time_point
		client.set(time_point,jsonObj) #set lastestAQIData
		insertCityJson(data)           # insert split data to city json
		return 
	#-------------update lastestdata ------------------
	if time_point_db!=time_point:
		print '程序开始更新历史信息---》最新德更新时间 ' ,time_point
		updateSourceData(time_point_db,data) #更新最新库中得源数据
		client.set("last_time_point",time_point) #设置最后更新时间
		updateCityJson(data)#更新最新库中的城市aqi信息
		updateHistoryCityJson(time_point_db,data)#更新历史库中得城市AQI信息 
		updateStationAQIData(data,urllogin='http://www.pm25.in/api/querys/pm2_5.json?token=5j1znBVAsnSf5xQyNQyq',url='http://www.pm25.in/api/querys/aqi_details.json')
	else:
		pass
#更新城市监测点的历史数据,登录获取session，查询一个城市所有监测点的数据
def updateStationAQIData(newData,urllogin,url): #use API 1.7
	print '程序开始更新 【历史库】>【检测点】信息'
	baiduSpaceEntryUrl = urllogin
	cj = cookielib.CookieJar();
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj));
	urllib2.install_opener(opener);
	resp = urllib2.urlopen(baiduSpaceEntryUrl);
	for index, cookie in enumerate(cj):
		print '[',index, ']',cookie;	
		dataObj = json.loads(newData) #解析当前时间点数据
		print '根据最新的城市AQI json 开始解析检测点数据,当前时间',dataObj[0]["time_point"]
		print '当前城市个数：',len(dataObj)
	for x in range(len(dataObj)):
		cityName = ''
		cityName = dataObj[x]["area"].encode("utf8")
		print '根据最新的城市AQI信息开始进行检测点数据抓取 开始查询 第',x,'一个的检测点 【',cityName,'】'
		loginBaiduUrl = url+'?city='+cityName;
		print "-----url-------->"+loginBaiduUrl+"<-----------url----------"
		req = urllib2.Request(loginBaiduUrl);
		req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36');
		req.add_header('Content-Type', 'application/x-www-form-urlencoded;; charset=utf-8');
		req.add_header('Cache-Control', 'no-cache');
		req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
		resp = urllib2.urlopen(req);
		stationAQIDataListStr = resp.read()#返回当前城市所有检测点的数据
		print "------station------>"+stationAQIDataListStr+"<--------station------------"
		#按照 城市_检测点_时间点--》key 保存数据
		stationDataObj = json.loads(stationAQIDataListStr)
		for k in range(len(stationDataObj)):
			if stationDataObj[k]["position_name"] is None:
				print 'station name is null--------- break'
				pass
			else:
				print 'position_name is ===>',stationDataObj[k]['position_name']
				key = stationDataObj[k]["area"]+'_'+stationDataObj[k]["position_name"]+'_'+dataObj[x]["time_point"]
				print 'key is ===>',key
				value = stationDataObj[k]
				historyStationClient.set(key,value)
				print 'position data key:',key,',has save in couchbase'
def updateSourceData(time_point_db,newData):
	client.delete(time_point_db) 
	jsonObj = json.loads(newData)
	time_point =  jsonObj[0]["time_point"]
	client.add(time_point,jsonObj)

# 多文档结构，key：城市-时间点，value：城市当前时间点的数据
def updateHistoryCityJson(old_time_point_key,newData):
	jsonObj = json.loads(newData)
	result  = {}
	for x in range(len(jsonObj)):
		dataList = []
		try:
			dataList = historyClient.get(jsonObj[x]["area"]+'_'+jsonObj[x]["time_point"]).value
		except Exception:
			dataList = [jsonObj[x]]
			firstTimePointDict = {"AQI":dataList}
			historyClient.set(jsonObj[x]["area"]+'_'+jsonObj[x]["time_point"],firstTimePointDict)
			continue
		dictOld = dataList["AQI"]
		dictNew = jsonObj[x]
		dictOld.append(dictNew)
		result["AQI"] = dictOld
		historyClient.set(jsonObj[x]["area"],result)
		
	
def updateCityJson(newData):
	jsonObj = json.loads(newData) 
	for x in range(len(jsonObj)):
		key = jsonObj[x]["area"]
		value= jsonObj[x]
		client.set(key,value)
		
		
if __name__ == '__main__':
	
	#jsonData = '[{"aqi": 19,"area": "三亚","pm2_5": 8,"time_point": "2012-05-26T08:00:00Z"}, {"aqi": 17,"area": "三门峡","pm2_5": 10,"time_point": "2012-05-26T08:00:00Z"}]'
    #jsonData = '[{"aqi": 19,"area": "广州","pm2_5":8,"time_point":"2012-05-28T09:00:00Z"},{"aqi": 17,"area": "北京","pm2_5": 10,"time_point": "2012-05-28T09:00:00Z"},{"aqi": 17,"area": "重庆","pm2_5": 10,"time_point": "2012-05-28T09:00:00Z"},{"aqi": 17,"area": "武汉","pm2_5": 10,"time_point": "2012-05-28T09:00:00Z"}]'

	#jsonData = '{"error": "Sorry，您这个小时内的API请求次数用完了，休息一下吧！"}'
	jsonData = aqiJson()
	if None==jsonData or ''==jsonData:
		print 'json is null'
	elif  ('error' in json.loads(jsonData)):
		print 'error in api'
			
	else:
		insertLastData(jsonData)
		
		
		
