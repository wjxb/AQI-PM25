#! /usr/bin/python
# -*- coding: utf-8 -*-
import cookielib,urllib2
from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError
from couchbase.views.params import Query
import  json
import sys
import traceback
import logging
import datetime
import random
from bitarray import bitarray
import mmh3
reload(sys)
sys.setdefaultencoding('utf8')
host='192.168.100.200'
port=8091
pwd='111111'
client = Couchbase.connect(bucket='lastestAQIData',  host=host ,port=port,password=pwd)
userProductclient = Couchbase.connect(bucket='aicc-UserProductDB',host=host,port=port,password=pwd)
historyClient = Couchbase.connect(bucket='historyAQIData',  host=host ,port=port,password=pwd)
historyStationClient = Couchbase.connect(bucket='historyStationAQIData',  host=host,port=port,password=pwd)
memclient = Couchbase.connect(bucket='aicc-MemcachedDB',  host=host,port=port,password=pwd)
baiduUrlAddressInCity ='http://api.map.baidu.com/geocoder/v2/?ak=zMCmT2jFBggL0fBT3MVIwSkj&ip=&output=json'
geoCountUrl='http://11.11.11.201:8092/aicc-UserProductDB/_design/spatial/_spatial/deviceCount?bbox='

citiesGeoFile = '/home/wntime/AQI/china_cities_geo.json'
aqiGeoFile= '/var/www/data/aqiGeo.js'
remotePath=''
remotePath='' 
ipPool=['125.39.66.67','218.78.210.190','114.112.91.97','114.215.108.155']
proxy_ip=ipPool[random.randint(0,3)]

class BloomFilter:

    def __init__(self, size, hash_count):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = bitarray(size)
        self.bit_array.setall(0)

    def add(self, string):
        for seed in xrange(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            self.bit_array[result] = 1

    def lookup(self, string):
        for seed in xrange(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            if self.bit_array[result] == 0:
                return False
        return True




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
	req.add_header('User-Agent', 'Mozilla/4.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36');
	req.add_header('Content-Type', 'application/x-www-form-urlencoded');
	req.add_header('Cache-Control', 'no-cache');
	req.add_header('X-Forwarded-For', ipPool[random.randint(0,3)]);
	req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
	resp = urllib2.urlopen(req)
	return resp.read()


def cutAqiJson(data):
	finallResult = []
	bf = BloomFilter(500000, 7)
	jsonObj = json.loads(data)
	stationGeo={}
	stationGeo=client.get("cityAndStation").value
	for cityGeoObj in stationGeo["cityAndStation"]:
		city=cityGeoObj["city"]
		bf.add(city)

	for x in range(len(jsonObj)): 
		key = jsonObj[x]["area"] 
		if bf.lookup(key)==True:
			finallResult.append(jsonObj[x])
	return json.dumps(finallResult, encoding='UTF-8', ensure_ascii=False)


def insertCityJson(data):
	url='http://www.pm25.in/api/querys/aqi_details.json' #查询城市所有监测点数据
	urllogin='http://www.pm25.in/api/querys/pm2_5.json?token=5j1znBVAsnSf5xQyNQyq'#登录
	stationGeo={}
	stationGeo=client.get("cityAndStation").value
	baiduSpaceEntryUrl = urllogin
	cj = cookielib.CookieJar();
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj));
	urllib2.install_opener(opener);
	resp = urllib2.urlopen(baiduSpaceEntryUrl);

	jsonObj = json.loads(data) 
	for x in range(len(jsonObj)): 
		key = jsonObj[x]["area"] 
		coordinate = getcoordinate(baiduUrlAddressInCity,key,'')
		if(coordinate==''):
				value= jsonObj[x]
				client.set(key,value)
				continue
		try :
	            coordinateObj = json.loads(coordinate)
	            if(coordinateObj['status']==1):
						value= jsonObj[x]
						client.set(key,value)
						continue
		except Exception,e:
				value= jsonObj[x]
				client.set(key,value)
				continue
		jsonObj[x]['latitude']=coordinateObj['result']['location']['lat']
		jsonObj[x]['longitude']=coordinateObj['result']['location']['lng']
		cityName = key 
		stationUrl = url+'?city='+cityName;
		req = urllib2.Request(stationUrl);
		req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36');
		req.add_header('Content-Type', 'application/x-www-form-urlencoded;; charset=utf-8');
		req.add_header('Cache-Control', 'no-cache');
		req.add_header('X-Forwarded-For', ipPool[random.randint(0,3)]);
		req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
		resp = urllib2.urlopen(req);
		stationAQIDataListStr = resp.read()
		stationDataObj = json.loads(stationAQIDataListStr)
		stationArray=[]
		for k in range(len(stationDataObj)):
			try:
				position_name = stationDataObj[k]["position_name"]
			except :
				break
			if stationDataObj[k]["position_name"] is None:
				pass
			else: 
				  
				for cityGeoObj in stationGeo["cityAndStation"]:
					for stationGeoObj in cityGeoObj["stations"]:
						if stationGeoObj["station_name"]==stationDataObj[k]["position_name"]:
							stationDataObj[k]["latitude"]=stationGeoObj["latitude"]
							stationDataObj[k]["longitude"]=stationGeoObj["longitude"]
				stationArray.append(stationDataObj[k]) #
		jsonObj[x]["stationList"] = stationArray 
		value= jsonObj[x]
		client.set(key,value) 

def updateCityJson(newData):
   	url='http://www.pm25.in/api/querys/aqi_details.json' #查询城市所有监测点数据
	urllogin='http://www.pm25.in/api/querys/pm2_5.json?token=5j1znBVAsnSf5xQyNQyq'#登录
	stationGeo={}
	stationGeo=client.get("cityAndStation").value
	baiduSpaceEntryUrl = urllogin
	cj = cookielib.CookieJar();
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj));
	urllib2.install_opener(opener);
	resp = urllib2.urlopen(baiduSpaceEntryUrl);
	jsonObj = json.loads(newData)
	for x in range(len(jsonObj)):
		key = jsonObj[x]["area"]
		cityName = key 
		stationUrl = url+'?city='+cityName;
		req = urllib2.Request(stationUrl);
		req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36');
		req.add_header('Content-Type', 'application/x-www-form-urlencoded;; charset=utf-8');
		req.add_header('Cache-Control', 'no-cache');
		req.add_header('X-Forwarded-For', ipPool[random.randint(0,3)]);
		req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
		resp = urllib2.urlopen(req);
		stationAQIDataListStr = resp.read()
		stationDataObj = json.loads(stationAQIDataListStr)
		stationArray=[]
		for k in range(len(stationDataObj)):
			try:
				position_name = stationDataObj[k]["position_name"]
			except :
				break
			if stationDataObj[k]["position_name"] is None:
				pass
			else: 
				  
				for cityGeoObj in stationGeo["cityAndStation"]:
					for stationGeoObj in cityGeoObj["stations"]:
						if stationGeoObj["station_name"]==stationDataObj[k]["position_name"]:
							stationDataObj[k]["latitude"]=stationGeoObj["latitude"]
							stationDataObj[k]["longitude"]=stationGeoObj["longitude"]
							stationArray.append(stationDataObj[k]) 
		jsonObj[x]["stationList"] = stationArray 
		value= jsonObj[x]
		client.set(key,value) 

def getcoordinate(baiduUrl,address,City):
    respInfo=''
    url = baiduUrl+'&address='+address+'&City='+City
    req = urllib2.Request(url);
    req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.')
    req.add_header('Content-Type', 'application/x-www-form-urlencoded');
    req.add_header('Cache-Control', 'no-cache');
    req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
    try :
    	resp = urllib2.urlopen(req);
	respInfo = resp.read();
    except :
	s=traceback.format_exc()
        print datetime.datetime.now(),s
	return ''
    # obj = json.loads(respInfo)
    # print respInfo
    # print obj['result']['location']['lng']
    return respInfo


def insertLastData(data): #user API 1.13
	jsonObj = json.loads(data)
	time_point =  jsonObj[0]["time_point"]
	time_point_db=''
	try:
		time_point_db = client.get("last_time_point").value.strip() #if excetpin =>no data in lastestAQI
	except CouchbaseError:
		client.set("last_time_point",time_point) #set time_point
		for x in range(len(jsonObj)):
			key = jsonObj[x]["area"]
			coordinate = getcoordinate(baiduUrlAddressInCity,key,'')
			if(coordinate==''):
				continue
			try :
				coordinateObj = json.loads(coordinate)
			except Exception,e:
				continue
			if(coordinateObj['status']==1):
				continue
			jsonObj[x]['latitude']=coordinateObj['result']['location']['lat']
			jsonObj[x]['longitude']=coordinateObj['result']['location']['lng']
		client.set(time_point,jsonObj)           
		insertCityJson(data)                     
		return
	#-------------update lastestdata ------------------
	if time_point_db!=time_point:
		client.set("last_time_point",time_point)
		updateSourceData(time_point_db,data)             
		updateCityJson(data)                             
		updateHistoryCityJson(time_point_db,data)        
		updateStationAQIData(data,urllogin='http://www.pm25.in/api/querys/pm2_5.json?token=5j1znBVAsnSf5xQyNQyq',url='http://www.pm25.in/api/querys/aqi_details.json')
	else:
		pass

def updateStationAQIData(newData,urllogin,url): #use API 1.7
	baiduSpaceEntryUrl = urllogin
	cj = cookielib.CookieJar();
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj));
	urllib2.install_opener(opener);
	resp = urllib2.urlopen(baiduSpaceEntryUrl);
	for index, cookie in enumerate(cj):
		print '[',index, ']',cookie;
		dataObj = json.loads(newData) 
		
	for x in range(len(dataObj)): 
		cityName = ''
		cityName = dataObj[x]["area"].encode("utf8")
		loginBaiduUrl = url+'?city='+cityName;
		req = urllib2.Request(loginBaiduUrl);
		req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36');
		req.add_header('Content-Type', 'application/x-www-form-urlencoded;; charset=utf-8');
		req.add_header('Cache-Control', 'no-cache');
		req.add_header('X-Forwarded-For', ipPool[random.randint(0,3)]);
		req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
		resp = urllib2.urlopen(req);
		stationAQIDataListStr = resp.read()
		stationDataObj = json.loads(stationAQIDataListStr)
		for k in range(len(stationDataObj)):
			try:
				a = stationDataObj[k]["position_name"]
			except :
				break
			if stationDataObj[k]["position_name"] is None:
				pass
			else:
				key = stationDataObj[k]["area"]+'_'+stationDataObj[k]["position_name"]+'_'+dataObj[x]["time_point"]
				value = stationDataObj[k]
				historyStationClient.set(key,value)


def updateSourceData(time_point_db,newData):
	jsonObj = json.loads(newData)
	time_point =  jsonObj[0]["time_point"]
	for x in range(len(jsonObj)):
		key = jsonObj[x]["area"]
		coordinate = getcoordinate(baiduUrlAddressInCity,key,'')
		if(coordinate==''):
              		continue
                try :
                	coordinateObj = json.loads(coordinate)
               	except Exception,e:
                       	continue
               	if(coordinateObj['status']==1):
                        continue
		jsonObj[x]['latitude']=coordinateObj['result']['location']['lat']
		jsonObj[x]['longitude']=coordinateObj['result']['location']['lng']
	try:

		client.delete(time_point_db)
		client.set(time_point,jsonObj)
	except :
		client.delete('time_point_db')



def updateGeoJsonWithAQI(data):
	geoAqiJsonDict = {"type": "FeatureCollection"}
	aqiJson = json.loads(data)
	geoAqiJsonArray= []
	if (True):
		f = file(citiesGeoFile)
		localGeoJson = json.load(f, encoding="utf-8")
		f.close()
		len(localGeoJson['features'])
		for x in range(len(aqiJson)):
			for y in range(len(localGeoJson['features'])):
				if(aqiJson[x]['area'] in localGeoJson['features'][y]['properties']['name']):
					print '在geo文件中发现 aqi信息',localGeoJson['features'][y]['properties']['name']
					localGeoJson['features'][y]['properties']['aqi']=aqiJson[x]["aqi"]
					geoAqiJsonArray.append(localGeoJson['features'][y])
					break
				else:
					continue
		geoAqiJsonDict['features']=geoAqiJsonArray
		# print len(geoAqiJsonDict['features'])
		# print geoAqiJsonDict
		#生成js 文件
		f = open(aqiGeoFile,'w')
		f.write("var statesData ="+json.dumps(geoAqiJsonDict))
		# print json.dumps(geoAqiJsonDict, encoding='UTF-8', ensure_ascii=False)
		f.close()

def getDeviceCountByGeo(long,lat):
    pass


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
if __name__ == '__main__':

	
	jsonData = aqiJson()
	
	if None==jsonData or ''==jsonData:
		print 'json is null'
	elif  ('error' in json.loads(jsonData)):
		print 'error in api'
	else:
		try :
			insertLastData(jsonData)
		except :
			s=traceback.format_exc()
			print datetime.datetime.now(),s
        		logging.error(s)


