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
host='10.10.10.80'
port=8091
pwd='111111'
client = Couchbase.connect(bucket='lastestAQIData',  host=host ,port=port,password=pwd)
userProductclient = Couchbase.connect(bucket='aicc-UserProductDB',host=host,port=port,password=pwd)
historyClient = Couchbase.connect(bucket='historyAQIData',  host=host ,port=port,password=pwd)
historyStationClient = Couchbase.connect(bucket='historyStationAQIData',  host=host,port=port,password=pwd)
memclient = Couchbase.connect(bucket='aicc-MemcachedDB',  host=host,port=port,password=pwd)
baiduUrlAddressInCity ='http://api.map.baidu.com/geocoder/v2/?ak=zMCmT2jFBggL0fBT3MVIwSkj&ip=&output=json'
geoCountUrl='http://11.11.11.201:8092/aicc-UserProductDB/_design/spatial/_spatial/deviceCount?bbox='
# citiesGeoFile = 'geo-test.json'
citiesGeoFile = '/home/wntime/AQI/china_cities_geo.json'
aqiGeoFile= '/var/www/data/aqiGeo.js'
remotePath='' #web服务器远程目录将生成的json文件拷贝到远程目录
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

# 访问pm25.in 网站抓取数据
# 二期bug: 2015年城市数据总数368 有经纬度的城市和检测站190个，由于接口1.7 每小时显示500次，
# 因此会浪费接口调用次数，所以在第一次获取城市aqi时，要将368个城市缩减成190个城市。

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
	#respInfo = resp.info()
	return resp.read()
'''
	将368个城市数据缩减到190 节省接口调用次数
'''
def cutAqiJson(data):
	finallResult = []
	bf = BloomFilter(500000, 7)
	jsonObj = json.loads(data)
	stationGeo={}
	stationGeo=client.get("cityAndStation").value
	for cityGeoObj in stationGeo["cityAndStation"]:
		city=cityGeoObj["city"]
		bf.add(city)

	for x in range(len(jsonObj)): #循环368个城市
		key = jsonObj[x]["area"] #城市名称
		if bf.lookup(key)==True:
			finallResult.append(jsonObj[x])
	print "经过裁剪之后的城市个数【",len(finallResult),"】"
	return json.dumps(finallResult, encoding='UTF-8', ensure_ascii=False)

''' 初始化城市 aqi 信息 在城市 实体中 加入 经纬度信息
    app 二期需求，加入城市监测点的实时数据，同时补充监测点经纬度
    参数：data,190个城市的aqi数组。
'''
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

	jsonObj = json.loads(data) #190个城市的json数组
	for x in range(len(jsonObj)): #循环190个城市
		key = jsonObj[x]["area"] #城市名称
		print"保存城市实体数据时城市名称",key
		coordinate = getcoordinate(baiduUrlAddressInCity,key,'')
		if(coordinate==''):
				print '城市【',key,'】没有查询到经纬度 请注意检查 aqi信息是否正确保存'
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
		cityName = key #当前城市名称
		print '【保存城市单独aqi信息方法】开始查询【',cityName,'】的监测点AQI'
		stationUrl = url+'?city='+cityName;
		# print "-----url-------->"+loginBaiduUrl+"<-----------url----------"
		req = urllib2.Request(stationUrl);
		req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36');
		req.add_header('Content-Type', 'application/x-www-form-urlencoded;; charset=utf-8');
		req.add_header('Cache-Control', 'no-cache');
		req.add_header('X-Forwarded-For', ipPool[random.randint(0,3)]);
		req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
		resp = urllib2.urlopen(req);
		stationAQIDataListStr = resp.read()#返回当前城市所有检测点的数据
		# print "------station------>"+stationAQIDataListStr+"<--------station------------"
		#按照 城市_检测点_时间点--》key 保存数据
		stationDataObj = json.loads(stationAQIDataListStr)
		# print "城市【",cityName,"】检测站数据",stationDataObj
		stationArray=[]
		for k in range(len(stationDataObj)):
			try:
				position_name = stationDataObj[k]["position_name"]
				print '准备保存【',cityName,'】中的【',position_name,'】数据信息'
			except :
				print '查询城市【',cityName,'】的检测点时发生异常，继续查询下一个城市'
				break
			if stationDataObj[k]["position_name"] is None:
				print 'station name is null--------- break'
				pass
			else: #1:查询监测点经纬度，
				  #2：保存监测点数组
				for cityGeoObj in stationGeo["cityAndStation"]:
					for stationGeoObj in cityGeoObj["stations"]:
						if stationGeoObj["station_name"]==stationDataObj[k]["position_name"]:
							stationDataObj[k]["latitude"]=stationGeoObj["latitude"]
							stationDataObj[k]["longitude"]=stationGeoObj["longitude"]
				stationArray.append(stationDataObj[k]) #数组中加入单独监测点数据
		jsonObj[x]["stationList"] = stationArray #城市实体中加入监测点数组
		value= jsonObj[x]
		client.set(key,value) #保存单独城市实体信息
'''
更新城市 aqi 信息
app 二期需求 在【城市名】为key的数据中，加入检测站的实时数据
'''
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
	# print jsonObj
	for x in range(len(jsonObj)):
		key = jsonObj[x]["area"]
		cityName = key #当前城市名称
		stationUrl = url+'?city='+cityName;
		# print "-----url-------->"+stationUrl+"<-----------url----------"
		req = urllib2.Request(stationUrl);
		req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36');
		req.add_header('Content-Type', 'application/x-www-form-urlencoded;; charset=utf-8');
		req.add_header('Cache-Control', 'no-cache');
		req.add_header('X-Forwarded-For', ipPool[random.randint(0,3)]);
		req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
		resp = urllib2.urlopen(req);
		stationAQIDataListStr = resp.read()#返回当前城市所有检测点的数据
		# print "------station------>"+stationAQIDataListStr+"<--------station------------"
		#按照 城市_检测点_时间点--》key 保存数据
		stationDataObj = json.loads(stationAQIDataListStr)
		print "城市【",cityName,"】检测站数据",stationDataObj
		stationArray=[]
		for k in range(len(stationDataObj)):
			try:
				position_name = stationDataObj[k]["position_name"]
				print '准备更新【',cityName,'】中的【',position_name,'】数据信息'
			except :
				print '查询城市【',cityName,'】的检测点时发生异常，继续查询下一个城市'
				break
			if stationDataObj[k]["position_name"] is None:
				# print 'station name is null--------- break'
				pass
			else: #1:查询监测点经纬度，
				  #2：保存监测点数组
				for cityGeoObj in stationGeo["cityAndStation"]:
					for stationGeoObj in cityGeoObj["stations"]:
						if stationGeoObj["station_name"]==stationDataObj[k]["position_name"]:
							stationDataObj[k]["latitude"]=stationGeoObj["latitude"]
							stationDataObj[k]["longitude"]=stationGeoObj["longitude"]
							stationArray.append(stationDataObj[k]) #数组中加入单独监测点数据
		jsonObj[x]["stationList"] = stationArray #城市实体中加入监测点数组
		value= jsonObj[x]
		client.set(key,value) #保存单独城市实体信息
		print  '[',key,']和它所有检测站的实时数据已经保存'
# 访问百度地图api 地址必选，城市（可选，用于过滤地址） 查询经纬度信息
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

#TODO 对于aqi 数据时更新还是插入 判断方式是根据最后更新时间来的。因此无法判断出城市个数的改变。
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
				print '没有查询到当前城市【',key ,'】的经纬度信息'
				continue
			jsonObj[x]['latitude']=coordinateObj['result']['location']['lat']
			jsonObj[x]['longitude']=coordinateObj['result']['location']['lng']
		client.set(time_point,jsonObj)           #set lastestAQIData 这个json中要包含经纬度
		#updateGeoJsonWithAQI(data)
		insertCityJson(data)                     # insert split data to city json
		return
	#-------------update lastestdata ------------------
	if time_point_db!=time_point:
		print '程序开始更新历史信息---》更新时间 ' ,time_point
		client.set("last_time_point",time_point)
		updateSourceData(time_point_db,data)             #更新最新库中的源数据 lastestAQIData->以时间为key的数据
		#updateGeoJsonWithAQI(data)
		updateCityJson(data)                             #更新最新库中的城市aqi信息 lastestAQIData-》以城市名为key的数据
		updateHistoryCityJson(time_point_db,data)        #更新历史库中得城市AQI信息 historyAQIData-》以城市_时间为key的数据
		updateStationAQIData(data,urllogin='http://www.pm25.in/api/querys/pm2_5.json?token=5j1znBVAsnSf5xQyNQyq',url='http://www.pm25.in/api/querys/aqi_details.json')
	else:
		print '没有最新的aqi数据，本次操作不更新数据'
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
		# print '根据最新的城市AQI json 开始解析检测点数据,当前时间',dataObj[0]["time_point"]
		print '当前城市个数：',len(dataObj)
	for x in range(len(dataObj)): #循环190个城市
		cityName = ''
		cityName = dataObj[x]["area"].encode("utf8")
		print '根据最新的城市AQI信息开始进行检测点数据抓取 开始查询 第',x,'个检测点 所属城市',cityName,'】'
		loginBaiduUrl = url+'?city='+cityName;
		# print "-----url-------->"+loginBaiduUrl+"<-----------url----------"
		req = urllib2.Request(loginBaiduUrl);
		req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36');
		req.add_header('Content-Type', 'application/x-www-form-urlencoded;; charset=utf-8');
		req.add_header('Cache-Control', 'no-cache');
		req.add_header('X-Forwarded-For', ipPool[random.randint(0,3)]);
		req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8');
		resp = urllib2.urlopen(req);
		stationAQIDataListStr = resp.read()#返回当前城市所有检测点的数据
		# print "------station------>"+stationAQIDataListStr+"<--------station------------"
		#按照 城市_检测点_时间点--》key 保存数据
		stationDataObj = json.loads(stationAQIDataListStr)
		for k in range(len(stationDataObj)):
			try:
				a = stationDataObj[k]["position_name"]
			except :
				print '查询城市【',cityName,'】的检测点时发生异常，继续查询下一个城市'
				break
			if stationDataObj[k]["position_name"] is None:
				print 'station name is null--------- break'
				pass
			else:
				#print 'position_name is ===>',stationDataObj[k]['position_name']
				key = stationDataObj[k]["area"]+'_'+stationDataObj[k]["position_name"]+'_'+dataObj[x]["time_point"]
				# print 'key is ===>',key
				value = stationDataObj[k]
				historyStationClient.set(key,value)
				print 'position data key:',key,',has save in couchbase'

# 更新源数据  源数包含经纬度
def updateSourceData(time_point_db,newData):
	print '上一次数据更新时间',time_point_db
	jsonObj = json.loads(newData)
	time_point =  jsonObj[0]["time_point"]
	print '本次数据更新时间',time_point
	#aqi 最新数据json 循环加入经纬度信息
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

#生成GEOjson 文件 远程拷贝到服务器文件目录
#
def updateGeoJsonWithAQI(data):
	geoAqiJsonDict = {"type": "FeatureCollection"}
	aqiJson = json.loads(data)
	geoAqiJsonArray= []
	if (True):
		print '数据库中没有最新的geo-aqi 数据,准备从本地文件进行拼装.....'
		f = file(citiesGeoFile)
		print '程序开始加载本地标准GEO文件(时间比较长)................'
		localGeoJson = json.load(f, encoding="utf-8")
		print '标准GEO文件加载完成................'
		f.close()
		# print localGeoJson['features'][0]['properties']['name']
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
		print '注意：全国城市个数【',len(localGeoJson['features']),'，其中',len(aqiJson)-len(geoAqiJsonArray),'】个城市 不在全国地级市城市列表，不会生成aqigeo数据'
		print '所有城市的aqi数据注入完成'
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
			#第一次保存的时候，加入经纬度信息
			firstTimePointDict = {"AQI":dataList}
			historyClient.set(jsonObj[x]["area"]+'_'+jsonObj[x]["time_point"],firstTimePointDict)
			continue
		dictOld = dataList["AQI"]
		dictNew = jsonObj[x]
		dictOld.append(dictNew)
		result["AQI"] = dictOld
		historyClient.set(jsonObj[x]["area"],result)
if __name__ == '__main__':

	#jsonData = '[{"aqi": 19,"area": "三亚","pm2_5": 8,"time_point": "2012-05-26T08:00:00Z"},{"aqi": 19,"area": "北京","pm2_5": 8,"time_point": "2012-05-26T08:00:00Z"}, {"aqi": 17,"area": "重庆","pm2_5": 10,"time_point": "2012-05-26T08:00:00Z"}]'
    #jsonData = '[{"aqi": 19,"area": "广州","pm2_5":8,"time_point":"2012-05-28T09:00:00Z"},{"aqi": 17,"area": "北京","pm2_5": 10,"time_point": "2012-05-28T09:00:00Z"},{"aqi": 17,"area": "重庆","pm2_5": 10,"time_point": "2012-05-28T09:00:00Z"},{"aqi": 17,"area": "武汉","pm2_5": 10,"time_point": "2012-05-28T09:00:00Z"}]'

	#jsonData = '{"error": "Sorry，您这个小时内的API请求次数用完了，休息一下吧！"}'
	#	jsonData = aqiJson()
	#jsonData = '[{"aqi": 19,"area": "鞍山","pm2_5":8,"time_point":"2014-09-28T09:01:07Z"}]'
	jsonData = aqiJson()
	# jsonData = '[{"aqi": 30,"area": "三亚","pm2_5":8,"time_point":"2012-07-28T09:00:00Z"},{"aqi": 30,"area": "三亚111","pm2_5":8,"time_point":"2012-07-28T09:00:00Z"},{"aqi": 30,"area": "三亚222","pm2_5":8,"time_point":"2012-07-28T09:00:00Z"},{"aqi": 17,"area": "北京","pm2_5": 10,"time_point": "2012-06-28T09:00:00Z"}]'
	#jsonData = '[{"aqi": 30,"area": "三亚","pm2_5":8,"time_point":"2012-05-28T09:00:00Z"},{"aqi": 17,"area": "北京","pm2_5": 10,"time_point": "2012-05-28T09:00:00Z"}]'
	jsonData = cutAqiJson(jsonData)
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


