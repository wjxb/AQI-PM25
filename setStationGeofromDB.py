#! /usr/bin/python
# -*- coding: utf-8 -*-
import cookielib,urllib2
from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError
from couchbase.views.params import Query
import  json
import xlrd
import xlwt
import sys
import os
import string
reload(sys)
sys.setdefaultencoding("utf8")
'''
    数据库中已经保存了所有城市的所有监测点名称。此脚本需要完成。
    1：调用百度api 只设置城市经纬度信息。
    2：对于所有监测站的经纬度读取excel文件获取。
    脚本最终作用：
    1：aqi抓取脚本的所有经纬度信息不再调用接口进行获取而读取数据库中已经保存的经纬度信息。
'''
baiduUrlAddressInCity ='http://api.map.baidu.com/geocoder/v2/?ak=zMCmT2jFBggL0fBT3MVIwSkj&ip=&output=json'
host='192.168.100.200'
port=8091
pwd='111111'
client = Couchbase.connect(bucket='lastestAQIData',  host=host ,port=port,password=pwd)
stationMap={} # 从文件中读取到的检测站经纬度list
areaidMap={}
noGeoStation = [] #excel 中没有填写的城市监测点 将监测站 写回excel中补充 城市名，监测站名。预留经纬度
def open_excel(file= 'a.xls'):
    try:
        data = xlrd.open_workbook(file)
        return data
    except Exception,e:
        print str(e)
#根据索引获取Excel表格中的数据   参数:file：Excel文件路径     colnameindex：表头列名所在行的所以  ，by_index：表的索引
# 返回值形式，[{'张家界_电业局'：[检测站row]},{'检测站名称'：[检测站row]}]
def excel_table_byindex(file= 'a.xls',colnameindex=1,by_index=0):
    data = open_excel(file)
    table = data.sheets()[by_index]
    nrows = table.nrows #行数
    # print '包括表头在内，数据行数',nrows
    ncols = table.ncols #列数
    # print '信息列数',ncols
    # colnames =  table.row_values(colnameindex) #某一行数据
    for rownum in range(1,nrows): #从数据行开始循环 忽略表头
         row = table.row_values(rownum)
         colnames =  table.row_values(rownum)
         # print type(row)
         if row:
             
             # print row,colnames[0]+'_'+colnames[1]
             stationMap[colnames[0].strip()+'_'+colnames[1].strip()] = row
             # print colnames[0]+'_'+colnames[1]
         else:
             # print '-----',colnames[0],colnames[1]
             continue
    print '检测站个数',len(stationMap)
    return stationMap

"""读取areaid.xls 获取城市 areaid 同时把areaid保存到db"""
def areaid_table_byindex(file= 'areaid_v.xlsx',colnameindex=1,by_index=0):
    cityAndStation ={}
    cityAndStation = client.get("cityAndStation").value #从数据库中查询检测站和城市

    data = open_excel(file)
    table = data.sheets()[by_index]
    nrows = table.nrows #行数
    # print '包括表头在内，数据行数',nrows
    ncols = table.ncols #列数
    # print '信息列数',ncols
    # colnames =  table.row_values(colnameindex) #某一行数据
    for rownum in range(1,nrows): #从数据行开始循环 忽略表头
         row = table.row_values(rownum)
         colnames =  table.row_values(rownum)
         # print type(row)
         if row:
             
             # print row,colnames[0]+'_'+colnames[1]
             # stationMap[colnames[0].strip()+'_'+colnames[1].strip()] = row
             print str(colnames[0]).strip(),str(colnames[2]).strip()
             areaidMap[colnames[2]]=colnames[0]
             continue
         else:
             # print '-----',colnames[0],colnames[1]
             continue

    """向数据库中设置areaid"""
    for cityObj in cityAndStation["cityAndStation"]:
        cityName = cityObj["city"]
        print cityName
        # print areaidMap
        try:
            areaid=areaidMap[cityName]
            cityObj["areaid"]=areaid
            # print cityObj["areaid"]
        except:
            print "error-->",cityName
            continue
        
    client.set("cityAndStation", cityAndStation)
    print '设置完成。。。'

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

# 从数据库中获取已经保存的城市和监测点的经纬度信息
def getCityAndStation():
    print '从excel中读取的行数',len(stationMap)
    cityAndStation ={}
    cityAndStation = client.get("cityAndStation").value #从数据库中查询检测站和城市
    # print(cityAndStation["cityAndStation"][0]['city'].encode("utf8")) 根据utf8编码读取汉字
    for cityObj in cityAndStation["cityAndStation"]: #循环190个城市

        cityName = cityObj["city"].encode("utf8")
        # print '当前城市:-----------',cityName
        #coordinate = getcoordinate(baiduUrlAddressInCity,cityName,'')
        #geoObj = json.loads(coordinate)
        #设置当前城市的经纬度
        # cityObj["latitude"]=geoObj['result']['location']['lat']
        # cityObj["longitude"]=geoObj['result']['location']['lng']
        #设置当前城市的所有监测点的经纬度
        for i in range(len(cityObj["stations"])): #循环每个城市的检测站
            #print '当前城市的监测点名称:',stationObj["station_name"].encode("utf8")
            key = cityObj["stations"][i]['station_name'] #DB中的检测站名称
            mapKey = cityObj["city"]+'_'+key
            row = stationMap.get(mapKey)
            # print mapKey.encode("utf8")
            if row !=None:
                try:
                    cityObj["stations"][i]["latitude"]=row[3]
                    cityObj["stations"][i]["longitude"]=row[2]
                except Exception, e:
                    print row
                    print '没有经纬度的城市',mapKey.encode("utf8")
                    noGeoStation.append(mapKey.encode("utf8"))
                    print str(e)
                    continue
            else:
                print '没有经纬度的城市',mapKey

    #print cityAndStation["cityAndStation"][0]
    client.set("cityAndStation", cityAndStation)
    print '没有经纬度信息的监测站个数：',len(noGeoStation)
    #print noGeoStation[0].split("_")[0]


# 主函数 程序入口
def main():
    # excel_table_byindex()
    # getCityAndStation()
    areaid_table_byindex()
    # coordinate = getcoordinate(baiduUrlAddressInCity,'天津','')
    # coordinate2 = getcoordinate(baiduUrlAddressInCity,'北海工业园','北海')
    # print str(coordinate2)
    # coordinateObj = json.loads(coordinate)
    # coordinateObj2 = json.loads(coordinate2)
    # print coordinateObj['result']['location']['lat']
    # print coordinateObj['result']['location']['lng']
    # print '-----------------'
    # print coordinateObj2['result']['location']['lat']
    # print coordinateObj2['result']['location']['lng']
    print '程序结束运行'
if __name__ == '__main__':
    main()


