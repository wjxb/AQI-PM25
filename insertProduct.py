#! /usr/bin/python
# -*- coding: utf-8 -*-
'''
	此脚本是为了硬件测试临时插入产品 到aicc-product桶中。
	流程：查询aicc-product桶中的“simple-文档”
'''
from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError
import  json
import time
import sys,os,hashlib,time,base64
import urllib,urllib2
from rc4base64 import rc4base64
import random
import xlrd,xdrlib
reload(sys)  
sys.setdefaultencoding('utf8')
db200='192.168.100.200'
db80 = '10.10.10.80'
db201='11.11.11.201'
server105='http://192.168.100.105:8088/aicc/v1.0/device/deviceData/'
# server202='http://11.11.11.202:8088/aicc/v1.0/device/deviceData/'
productClient = Couchbase.connect(bucket='aicc-ProductDB',  host=db200 ,port=8091,password='111111')
couchbaseData =Couchbase.connect(bucket='aicc-CouchbaseDB',  host=db200 ,port=8091,password='111111')
userproductClient =Couchbase.connect(bucket='aicc-UserProductDB',  host=db200 ,port=8091,password='111111')
testProductClient = Couchbase.connect(bucket='aicc-ProductDB',  host=db80 ,port=8091,password='111111')

# productClient = testProductClient # 代码在正式运行时注释

productBegin =10 #当前产品库中最大的设备号 2104-09-29 14:13 AISN145010010060
count = 2 #初始化个数
productEnd =productBegin+count #本次设备初始化，设备编号结束个数
dataBegin=0
dataEnd=0
SNTEMP= 'AISN14501001' 	# AISN145010010001
							#2014代表年份
							#50代表批次
							#1001代表10月1日
							#0001~50XX是产品编码可以是断码，所以最后"XX"不一定是多少

Sheet1='mac' #第一个工作表的名称
excelName='mac.xlsx'							#
def open_excel(file= excelName):
    try:
        data = xlrd.open_workbook(file)
        return data
    except Exception,e:
        print str(e)

'''
	插入原始传感器设备信息，非激活设备，所有激活操作让傻逼自己去完成。
'''

def getSimpleProduct():
	simple = productClient.get("simple").value # dict
	return simple 

'''
	根据特殊规则进行设备编号设定,传入参数为设备顺序号
	编号规则
'''
def insertProduct(begin,end,newObj):
	for i in range(productBegin,productEnd):
		no=("%04d" % (i+1))
		sn = SNTEMP+str(no)
		simpleObj["sn"]=sn.strip()
		productClient.set(sn, simpleObj)
		print '设备 【',sn,'】已经初始化到 aicc-product库中'
'''
	从文件中导入设备mac地址， 每个编号占一行
'''
def insertProductFromNomalFile(fileName,simpleObj):
	file = open(fileName)
	for line in file:
		line = line.strip('\n')
		# productClient.set(sn, simpleObj)
		print '设备 【',line,'】已经初始化到 aicc-product库中'

#根据名称获取Excel表格中的数据   参数:file：Excel文件路径     colnameindex：表头列名所在行的所以  ，by_name：Sheet1名称
def excel_table_byname(file= excelName,colnameindex=1,by_name=Sheet1):
    data = open_excel(file)
    table = data.sheet_by_name(by_name)
    nrows = table.nrows #行数 
    colnames =  table.row_values(colnameindex) #某一行数据 
    list =[]
    for rownum in range(1,nrows):
         row = table.row_values(rownum)
         if row:
             app = {}
             for i in range(len(colnames)):
                app[rownum] = row[i]
             list.append(row[i])
    return list
'''
	从excel中读取第一个sheel 的 mac地址
'''
def insertProductFromExcel(file):
	list = excel_table_byname(file)
	for mac in list:
		print mac
	print '读取mac地址 【',len(list),'】条'

'''
	删除竞赛测试设备信息，DC3-DC200 aicc-productDB,aicc-userProductDB
'''
def deleteTestProduct():
	print '开始删除测试设备'
	for i in range(productBegin,productEnd):
		sn = 'DC'+str(i+1)
		try:
			productClient.delete(sn)
			userproductClient.delete(sn)
		except :
			continue
		print '设备 [',sn,'] 已经被删除'

'''
	删除测试数据
'''
def deteleTestData():
	for i in range(dataBegin,dataEnd):
		sn = 'DC'+str(i+1)
		rows = couchbaseData.query("test", "test",key=sn)
		for row in rows:
			try:
				couchbaseData.delete(row.value)
			except:
				continue
		print '完成删除设备【',sn,'】 的所有传感器数据'
if __name__ == '__main__':
	simpleObj = getSimpleProduct()
	# insertProduct(productBegin, productEnd, simpleObj)
	# deleteTestProduct()
	# deteleTestData()
	# insertProductFromNomalFile('mac.txt', simpleObj)
	insertProductFromExcel('mac.xlsx')
	






