# -*- coding: utf-8 -*-
"""
Created on Sun Sep 28 10:55:10 2014

@author: space_000
"""
from mgWindTools import empDates,stockSets,dateMark,deExcept
from WindPy import w
import pymongo as mg
#%%
def upiter(Data=[[]],Codes=[],Times=[],Fields='',col=None):
    if Fields=='OPEN':
        Fields='o'
    elif Fields=='CLOSE':
        Fields='c'
    elif Fields=='HIGH':
        Fields='h'
    elif Fields=='LOW':
        Fields='l'
    elif Fields=='VOLUME':
        Fields='v'
    else:
        raise Exception('Check the Fields, all letters must be lower level')
#    当只输入一个日期、m个股票时，Wind会返回data.Data[[m]]而非[[]...m个...[]]
    if len(Data)==1:
        Data=[[i] for i in Data[0]]
    
    for c in xrange(len(Codes)):
        docu={}
        for t in xrange(len(Times)):
            docu['%s.%s.%s'%(Codes[c][:6],Times[t].strftime('%Y%m%d'),Fields)]=\
            Data[c][t]
        col.update({'_id':int(Codes[c][:6])},{'$set':docu})
#%% 根据date下载数据
def mgWsdUp(date=[]):
    w.start()
    # Wind所需股票、指标、日期
    Field=stockSets('shsz')
    indicPara=['OPEN','HIGH','LOW','CLOSE','VOLUME']
    if date:#若指定日期则下载指定的，否则下载markDate中没被下载的日期
        date=[str(d) for d in date]
    else:
        date=empDates('day')
    stride=500
    numDate=range(len(date))[::stride]+[len(date)]
    #Mongo数据库
    client=mg.MongoClient()
    db=client['MKD']
    colD=db['dayData']
    #异常处理矩阵（与日期date）长度相同
    unDownStock=[[] for i in range(len(date))]
    #下载并插入数据库
    for i in indicPara:
        for n in xrange(len(numDate)-1):
            data=w.wsd(Field,i,str(date[numDate[n]]),str(date[numDate[n+1]-1]),'showblank=0')
            #Todo: 异常处理如Internet Timeout
            unDownStock=deExcept(date[numDate[n]:numDate[n+1]],unDownStock)
            #数据存入数据库
            upiter(Data=data.Data,Codes=data.Codes,Times=data.Times,Fields=i,col=colD)
    #将MarketInit中 dateMark标记为已下载 异常股票存入相应日期、频率
    dateMark('day',date[numDate[n]:numDate[n+1]],unDownStock)

if __name__=='__main__':
    mgWsdUp()