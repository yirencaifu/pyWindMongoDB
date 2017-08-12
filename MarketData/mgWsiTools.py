# -*- coding: utf-8 -*-
"""
Created on Sat Sep 27 08:11:48 2014

@author: space_000
"""
from mgWindTools import empDates,stockSets,dateMark,deExcept
from WindPy import w
import pymongo as mg
#from wsiTools import findDate
#%%
def upiter(data=[[]],uniField=[],t='',col=None,tokens=['o','h','l','c','v']):
    
    for i in uniField:
        be=data[0].index(i)
        en=be+242
        docu={}
        for j in xrange(1,len(tokens)+1):
            docu['%s.%s.%s'%(i[:6],t,tokens[j-1])]=\
            data[j][be:en]
        col.update({'_id':int(i[:6])},{'$set':docu})

#%%
#20130708
def mgWsiUp(date=[]):
    w.start()
    # Wind所需股票、日期
    Field=stockSets('shsz')
    stride=100
    numF=range(len(Field))[::stride]
    if date:#若指定日期则下载指定的，否则下载markDate中没被下载的日期
        date=[str(d) for d in date]
    else:
        date=empDates('min')
#    dt=loadmat('D:\dataTime')
#    timeD=dt['time']
#    times=findDate(timeD,'20140925',30)
    #Mongo数据库
    date=date[:2025]
    client=mg.MongoClient()
    db=client['MKD']
    col=db['minData']
    #异常处理矩阵（与日期date）长度相同
    unDownStock=[[] for i in range(len(date))]
    #下载并插入数据库
    for i,t in enumerate(reversed(date)):
        for f in numF:
            data=w.wsi(Field[f:f+stride],'open,high,low,close,volume',t,t+' 15:01:00','showblank=0',barsize=1).Data[1:]
            if data:
                uniField=set(data[0])
                #Todo: 异常处理如Internet Timeout
                unDownStock=deExcept(t,unDownStock)
                #数据存入数据库
                upiter(data,uniField,t,col)
            else:
                unDownStock[i]=[-1]
    dateMark('min',date,unDownStock)

