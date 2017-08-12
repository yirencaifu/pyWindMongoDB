# -*- coding: utf-8 -*-
"""
Created on Thu Nov 27 14:50:47 2014

@author: space_000
"""

#%%
data=col.find({'_id':1}).next()['000001']
skey=sorted(data.keys())
datas=[]
for i in skey:
    datas.append(data[i])
emp=[]
for j,d in enumerate(datas):
    if d['c']==[]:
        emp.append(skey[j])
#%%
from mgWindTools import empDates,stockSets,deExcept,dateMark
from WindPy import w
import pymongo as mg
from mgWsiTools import upiter
w.start()
# Wind所需股票、日期
Field=stockSets('shsz')
stride=100
numF=range(len(Field))[::stride]
date=empDates('min')
date=date[1001:date.index(u'20130118')+1]

client=mg.MongoClient()
db=client['MKD']
col=db['minData']
#异常处理矩阵（与日期date）长度相同
unDownStock=[[] for i in range(len(date))]
#下载并插入数据库
for i,t in enumerate(reversed(date)):
    aggData=[[w.wsi(Field[f:f+stride],'open,high,low,close,volume',\
    t,t+' 15:01:00','showblank=0',barsize=1).Data[1:]]\
    for f in numF]
    for data in aggData:
        if data[0]:
            uniField=set(data[0][0])
            #Todo: 异常处理如Internet Timeout
            unDownStock=deExcept(t,unDownStock)
            #数据存入数据库
            upiter(data[0],uniField,t,col)
        else:
            unDownStock[i]=[-1]
dateMark('min',date,unDownStock)