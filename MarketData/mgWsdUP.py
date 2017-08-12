# -*- coding: utf-8 -*-
"""
Created on Sun Sep 28 10:55:10 2014

@author: space_000
"""
from marketInit import empDates,stockSets
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

    for c in xrange(len(Codes)):
        docu={}
        for t in xrange(len(Times)):
            docu['%s.%s.%s'%(Codes[c][:6],Times[t].strftime('%Y%m%d'),Fields)]=\
            Data[c][t]
        col.update({'_id':int(Codes[c][:6])},{'$set':docu})
#%%
def mgWsdUp(date=[]):
    client=mg.MongoClient()
    db=client['MKD']
    
    w.start()
    
    Field=stockSets('shsz')
    if ~date:
        date=empDates('day')
    indicPara=['OPEN','HIGH','LOW','CLOSE','VOLUME']
    
    stride=500
    numDate=range(len(date))[::stride]+[len(date)]
    colD=db['dayData']
    
    for i in indicPara:
        for n in xrange(len(numDate)-1):
            data=w.wsd(Field,i,str(date[numDate[n]]),str(date[numDate[n+1]-1]),'showblank=0')
            upiter(Data=data.Data,Codes=data.Codes,Times=data.Times,Fields=i,col=colD)

if __name__=='__main__':
    mgWsdUp()