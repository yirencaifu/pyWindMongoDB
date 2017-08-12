# -*- coding: utf-8 -*-
"""
Created on Sun Sep 28 10:55:10 2014

@author: space_000
"""

from scipy.io import loadmat
from WindPy import w
import pymongo as mg
#%%
def upiter(Data=[[]],Codes=[],Times=[],Fields='',col=None):
    if Fields=='open':
        Fields='o'
    elif Fields=='close':
        Fields='c'
    elif Fields=='high':
        Fields='h'
    elif Fields=='low':
        Fields='l'
    elif Fields=='volume':
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
def mgWsdUp():
    d=loadmat('D:\FieldSHSZ')
    Field=d['Field'].tolist()
    
    dt=loadmat('D:\dataTime')
    timeD=dt['time']
    
    w.start()
    
    client=mg.MongoClient()
    db=client['MKD']
    col=db['dayData']
    indicPara=['open','high','low','close','volume']
    for i in indicPara:
        data=w.wsd(Field,i,str(timeD[0][0]),str(timeD[-1][0]),'showblank=0')
        upiter(Data=data.Data,Codes=data.Codes,Times=data.Times,Fields=i,col=col)
    
if __name__=='__main__':
    mgWsdUp()