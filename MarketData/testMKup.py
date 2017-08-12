# -*- coding: utf-8 -*-
"""
Created on Sat Sep 27 08:11:48 2014

@author: space_000
"""

from scipy.io import loadmat
from WindPy import w
import pymongo as mg
from wsiTools import findDate
from mgWsi import upiter

d=loadmat('D:\FieldSHSZ')
Field=d['Field'].tolist()
stride=100
numF=range(len(Field))[::stride]

dt=loadmat('D:\dataTime')
timeD=dt['time']
times=findDate(timeD,'20140925',30)

w.start()

client=mg.MongoClient()
db=client['MKD']
col=db['minData']

for t in times:
    for f in numF:
        data=w.wsi(Field[f:f+stride],'open,high,low,close,volume',str(t),str(t)+'15:01:00','showblank=0',barsize=1).Data[1:]
        uniField=set(data[0])
        upiter(data,uniField,t,col)
