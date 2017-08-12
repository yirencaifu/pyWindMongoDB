# -*- coding: utf-8 -*-
"""
Created on Tue Nov 18 22:56:28 2014

@author: space_000
"""
from scipy.io import loadmat
import numpy as np
import pymongo as mg

client=mg.MongoClient()
db=client['MKD']
colMKInit=db['marketInit']
#%% Create market trading days
d=loadmat('E:\\Code Laboratory\\MFpy\\MongoPy\\MarketData\\wtdays')
tdays=d['c']
daa=[int(t[0][0]) for t in tdays]
colMKInit.insert({'_id':'tdays','tdays':daa})
#%% Create 2014 stock code list
d=loadmat('D:\dbField1')
Field=[int(s) for s in d['Field']]
colMKInit.insert({'_id':'2014intStockCode','intStockCode':Field})
Field=[str(s) for s in d['Field']]
colMKInit.insert({'_id':'2014strStockCode','strStockCode':Field})
Field=np.array(Field)
mField=[]
for i in xrange(Field.shape[0]):
    lf=6-len(str(Field[i]))
    mField.append('0'*lf+str(Field[i]))
field=[]
for i in mField:
    if i[0]=='6':
        field.append(i+'.SH')
    else:
        field.append(i+'.SZ')
colMKInit.insert({'_id':'2014shszStockCode','shszStockCode':field})
#%% 生成是否下载了当天、对应的股票集、五个行情数据的矩阵。暂包括日数据、分钟数据
tdays=colMKInit.find({'_id':'tdays'},{'_id':0}).next()

mark={'min':0,'day':0}
query={}
for i in tdays['tdays']:
    query[str(i)]=mark
colMKInit.insert(dict({'_id':'2014DateMark'},**query))