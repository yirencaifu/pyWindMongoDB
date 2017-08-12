# -*- coding: utf-8 -*-
"""
Created on Sun Nov 30 09:30:51 2014

@author: space_000
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Sep 26 22:15:46 2014

@author: space_000
"""
#%% Initialization
from scipy.io import loadmat
import pymongo as mg

d=loadmat('D:\dbField1')
Field=d['Field']

dt=loadmat('D:\dataTime')
time=dt['time']

client=mg.MongoClient()
db=client['MKD']
colMKInit=db['marketInit']

stockCode=colMKInit.find({'_id':'2014strStockCode'}).next()\
['strStockCode'][:100]
dateSet2014=[i for i in colMKInit.find({'_id':'tdays'}).next()\
['tdays'] if i>20140101]
dateSet2013=[i for i in colMKInit.find({'_id':'tdays'}).next()\
['tdays'] if 20121231<i<20140101]

db=client['sepMD']
colSep2013=db['min2013']
colSep2014=db['min2014']

#%% DB contents
lag=['o','h','l','c','v']
alag={}
for l in lag:
    alag[l]=[]

data2013={str(i):alag for i in dateSet2013}
data2014={str(i):alag for i in dateSet2014}

indata3=[]
for i in xrange(len(Field)):
    indata3.append({'_id':int(Field[i]),'%s' % (Field[i]):data2013})

indata4=[]
for i in xrange(len(Field)):
    indata4.append({'_id':int(Field[i]),'%s' % (Field[i]):data2014})

#%% Create minute data DB
colSep2013.insert(indata3)
colSep2014.insert(indata4)