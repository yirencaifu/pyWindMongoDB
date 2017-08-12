# -*- coding: utf-8 -*-
"""
Created on Sat Oct 25 09:58:36 2014

@author: space_000
"""

import multiprocessing
from scipy.io import loadmat
import pymongo as mg
import shelve
#%%
def calculate(args):
    func,arg=args
    result=func(*arg)
    return result

def upiter(Data=[[]],Codes=[],Times=[],Fields='',col=None):
    client=mg.MongoClient()
    db=client['test']
    col=db['dayData']
    
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
    
    client.close()

def mgWsdUp(w,Field,timeD,i):
    
    data=w.wsd(Field,i,str(timeD[0][0]),str(timeD[-1][0]),'showblank=0')
    
    return data
#%%
def test(Data):    
    
    
    d=loadmat('D:\FieldSHSZ')
    Field=d['Field'].tolist()
    
    dt=loadmat('D:\dataTime')
    timeD=dt['time']
    
    w.start()
    
    indicPara=['open','high','low','close','volume']
    Tasks=[(mgWsdUp,(w,Field,timeD,i)) for i \
    in indicPara]
    results=map(calculate,Tasks)    
    
#    Processes=4
#    pool= multiprocessing.Pool(Processes)
#    
##    Tasks=[(upiter,(r['Data'],r['Codes'],r['Times'],r['Fields'][0])) for r \
##    in Data]
#    
#    results=pool.imap(calculate,Tasks)
#    pool.close()
#    pool.join()
    return results

#%%
if __name__=='__main__':
    filename='E:\\temp.out'
    f = shelve.open(filename)
    Data=f['ress']
    f.close()
    results=test(Data)