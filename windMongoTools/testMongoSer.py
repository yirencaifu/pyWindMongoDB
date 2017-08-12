# -*- coding: utf-8 -*-
"""
Created on Sat Oct 25 11:07:13 2014

@author: space_000
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Oct 25 09:58:36 2014

@author: space_000
"""
#ress=[{'Data':None,'Codes':None,'Times':None,'Fields':None} for i in xrange(5)]
#
#for i,r in enumerate(results1):
#    ress[i]['Data']=r.Data
#    ress[i]['Fields']=r.Fields
#    ress[i]['Times']=r.Times
#    ress[i]['Codes']=r.Codes
#import multiprocessing
import pymongo as mg
import shelve
#%%
def calculate(args):
    func,arg=args
    result=func(*arg)
    return result

def upiter(Data=[[]],Codes=[],Times=[],Fields='',col=None):
#    client=mg.MongoClient()
#    db=client['test']
#    col=db['dayData']
    
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
def test(Data):    
    
#    Processes=4
#    pool= multiprocessing.Pool(Processes)
    
    col=mg.MongoClient()['test']['dayData']
    
    Tasks=[(upiter,(r['Data'],r['Codes'],r['Times'],r['Fields'][0],col)) for r \
    in Data]
#    Tasks=[(upiter,(r['Data'],r['Codes'],r['Times'],r['Fields'][0],c)) for c,r \
#    in zip(col,Data)]
    results=map(calculate,Tasks)
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