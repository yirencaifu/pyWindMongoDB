# -*- coding: utf-8 -*-
"""
Created on Sat Oct 25 09:58:36 2014

@author: space_000
"""

import multiprocessing
from scipy.io import loadmat
import WindPy
#%%
def calculate(args):
    func,arg=args
    result=func(*arg)
    return result

def mgWsdUp(Field,timeD,i):
    
    WindPy.w.start()
    
    data=WindPy.w.wsd(Field,i,str(timeD[0][0]),str(timeD[-1][0]),'showblank=0')
    
    return data

#%%
def test():
    
    d=loadmat('D:\FieldSHSZ')
    Field=d['Field'].tolist()
    
    dt=loadmat('D:\dataTime')
    timeD=dt['time']
    
    indicPara=['open','high','low','close','volume']
    Tasks=[(mgWsdUp,(Field,timeD,i)) for i \
    in indicPara]  
    
    Processes=4
    pool= multiprocessing.Pool(Processes)
    
    results=pool.map_async(calculate,Tasks)
    pool.close()
    pool.join()
    return results

#%%
if __name__=='__main__':
    results=test()
    for i in results:
        print i