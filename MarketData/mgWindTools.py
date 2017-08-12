#spdprocjects



# -*- coding: utf-8 -*-
"""
Created on Tue Nov 25 10:25:48 2014

@author: space_000
"""
import pymongo as mg
client=mg.MongoClient()
db=client['MKD']
colMKInit=db['marketInit']
#%% 返回仍未被下载数据的日期
def empDates(freq=''):
    if freq=='day' or freq=='D' or freq=='d':
        rawDate=colMKInit.find({'_id':'2014DateMark'},{'_id':0}).next()
        keysDate=rawDate.keys()
        valuesDate=rawDate.values()
        date=[keysDate[i] for i,v in enumerate(valuesDate) if v['day']==0]
        date.sort()
        return date
    elif freq=='min'or freq=='M' or freq=='m':
        rawDate=colMKInit.find({'_id':'2014DateMark'},{'_id':0}).next()
        keysDate=rawDate.keys()
        valuesDate=rawDate.values()
        date=[keysDate[i] for i,v in enumerate(valuesDate) if v['min']==0]
        date.sort()
        return date

#%% 返回对应形式（int\str\shsz）、对应年份的 全部A股
def stockSets(form='',year='2014'):
    tok='StockCode'
    if form=='int' or form=='num':
        stri=year+'int'+tok
    elif form=='str':
        stri=year+'str'+tok
    elif form == 'shsz' or form=='SHSZ':
        stri=year+'shsz'+tok
        
    sets=colMKInit.find({'_id':stri},{stri[4:]:1,'_id':0}).next()[stri[4:]]
    return sets

#%% 将 数据下载结果（已下载 1 未下载 0 超时等异常 异常的股票代码）保存至2014DateMark
def dateMark(freq='',date=[],unDownStock=[[]],year='2014'):
    '''
    unDownStock为出现异常没有被正确下载、网络连接异常等情况的 股票代码
    '''
#    freq='day'
#    year='2014'
#    date=empDates('day')
#    unDownStock=[[] for i in range(len(date))]
    unDown=[(i,s) for i,s in enumerate(unDownStock) if s] #取出异常天i  异常股票集u
    stri=year+'DateMark'
    
    query={}
    if freq=='day':
        for i in date:
            query['%s.day'%(i)]=1
        for i,u in unDown:
            query['%s.day'%(date[i])]=u
    else:
        for i in date:
            query['%s.min'%(i)]=1
        for i,u in unDown:
            query['%s.min'%(date[i])]=u
    
    colMKInit.update({'_id':stri},{'$set':query})

#%% Todo: 异常处理如Internet Timeout
def deExcept(date=[],unDownStock=[],data=[]):
    pass
    return unDownStock
