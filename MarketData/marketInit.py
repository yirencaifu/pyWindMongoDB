# -*- coding: utf-8 -*-
"""
Created on Tue Nov 18 22:56:08 2014

@author: space_000
"""
import pymongo as mg
import datetime
from WindPy import w
from mgWsdTools import mgWsdUp
client=mg.MongoClient()
db=client['MKD']
colMKInit=db['marketInit']
w.start()
#%% 每次marketInit调用时更新的数据库信息
def marketInit():
    oldtdays=colMKInit.find({'_id':'tdays'}).next()['tdays']
    today=str(datetime.date.today())
    tdays=[int(t.strftime('%Y%m%d')) for t in w.tdays(str(oldtdays[-1]),today).Times]
    newtdays=oldtdays+tdays[1:]#每次下载oldtdays最后一天到今天之间所有的交易日期
    if datetime.datetime.today().hour>=15:#以下初始化仅在收盘后运行
        if tdays[1:]:
    #        更新trading days信息
            colMKInit.update({'_id':'tdays'},{'$set':{'tdays':newtdays}})
    #        更新没被下载的日期'2014DateMark'的信息
            mark={'min':0,'day':0}
            query={}
            for i in tdays[1:]:
                query[str(i)]=mark
            colMKInit.update({'_id':'2014DateMark'},{'$set':query})
    #        更新新日期中的dayData Collection
            mgWsdUp(tdays[1:])
    #        更新新日期中的minData Collection

#%%
marketInit()