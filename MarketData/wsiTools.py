# -*- coding: utf-8 -*-
"""
Created on Sat Sep 27 09:19:06 2014

@author: space_000
"""

def findDate(timeD=[[]],endTime=0,lagt=0,betime=0):
    """
    timeD: wtdays
    endTime: time str '20140926' default=today
    lagt: lag time
    OR
    betime: time str
    """
    import time
    # 用户没有指定时间，生成当前时间
    if endTime==0:
        endTime=time.strftime('%Y%m%d')
    # 字符串时间列表 -> 数值时间列表
    timeDD=[]
    for i in timeD:
        timeDD.append(int(i[0]))
    # 找到trade days 中的离当前时间最近的时间
    tpTime=int(endTime)
    fl=False
    while fl==False:
        if tpTime in timeDD:
            fl=True
        else:
            tpTime=tpTime-1
    
    if betime==0:
        # 返回用户指定的lagt长度的时间列表times
        times=timeDD[timeDD.index(tpTime)-\
        lagt+1:timeDD.index(tpTime)+1]
    else:
        # 找到begin time 之前最近的wtdays
        tpbeTime=int(betime)
        fl=False
        while fl==False:
            if tpbeTime in timeDD:
                fl=True
            else:
                tpbeTime=tpbeTime-1
        # 返回用户指定时间区间betime-endtime的times
        times=timeDD[timeDD.index(tpbeTime):\
        timeDD.index(tpTime)+1]
    return times