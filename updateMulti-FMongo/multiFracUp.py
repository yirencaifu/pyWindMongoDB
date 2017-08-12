# -*- coding: utf-8 -*-
"""
Created on Sun Oct 26 18:26:33 2014

@author: space_000
"""
import pymongo as mg
import numpy as np

def upiter(field='',f_aRes=[],AlphaRes=[],f_a1Res=[],Alpha1Res=[],ADayRes=[],\
timed=[],alag=[4,5,6,7]): # update per field's data

    client=mg.MongoClient()
    db=client['test']
    
    colADayR=db['ADayR']
    
    colf_a=db['f_a']
    colAlpha=db['Alpha']
    
    colf_aC=db['crackf_a']
    colAlphaC=db['crackAlpha']
    
    queryADayR,queryf_a,queryAlpha,queryf_aC,queryAlphaC=({} for i in xrange(5))
    for i,l in enumerate(alag):
        for j,t in enumerate(timed):
            if isinstance(f_aRes[1,2],np.ndarray):
                queryADayR['%s.%s.%s'%(field,t,l)]= \
                ADayRes[i,j].tolist()
                
                queryf_a['%s.%s.%s'%(field,t,l)]= \
                f_aRes[i,j].tolist()
                queryAlpha['%s.%s.%s'%(field,t,l)]= \
                AlphaRes[i,j].tolist()
                
                queryf_aC['%s.%s.%s'%(field,t,l)]= \
                f_a1Res[i,j].tolist()
                queryAlphaC['%s.%s.%s'%(field,t,l)]= \
                Alpha1Res[i,j].tolist()
            
    field=int(field)
    colADayR.update({'_id':field},{'$set':queryADayR})
    
    colf_a.update({'_id':field},{'$set':queryf_a})
    colAlpha.update({'_id':field},{'$set':queryAlpha})
    
    colf_aC.update({'_id':field},{'$set':queryf_aC})
    colAlphaC.update({'_id':field},{'$set':queryAlphaC})