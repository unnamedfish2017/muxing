#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
df=[]

root='/home/jovyan/data/store/rsync/facts-stockpool/predict/t0_model/v_20231115/'
for ml in os.listdir(root):
    tp=pd.read_csv(os.path.join(root,ml)).iloc[:,1:]
    tp.loc[:,'seg']='T0'
    tp.loc[:,'code']=tp.instrument.apply(lambda x:x[-6:]+'.'+x[:2].lower())
    tp.loc[:,'date']=ml[:8]
    df.append(tp)
df_t0=pd.concat(df).reset_index(drop=True)


# In[2]:


df=[]
root='/home/jovyan/data/store/rsync/facts-stockpool/predict/open_928/v_20231109/'
for ml in os.listdir(root):
    tp=pd.read_csv(os.path.join(root,ml)).iloc[:,1:]
    tp.loc[:,'seg']='hc'
    tp.loc[:,'code']=tp.instrument.apply(lambda x:x[-6:]+'.'+x[:2].lower())
    tp.loc[:,'date']=ml[:8]
    df.append(tp)
df_hc=pd.concat(df).reset_index(drop=True)


# In[3]:


def get_p(dt,stk,tm_st1=930,tm_et1=931,tm_st2=936,tm_et2=937,tm_st3=321,tm_et3=932):
    dt=str(dt)
    y=dt[:4]
    m=dt[4:6]
    d=dt[6:]
    tm_st1*=100000
    tm_st2*=100000
    tm_et1*=100000
    tm_et2*=100000
    tm_st3*=100000
    tm_et3*=100000
    
    path='/home/jovyan/data/store/stock/data_/second_3'+'/year='+str(y)+'/month='+str(m)+'/date='+str(d)+'/'+stk.lower()+'.parquet'

    df_sec=pd.read_parquet(path, engine='pyarrow').reset_index(drop=True)
    t1=df_sec[df_sec.Time>=tm_et1].iloc[0,:].to_dict()
    t2=df_sec[df_sec.Time>=tm_st1].iloc[0,:].to_dict()
    avg_p1=(t2['AccTurnover']-t1['AccTurnover'])/(t2['AccVolume']-t1['AccVolume'])
    
    t1=df_sec[df_sec.Time>=tm_et2].iloc[0,:].to_dict()
    t2=df_sec[df_sec.Time>=tm_st2].iloc[0,:].to_dict()
    avg_p2=(t2['AccTurnover']-t1['AccTurnover'])/(t2['AccVolume']-t1['AccVolume'])
    
    t1=df_sec[df_sec.Time>=tm_et3].iloc[0,:].to_dict()
    t2=df_sec[df_sec.Time>=tm_st3].iloc[0,:].to_dict()
    avg_p3=(t2['AccTurnover']-t1['AccTurnover'])/(t2['AccVolume']-t1['AccVolume'])
    
    p_st=t1['Open']
    return dt,stk,p_st,avg_p1,avg_p2,avg_p3


# In[4]:


tp_unq=df_t0[['code','date']].append(df_hc[['code','date']]).drop_duplicates().reset_index(drop=True)
cs=[]
for k in tp_unq.index:
    v=tp_unq.loc[k,:]
    dt=v['date']
    stk=v['code']
    cs.append((dt,stk))


# In[5]:


#cs=cs[:30]
import numpy as np
数据={}
from multiprocessing import Queue, Process, Pool
import os

def add(传入):
    global 数据
    if 传入 is None:
        return
    dt=传入[0]
    stk=传入[1]
    try:
        数据[stk+'_'+dt]=传入
    except:
        #print(dt,'error')
        pass
    return

def get_pool(n=20,cs=[]):
    p = Pool(n) # 设置进程池的大小
    for v in cs:
        p.apply_async(get_p, (v[0],v[1],),callback=add)   #维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
    p.close() # 关闭进程池
    p.join()
get_pool(30,cs)
tp=[v for k,v in 数据.items()]
tp=pd.DataFrame(tp,columns=['date','code','p_st','p_avg1','p_avg2','p_avg3'])


# In[6]:


ret_t0=df_t0.merge(tp)
ret_t0.loc[:,'ret']=(ret_t0.p_avg2/ret_t0.p_avg1-1)*ret_t0.action.apply(lambda x:-1 if x=='sell' else 1)
ret_t0.ret.mean()


# In[7]:


ret_t0.groupby(['date','action']).ret.agg(['mean','count'])


# In[8]:


ret_hc=df_hc.merge(tp)
ret_hc.loc[:,'ret']=(ret_hc.p_avg3/ret_hc.p_avg1-1)*ret_hc.action.apply(lambda x:1 if x=='sell' else -1)
ret_hc.ret.mean()


# In[9]:


ret_hc.groupby(['date','action']).ret.agg(['mean','count'])


# In[ ]:




