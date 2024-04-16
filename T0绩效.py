#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pickle
import pandas as pd
import numpy as np
import os,pickle
import pandas as pd
path='/home/jovyan/work/commons/data/daily_data/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
closew = data_daily['closew'].fillna(method='ffill')
lst_trd_day=closew.index[-1].strftime('%Y%m%d')
root='..//..//原始数据_托管'

import sys
sys.path.append('..//..//代码')
from 报表函数 import *
ios_all,ods_all=get_records(root)

ods_all.loc[:,'日期']=pd.to_datetime(ods_all.DealDateTime.apply(lambda x:x.date()))
ods_all.loc[:,'TradedVolume']=ods_all.loc[:,'TradedVolume'].astype(float)
ods_all.loc[:,'Amt']=ods_all.loc[:,'TradedPrice'].astype(float)*ods_all.loc[:,'TradedVolume'].astype(float)
ods_all.loc[:,'tm']=ods_all.DealDateTime.apply(lambda x:x.strftime('%H%M'))


# In[2]:


ods_all[ods_all.T0_MIYUAN==1].to_parquet('/home/jovyan/work/commons/T0_orders.parquet')


# In[8]:


ods_kft0=ods_all[ods_all.OrderRef.str.contains('T0_KaFang')]


# In[9]:


tp=ods_kft0.groupby(['日期','ProductName','StrategyName','Instrument','BuySell']).apply(lambda x:(x['TradedPrice'].astype(float)*x['Amt'].astype(int)).sum()/x['Amt'].sum())
p=tp.rename('p')#.reset_index()

tp=ods_kft0.groupby(['日期','ProductName','StrategyName','Instrument','BuySell']).apply(lambda x:x['TradedVolume'].sum())
v=tp.rename('v').reset_index()

v=v.groupby(['日期','ProductName','StrategyName','Instrument']).v.mean().reset_index()

tp=p.reset_index().merge(v,how='left')
ts=tp[tp.BuySell=='Sell'].rename(columns={'p':'ps'})
tb=tp[tp.BuySell=='Buy'].rename(columns={'p':'pb'})
del ts['BuySell']
del tb['BuySell']
tp=ts.merge(tb)
tp.loc[:,'delta']=(tp.ps-tp.pb)*tp.v
tp.loc[:,'amt']=tp.pb*tp.v
t1=tp.groupby(['日期']).apply(lambda x:[x.delta.sum()/x.amt.sum(),x.amt.sum()//10000])
t1


# In[10]:


tp


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




