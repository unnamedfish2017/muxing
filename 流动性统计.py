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


# In[2]:


import os
root='/home/jovyan/work/workspaces/daily report/每日报表/持仓数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('系统_盘后持仓')])))
现有持仓=df[df.日期==pd.to_datetime(lst_trd_day)]


# In[3]:


# tm_st=132000000
# tm_et=133000000

def get_p(dt,stk,tm_st,tm_et):
    dt=str(dt)
    y=dt[:4]
    m=dt[4:6]
    d=dt[6:]
    path='/home/jovyan/data/store/stock/data_/second_3'+'/year='+str(y)+'/month='+str(m)+'/date='+str(d)+'/'+stk.lower()+'.parquet'

    df_sec=pd.read_parquet(path, engine='pyarrow').reset_index(drop=True)
    t1=df_sec[df_sec.Time>=tm_et].iloc[0,:].to_dict()
    t2=df_sec[df_sec.Time>=tm_st].iloc[0,:].to_dict()
    accT=t1['AccTurnover']-t2['AccTurnover']
    

    return dt,stk,accT


# In[4]:


tm_st=93000000
tm_et=94000000


# In[6]:


#get_p(dt,stk,tm_st,tm_et)


# In[ ]:


tp_unq=现有持仓[['日期','Instrument']].drop_duplicates()

cs=[]
for k in tp_unq.index:
    v=tp_unq.loc[k,:]
    dt=v['日期'].strftime('%Y%m%d')
    stk=v['Instrument'][-6:]+'.'+v['Instrument'][:2].lower()
    cs.append((dt,stk))

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
        p.apply_async(get_p, (v[0],v[1],tm_st,tm_et,),callback=add)   #维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
    p.close() # 关闭进程池
    p.join()
get_pool(30,cs)
tp=[v for k,v in 数据.items()]
tp=pd.DataFrame(tp,columns=['date','code','accT'])
tp.loc[:,'日期']=pd.to_datetime(tp.date.astype(str))
tp.loc[:,'Instrument']=tp.code.apply(lambda x:'SHSE.S+'+x[:6] if x.endswith('.sh') else 'SZSE.S+'+x[:6])


# In[ ]:


现有持仓=现有持仓.merge(tp.iloc[:,-3:],how='left')
现有持仓.groupby(['账户','策略']).accT.agg(['mean','median'])*0.2


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




