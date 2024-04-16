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


ods_all[ods_all.日期==ods_all.日期.max()].ProductName.unique()


# In[3]:


ods_all[ods_all.日期==ods_all.日期.max()].groupby(['日期','ProductName','StrategyName','BuySell']).Amt.sum()


# In[4]:


# tm_st=132000000
# tm_et=133000000

def get_p(dt,stk,tm_st,tm_et):
    dt=str(dt)
    y=dt[:4]
    m=dt[4:6]
    d=dt[6:]
    path='/home/jovyan/data/store/stock/data_/second_3'+'/year='+str(y)+'/month='+str(m)+'/date='+str(d)+'/'+stk.lower()+'.parquet'

    df_sec=pd.read_parquet(path, engine='pyarrow').reset_index(drop=True)
    amt_25=0
    amt_30=0
    amt_33=0
    try:
        amt_25=df_sec[(df_sec.Open>0)&(df_sec.Time<93000000)]['AccTurnover'].values[0]/10000
    except:
        pass
    try:
        amt_30=df_sec[(df_sec.Open>0)&(df_sec.Time>=92600000)&(df_sec.Time<=93100000)]['AccTurnover'].values[0]/10000-amt_25
    except:
        pass
    try:
        amt_33=df_sec[(df_sec.Open>0)&(df_sec.Time<=93300000)]['AccTurnover'].values[-1]/10000-amt_30
    except:
        pass
    return dt,stk,amt_25,amt_30,amt_33

# ret=[]
# for k in tp_unq.index:
#     v=tp_unq.loc[k,:]
#     dt=v['日期'].strftime('%Y%m%d')
#     stk=v['Instrument'][-6:]+'.'+v['Instrument'][:2].lower()
#     ret.append(get_p(dt,stk,tm_st,tm_et))


# In[5]:


tm_st=93000000
tm_et=93300000
st=('EA','ET','E1')

flag_tm='0945'
tp=ods_all[(ods_all.StrategyName.str.startswith(st))&(ods_all.tm<=flag_tm)&(ods_all.ProductName!='QUANT_PAPER')]
tp=tp[tp.StrategyName!='EAW']


# In[6]:


tp_hist=pd.read_parquet('历史开盘流动性.parquet')
list_hist=list(tp_hist.日期.apply(lambda x:x.strftime('%Y%m%d')).str.cat(tp_hist.Instrument.apply(lambda x:x[-6:]+'.'+x[:2].lower())))


# In[7]:


tp_hist=pd.DataFrame([],columns=tp_hist.columns)
list_hist=[]


# In[8]:


t=tp.groupby(['StrategyName','ProductName','BuySell','Instrument','日期'])
t1=(t.Amt.sum()/t.TradedVolume.sum()).rename('Price_traded').reset_index()
t2=t.Amt.sum().rename('Amt').reset_index()
records=t1.merge(t2,how='left')
records.loc[:,'月份']=records.日期.apply(lambda x:x.strftime('%Y%m'))
tp_unq=records[['日期','Instrument']].drop_duplicates()

cs=[]
for k in tp_unq.index:
    v=tp_unq.loc[k,:]
    dt=v['日期'].strftime('%Y%m%d')
    stk=v['Instrument'][-6:]+'.'+v['Instrument'][:2].lower()
    if dt+stk not in list_hist:
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
tp=pd.DataFrame(tp,columns=['date','code','amt_25','amt_30','amt_33'])
tp.loc[:,'日期']=pd.to_datetime(tp.date.astype(str))
tp.loc[:,'Instrument']=tp.code.apply(lambda x:'SHSE.S+'+x[:6] if x.endswith('.sh') else 'SZSE.S+'+x[:6])


# In[9]:


tp_hist.append(tp).reset_index(drop=True).to_parquet('历史开盘流动性.parquet')


# In[10]:


tp=tp_hist.append(tp).reset_index(drop=True)


# In[11]:


# tm_st=132000000
# tm_et=133000000
# st='Ma'
# bs='Sell'
# tp=ods_all[(ods_all.StrategyName.str.startswith(st))&(ods_all.BuySell==bs)]



ret=records.merge(tp,how='left')


# In[12]:


ret


# In[13]:


ret.reset_index(drop=True).to_excel('最近开盘流动性统计.xlsx')


# In[14]:


#ret[ret.日期>='2023-07-28'].groupby(['日期','ProductName','StrategyName','BuySell']).Amt.sum()


# In[ ]:





# In[15]:


output=[]
for v,tp in ret.groupby(['StrategyName','BuySell','日期']):
    #print(v,tp.Amt.sum(),(tp.perform_q*tp.Amt).sum()/tp.Amt.sum())
    t=(v[-1],v[-2],v[0],v[1],tp.Amt.sum(),tp.amt_25.median(),tp.amt_25.quantile(0.1),tp.amt_30.median(),tp.amt_30.quantile(.1),tp.amt_33.median(),tp.amt_33.quantile(.1))
    output.append(t)
output=pd.DataFrame(output,columns=['日期','方向','策略','产品','交易额w','竞价成交额中位数w','竞价成交额10分位w','30一跳成交额中位数w','30一跳成交额10分位w','33成交额中位数w','33成交额10分位w'])
output.loc[:,'交易额w']=np.round(output.loc[:,'交易额w']/10000,1).astype(int)
output.sort_values(by=['交易额w'],ascending=False,inplace=True)
for v in output.columns[-6:]:
    output.loc[:,v]=np.round(output.loc[:,v],1).astype(int)


# In[16]:


output=output[output.交易额w>0]


# In[17]:


output


# In[18]:


ret_all=output.set_index(['日期','策略']).sort_values(by='交易额w')[[ '方向','交易额w','竞价成交额中位数w','竞价成交额10分位w','30一跳成交额中位数w','30一跳成交额10分位w','33成交额中位数w','33成交额10分位w']].stack().reset_index()
ret_all.columns=['日期','A属性','B属性','取值']

ret_all=ret_all
分析名称='test'

tp=ret_all[['日期','A属性','B属性','取值']]
tp.loc[:,'分析名称']=分析名称
path='..//产出//%s//%s.xlsx'%(分析名称,dt)
tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)

import requests

url = "https://61.172.245.225:26829/profit-mgt/api/v1/posthours-analysis/import"
payload = {}
files = [('file',
          (path.split('/')[-1], open(path, 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))]
headers = {
    'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
    "User-Agent": "curl/7.78.0"
           }
response = requests.request("POST", url, headers=headers, data=payload, files=files, verify=False)
print(response.text)


# In[19]:


ret_all[ret_all.日期==pd.to_datetime('2023-07-28')]#.A属性.unique()


# In[ ]:




