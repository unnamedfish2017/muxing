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


#ods_all[ods_all.日期==ods_all.日期.max()].groupby(['日期','ProductName','StrategyName','BuySell']).Amt.sum()


# In[5]:


tm_st=93000000
tm_et=93300000
st=('EA','ET','E1')

flag_tm='1010'
tp=ods_all[(ods_all.StrategyName.str.startswith(st))&(ods_all.tm<=flag_tm)&(ods_all.ProductName!='QUANT_PAPER')]
tp=tp[tp.StrategyName!='EAW']
tp.loc[:,'StrategyName']=tp.loc[:,'StrategyName'].apply(lambda x:x.replace('XX2','')+'_').str.cat(tp.ProductName.apply(lambda x:x[:2].lower()))


# In[6]:


tp_hist=pd.read_parquet('历史标的开盘表现.parquet')
list_hist=list(tp_hist.日期.apply(lambda x:x.strftime('%Y%m%d')).str.cat(tp_hist.Instrument.apply(lambda x:x[-6:]+'.'+x[:2].lower())))


# In[7]:


# tp_hist=pd.DataFrame([],columns=tp_hist.columns)
# list_hist=[]


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
tp=pd.DataFrame(tp,columns=['date','code','p_st','p_avg'])
tp.loc[:,'日期']=pd.to_datetime(tp.date.astype(str))
tp.loc[:,'Instrument']=tp.code.apply(lambda x:'SHSE.S+'+x[:6] if x.endswith('.sh') else 'SZSE.S+'+x[:6])


# In[9]:


tp_hist.append(tp).reset_index(drop=True).to_parquet('历史标的开盘表现.parquet')


# In[10]:


tp=tp_hist.append(tp).reset_index(drop=True)


# In[11]:


# tm_st=132000000
# tm_et=133000000
# st='Ma'
# bs='Sell'
# tp=ods_all[(ods_all.StrategyName.str.startswith(st))&(ods_all.BuySell==bs)]



ret=records.merge(tp,how='left')
ret.loc[:,'perform_p']=ret.Price_traded/ret.p_st
ret.loc[:,'perform_q']=ret.Price_traded/ret.p_avg
ret=ret.dropna()
print((ret.perform_q*ret.Amt).sum()/ret.Amt.sum())


# In[12]:


ret[ret.日期==ret.日期.max()].rename(columns={'p_st':'开盘价','p_avg':'均价','perform_p':'相对开盘价','perform_q':'相对均价'}).ProductName.unique()


# In[13]:


ret.loc[ret.BuySell=='Buy','相对开盘价']=1-ret.perform_p
ret.loc[ret.BuySell=='Sell','相对开盘价']=ret.perform_p-1
ret.loc[ret.BuySell=='Buy','相对均价']=1-ret.perform_q
ret.loc[ret.BuySell=='Sell','相对均价']=ret.perform_q-1


# In[14]:


ret[ret.日期==ret.日期.max()].reset_index(drop=True).to_excel('最近成交效率统计.xlsx')


# In[15]:


ret[ret.日期==ret.日期.max()].groupby(['BuySell']).Amt.sum()/10000


# In[16]:


ret[ret.日期==ret.日期.max()].groupby(['BuySell']).apply(lambda x:(x['Amt']*x['相对均价']).sum()/x['Amt'].sum())*1000


# In[17]:


ret[ret.日期==ret.日期.max()].groupby(['BuySell']).apply(lambda x:(x['Amt']*x['相对开盘价']).sum()/x['Amt'].sum())*1000


# In[18]:


#ret[ret.日期==ret.日期.max()].groupby(['StrategyName','BuySell']).apply(lambda x:(x['Amt']*x['相对均价']).sum()/x['Amt'].sum())*1000


# In[19]:


#ret[ret.日期>='2023-07-28'].groupby(['日期','ProductName','StrategyName','BuySell']).Amt.sum()


# In[20]:


#ret.loc[:,'StrategyName']=ret.loc[:,'StrategyName'].apply(lambda x:x.replace('XX2','')).str.cat(ret.ProductName.apply(lambda x:'_'+x[:2].lower()))


# In[21]:


output=[]
for v,tp in ret.groupby(['StrategyName','BuySell','日期']):
    #print(v,tp.Amt.sum(),(tp.perform_q*tp.Amt).sum()/tp.Amt.sum())
    output.append((v[-1],v[-2],v[0],v[1],tp.Amt.sum(),(tp.perform_p*tp.Amt).sum()/tp.Amt.sum(),(tp.perform_q*tp.Amt).sum()/tp.Amt.sum()))
output=pd.DataFrame(output,columns=['日期','方向','策略','产品','交易额w','交易效率p','交易效率q'])
output.loc[:,'千分之（相对开盘）']=np.round((output.交易效率p-1)*1000,2)
output.loc[:,'千分之（相对均价）']=np.round((output.交易效率q-1)*1000,2)
output.loc[:,'交易额w']=np.round(output.loc[:,'交易额w']/10000,1).astype(int)
output.sort_values(by=['交易额w'],ascending=False,inplace=True)
#output.to_excel('卖出效率_EIF_%s.xlsx'%str.upper(bs))


# In[22]:


output=output[output.交易额w>0]


# In[23]:


output.loc[output.方向=='Buy',['千分之（相对开盘）','千分之（相对均价）']]=-output.loc[output.方向=='Buy',['千分之（相对开盘）','千分之（相对均价）']]


# In[24]:


output[(output.日期=='2023-08-04')&(output.策略=='EALG98A_hc')]


# In[25]:


# 分析名称='指增换仓绩效分析_卖出'
# ret_all=output[output.方向=='Sell'].set_index(['日期','策略']).sort_values(by='交易额w')[[ '交易额w','千分之（相对开盘）','千分之（相对均价）']].stack().reset_index()
# ret_all.columns=['日期','A属性','B属性','取值']

# tp=ret_all[['日期','A属性','B属性','取值']]
# tp.loc[:,'分析名称']=分析名称
# path='..//产出//%s//%s.xlsx'%(分析名称,dt)
# tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
# tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)


# In[26]:


# import requests

# url = "https://61.172.245.225:26829/profit-mgt/api/v1/posthours-analysis/import"
# payload = {}
# files = [('file',
#           (path.split('/')[-1], open(path, 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))]
# headers = {
#     'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
#     "User-Agent": "curl/7.78.0"
#            }
# response = requests.request("POST", url, headers=headers, data=payload, files=files, verify=False)
# print(response.text)


# In[27]:


# 分析名称='指增换仓绩效分析_买入'
# ret_all=output[output.方向=='Buy'].set_index(['日期','策略']).sort_values(by='交易额w')[[ '交易额w','千分之（相对开盘）','千分之（相对均价）']].stack().reset_index()
# ret_all.columns=['日期','A属性','B属性','取值']

# tp=ret_all[['日期','A属性','B属性','取值']]
# tp.loc[:,'分析名称']=分析名称
# path='..//产出//%s//%s.xlsx'%(分析名称,dt)
# tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
# tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)

# # import requests

# # url = "https://61.172.245.225:26829/profit-mgt/api/v1/posthours-analysis/import"
# # payload = {}
# # files = [('file',
# #           (path.split('/')[-1], open(path, 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))]
# # headers = {
# #     'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
# #     "User-Agent": "curl/7.78.0"
# #            }
# # response = requests.request("POST", url, headers=headers, data=payload, files=files, verify=False)
# # print(response.text)


# In[28]:


ret_all1=output[output.方向=='Buy'].set_index(['日期','策略']).sort_values(by='交易额w')[[ '交易额w','千分之（相对开盘）','千分之（相对均价）']].stack().reset_index()
ret_all1.columns=['日期','A属性','B属性','取值']
ret_all1.B属性=ret_all1.B属性.apply(lambda x:'买'+x)
ret_all2=output[output.方向=='Sell'].set_index(['日期','策略']).sort_values(by='交易额w')[[ '交易额w','千分之（相对开盘）','千分之（相对均价）']].stack().reset_index()
ret_all2.columns=['日期','A属性','B属性','取值']
ret_all2.B属性=ret_all2.B属性.apply(lambda x:'卖'+x)
ret_all=ret_all1.append(ret_all2)

tp=ret_all.pivot(index=['日期','A属性'],columns='B属性',values='取值').fillna(0)
tp.loc[:,'总金额']=tp.买交易额w+tp.卖交易额w
#tp.loc[:,'合计千分之（相对开盘）']=np.round((tp.loc[:,'买千分之（相对开盘）']*tp.loc[:,'买交易额w']+tp.loc[:,'卖千分之（相对开盘）']*tp.loc[:,'卖交易额w'])/tp.总金额*2.,2)
#tp.loc[:,'合计千分之（相对均价）']=np.round((tp.loc[:,'买千分之（相对均价）']*tp.loc[:,'买交易额w']+tp.loc[:,'卖千分之（相对均价）']*tp.loc[:,'卖交易额w'])/tp.总金额*2.,2)

tp.loc[:,'合计千分之（相对开盘）']=np.round((tp.loc[:,'买千分之（相对开盘）']+tp.loc[:,'卖千分之（相对开盘）']),2)
tp.loc[:,'合计千分之（相对均价）']=np.round((tp.loc[:,'买千分之（相对均价）']+tp.loc[:,'卖千分之（相对均价）']),2)
ret3=tp.stack().reset_index()
ret3.columns=['日期','A属性','B属性','取值']
ret3=ret3[ret3.B属性.isin(['合计千分之（相对开盘）','合计千分之（相对均价）'])]

ret_all=ret_all.append(ret3)


分析名称='指增换仓绩效分析'

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


# In[30]:


ret_all[ret_all.A属性.str.startswith('EAHS98T')]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




