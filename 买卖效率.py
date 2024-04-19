#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pickle
import pandas as pd
import numpy as np
import os,pickle
import pandas as pd

DATA_PATH = '/home/vscode/workspace/data/store/rsync'
root_sc='/home/vscode/workspace/work/生产/素材'
root_cd='/home/vscode/workspace/work/生产/脚本'
root_save='/home/vscode/workspace/work/生产/产出/票池/其他'

path=f'{DATA_PATH}/data_daily/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
closew = data_daily['closew'].fillna(method='ffill')
lst_trd_day=closew.index[-1].strftime('%Y%m%d')
root=f'{root_sc}/托管数据'

import sys
sys.path.append(root_cd)
from 报表函数 import *
ios_all,ods_all=get_records(root)

ods_all.loc[:,'日期']=pd.to_datetime(ods_all.DealDateTime.apply(lambda x:x.date()))
ods_all.loc[:,'TradedVolume']=ods_all.loc[:,'TradedVolume'].astype(float)
ods_all.loc[:,'Amt']=ods_all.loc[:,'TradedPrice'].astype(float)*ods_all.loc[:,'TradedVolume'].astype(float)
ods_all.loc[:,'tm']=ods_all.DealDateTime.apply(lambda x:x.strftime('%H%M'))
ods_all[ods_all.日期==ods_all.日期.max()].ProductName.unique()
ods_all[(ods_all.日期==ods_all.日期.max())&(ods_all.ProductName=='HX_XYSS')].sort_values(by='Amt')
ods_all[ods_all.日期==ods_all.日期.max()].groupby(['日期','ProductName','StrategyName','BuySell']).Amt.sum()


def get_p(dt,stk,tm_st,tm_et):
    dt=str(dt)
    y=dt[:4]
    m=dt[4:6]
    d=dt[6:]
    
    path='/home/vscode/workspace/data/store/stock/data_/second_3'+'/year='+str(y)+'/month='+str(m)+'/date='+str(d)+'/'+stk.lower()+'.parquet'

    df_sec=pd.read_parquet(path, engine='pyarrow').reset_index(drop=True)
    t1=df_sec[df_sec.Time>=tm_et].iloc[0,:].to_dict()
    t2=df_sec[df_sec.Time>=tm_st].iloc[0,:].to_dict()
    p_st=t1['Open']
    avg_p=(t2['AccTurnover']-t1['AccTurnover'])/(t2['AccVolume']-t1['AccVolume'])
    
    try:
        lp=df_sec[df_sec.Time<=92457000].AskPrice01.values[-1]
    except:
        lp=0
    return dt,stk,p_st,avg_p,lp

tm_st=93000000
tm_et=93300000
st=('EA','ET','E1','E3','EMIG','EMFA','EMHS','EMAL','EMHS','SISIG')

flag_tm='1010'
tp=ods_all[(ods_all.StrategyName.str.startswith(st))&(ods_all.tm<=flag_tm)&(ods_all.ProductName!='QUANT_PAPER')]
tp=tp[tp.StrategyName!='EAW']
tp.loc[:,'StrategyName']=tp.loc[:,'StrategyName'].apply(lambda x:x.replace('XX2','')+'_').str.cat(tp.ProductName.str.lower())

import re

算法列表=r'(FT_VWAP_AI_PLUS|HX_SMART_VWAP|VWAPPLUS|TWAPPLUS|VWAP|TWAP)'
def extract_and_match(s):
    pattern = re.compile(算法列表, re.IGNORECASE)
    match = pattern.search(s)
    
    if match:
        return match.group().upper()  # 将匹配到的结果转为大写
    else:
        return None  # 如果没有匹配到，返回 None
tp.loc[:,'algo']=tp.OrderRef.apply(lambda s:extract_and_match(s))


# In[11]:


tp.loc[(tp.ProductName.isin(['GT_XYSS'])|(tp.ProductName.str.startswith('HT_')))&(tp.algo=='VWAPPLUS'),'algo']='KF_VWAPPLUS'


tp.algo=tp.algo.str.cat(tp.ProductName,sep='_')


# In[13]:


集合成交=tp[(tp.tm<='0926')&(tp.tm>='0924')].groupby(['StrategyName','ProductName','BuySell','日期']).Amt.sum().rename('集合成交').reset_index()


# In[14]:


t0=集合成交.copy()
t0.loc[:,'StrategyName']='合计'
t0.loc[:,'ProductName']='合计'
for 账户 in 集合成交.StrategyName.apply(lambda x:'_'.join(x.split('_')[1:])).unique():
    t1=集合成交[集合成交.StrategyName.str.endswith(账户)].copy()
    t1.loc[:,'StrategyName']=账户
    集合成交=集合成交.append(t1)
集合成交=集合成交.append(t0)
集合成交=集合成交.reset_index(drop=True)
集合成交=集合成交.groupby(['StrategyName', 'ProductName', 'BuySell', '日期'])['集合成交'].sum().rename('集合成交').reset_index()


tp_hist=pd.read_parquet('/home/vscode/workspace/work/生产/产出/盘后分析/历史开盘表现/历史标的开盘表现.parquet')
list_hist=list(tp_hist.日期.apply(lambda x:x.strftime('%Y%m%d')).str.cat(tp_hist.Instrument.apply(lambda x:x[-6:]+'.'+x[:2].lower())))


# In[16]:


# tp_hist=pd.DataFrame([],columns=tp_hist.columns)
# list_hist=[]


# In[17]:


t=tp[~tp.OrderRef.str.contains('Virtual')].groupby(['StrategyName','ProductName','BuySell','Instrument','日期','RequestDateTime'])
#t=tp[~tp.OrderRef.str.contains('Virtual')].groupby(['StrategyName','ProductName','BuySell','Instrument','日期'])
t1=(t.Amt.sum()/t.TradedVolume.sum()).rename('Price_traded').reset_index()
t2=t.Amt.sum().rename('Amt').reset_index()
t3=t.TradedVolume.sum().rename('TradedVolume').reset_index()
t4=t.RequestPrice.max().rename('RequestPrice').reset_index()
t5=t.algo.max().rename('algo').reset_index()
records=t1.merge(t2,how='left').merge(t3,how='left').merge(t4,how='left').merge(t5,how='left')
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
tp=pd.DataFrame(tp,columns=['date','code','p_st','p_avg','lp'])
tp.loc[:,'日期']=pd.to_datetime(tp.date.astype(str))
tp.loc[:,'Instrument']=tp.code.apply(lambda x:'SHSE.S+'+x[:6] if x.endswith('.sh') else 'SZSE.S+'+x[:6])

tp_hist.append(tp).reset_index(drop=True).to_parquet('/home/vscode/workspace/work/生产/产出/盘后分析/历史开盘表现/历史标的开盘表现.parquet')



tp=tp_hist.append(tp).reset_index(drop=True)


# In[20]:


# tm_st=132000000
# tm_et=133000000
# st='Ma'
# bs='Sell'
# tp=ods_all[(ods_all.StrategyName.str.startswith(st))&(ods_all.BuySell==bs)]

ret=records.merge(tp,how='left')
ret.loc[:,'perform_p']=ret.Price_traded/ret.p_st
ret.loc[:,'perform_q']=ret.Price_traded/ret.p_avg
ret=ret.dropna(subset=['StrategyName','ProductName','Instrument','日期','RequestDateTime','TradedVolume'])

ret[ret.日期==ret.日期.max()].rename(columns={'p_st':'开盘价','p_avg':'均价','perform_p':'相对开盘价','perform_q':'相对均价'}).ProductName.unique()

ret.loc[ret.BuySell=='Buy','相对开盘价']=1-ret.perform_p
ret.loc[ret.BuySell=='Sell','相对开盘价']=ret.perform_p-1
ret.loc[ret.BuySell=='Buy','相对均价']=1-ret.perform_q
ret.loc[ret.BuySell=='Sell','相对均价']=ret.perform_q-1


#ret[ret.日期==ret.日期.max()].reset_index(drop=True).to_excel('最近成交效率统计.xlsx')



t0=ret.copy()
t0.loc[:,'StrategyName']='合计'
t0.loc[:,'ProductName']='合计'
for 账户 in ret.StrategyName.apply(lambda x:'_'.join(x.split('_')[1:])).unique():
    t1=ret[ret.StrategyName.str.endswith(账户)].copy()
    t1.loc[:,'StrategyName']=账户
    ret=ret.append(t1)

ret=ret.append(t0)
ret=ret.reset_index(drop=True)


# In[25]:


output=[]
存疑挂单=[]
for v,tp in ret.groupby(['StrategyName','BuySell','日期','ProductName']):
    #print(v,tp.Amt.sum(),(tp.perform_q*tp.Amt).sum()/tp.Amt.sum())
    t1=tp[(tp.RequestPrice.replace('',0).astype(float)>0)&(tp.lp>0)]
    存疑率=0
    if len(t1):
        t1=t1[(t1.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))<926)&(t1.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))>=924)]
    if len(t1):
        t1.loc[:,'flag']=0
        t1.loc[(np.abs(t1.lp-t1.RequestPrice.replace('',0).astype(float)-0.01)<=0.0001)&(t1.BuySell=='Sell')&(t1.日期<=pd.to_datetime('2023-08-15')),'flag']=1
        t1.loc[(np.abs(t1.lp-t1.RequestPrice.replace('',0).astype(float)+0.01)<=0.0001)&(t1.BuySell=='Buy')&(t1.日期<=pd.to_datetime('2023-08-15')),'flag']=1
        
        理论买价=t1.lp.apply(lambda x:max(x+0.01,np.round(x*1.001,2)))
        理论卖价=t1.lp.apply(lambda x:min(x-0.01,np.round(x*0.999,2)))
        t1.loc[(np.abs(理论卖价-t1.RequestPrice.replace('',0).astype(float))<=0.0001)&(t1.BuySell=='Sell')&(t1.日期>=pd.to_datetime('2023-08-15')),'flag']=1                                                                                                   
        t1.loc[(np.abs(理论买价-t1.RequestPrice.replace('',0).astype(float))<=0.0001)&(t1.BuySell=='Buy')&(t1.日期>=pd.to_datetime('2023-08-15')),'flag']=1                                                                                                   
        存疑挂单.append(t1[t1.flag==0])
        存疑率=1-t1.flag.mean()
    

    output.append((v[0],v[1],v[2],v[3],tp.Amt.sum(),                   ((tp.Price_traded-tp.p_st)*tp.TradedVolume).sum()/(tp.p_st*tp.TradedVolume).sum(),                   ((tp.Price_traded-tp.p_avg)*tp.TradedVolume).sum()/(tp.p_avg*tp.TradedVolume).sum(),                  存疑率,                  )
                 )
output=pd.DataFrame(output,columns=['策略','方向','日期','账户','交易额w','交易效率p','交易效率q','存疑率'])
output.loc[:,'千分之（相对开盘）']=np.round((output.交易效率p)*1000,2)
output.loc[:,'千分之（相对均价）']=np.round((output.交易效率q)*1000,2)

output=output.merge(集合成交.rename(columns={'StrategyName':'策略','ProductName':'账户','BuySell':'方向'}),how='left')
output.loc[:,'集合成交比例']=(output['集合成交'].fillna(0)/output.交易额w.replace(0,1)*100).astype(int)
output=output[output.交易额w>0]

output.loc[:,'交易额w']=np.round(output.loc[:,'交易额w']/10000,1).astype(int)
output.loc[:,'存疑率']=(output.存疑率*100).astype(int)
output.sort_values(by=['交易额w'],ascending=False,inplace=True)
output.loc[output.方向=='Buy',['千分之（相对开盘）','千分之（相对均价）']]=-output.loc[output.方向=='Buy',['千分之（相对开盘）','千分之（相对均价）']]
#output.to_excel('卖出效率_EIF_%s.xlsx'%str.upper(bs))


# In[ ]:





# In[26]:


output.shape,output.drop_duplicates(['日期','方向','策略','账户']).shape


# In[27]:


时间条件1=(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))<926)&(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))>=924)
时间条件2=(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))<950)&(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))>=930)

卖价不对=(np.abs(np.round(ret.lp*0.999,2)-ret.RequestPrice.replace('',0).astype(float))>=0.01)
卖价能成交=(ret.lp>0)&(np.round(ret.lp*0.999,2)<=ret.p_st)&(ret.BuySell=='Sell')

买价不对=(np.abs(np.round(ret.lp*1.001,2)-ret.RequestPrice.replace('',0).astype(float))>=0.01)
买价能成交=(ret.lp>0)&(np.round(ret.lp*1.001,2)>=ret.p_st)&(ret.BuySell=='Buy')
统计日期条件=ret.日期>=pd.to_datetime('2023-08-15')


# In[28]:


ret.loc[:,'flag2']=0
# ret.loc[(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))<926)&(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))>=924)\
#         &(ret.p_st>0)&(np.round(ret.lp*0.999,2)<=ret.p_st)&(np.abs(np.round(ret.lp*0.999,2)-ret.RequestPrice.replace('',0).astype(float))>=0.01)&(ret.BuySell=='Sell')&(ret.日期>=pd.to_datetime('2023-08-15')),'flag2']=1                                                                                                   
# ret.loc[(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))<926)&(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))>=924)\
#         &(ret.p_st>0)&(np.round(ret.lp*1.001,2)>=ret.p_st)&(np.abs(np.round(ret.lp*1.001,2)-ret.RequestPrice.replace('',0).astype(float))>=0.01)&(ret.BuySell=='Buy')&(ret.日期>=pd.to_datetime('2023-08-15')),'flag2']=1        
ret.loc[(时间条件2|时间条件1&卖价不对)&卖价能成交&统计日期条件,'flag2']=1
ret.loc[(时间条件2|时间条件1&买价不对)&买价能成交&统计日期条件,'flag2']=1

tp=ret[ret.flag2==1][['StrategyName','ProductName','BuySell','Instrument','日期']].drop_duplicates()
tp.loc[:,'flag_delay']=1
ret=ret.merge(tp,how='left')
ret.loc[:,'delay_profit']=(ret.loc[ret.flag_delay==1,'Price_traded']-ret.loc[ret.flag_delay==1,'p_st'])*ret.loc[ret.flag_delay==1,'TradedVolume']*ret.loc[ret.flag_delay==1,'BuySell'].apply(lambda x:-1 if x=='Buy' else 1)
tp=ret.groupby(['StrategyName','ProductName','BuySell','日期']).delay_profit.sum()/ret.groupby(['StrategyName','ProductName','BuySell','日期']).Amt.sum()
tp=np.round(tp*1000,2)
tp=tp.rename('千分之（延迟收益）').reset_index()
tp.columns=['策略','账户','方向','日期','千分之（延迟收益）']
output=output.merge(tp,how='left')

ret_all1=output[output.方向=='Buy'].set_index(['日期','策略']).sort_values(by='交易额w')[[ '交易额w','千分之（相对开盘）','千分之（相对均价）','集合成交比例','千分之（延迟收益）','存疑率']].stack().reset_index()
ret_all1.columns=['日期','A属性','B属性','取值']
ret_all1.B属性=ret_all1.B属性.apply(lambda x:'买'+x)
ret_all2=output[output.方向=='Sell'].set_index(['日期','策略']).sort_values(by='交易额w')[[ '交易额w','千分之（相对开盘）','千分之（相对均价）','集合成交比例','千分之（延迟收益）','存疑率']].stack().reset_index()
ret_all2.columns=['日期','A属性','B属性','取值']
ret_all2.B属性=ret_all2.B属性.apply(lambda x:'卖'+x)
ret_all=ret_all1.append(ret_all2)

tp=ret_all.pivot(index=['日期','A属性'],columns='B属性',values='取值').fillna(0)
tp.loc[:,'总金额']=tp.买交易额w+tp.卖交易额w
tp.loc[:,'合计千分之（相对开盘）']=np.round((tp.loc[:,'买千分之（相对开盘）']*tp.loc[:,'买交易额w']+tp.loc[:,'卖千分之（相对开盘）']*tp.loc[:,'卖交易额w'])/tp.总金额*1.,2)
tp.loc[:,'合计千分之（相对均价）']=np.round((tp.loc[:,'买千分之（相对均价）']*tp.loc[:,'买交易额w']+tp.loc[:,'卖千分之（相对均价）']*tp.loc[:,'卖交易额w'])/tp.总金额*1.,2)

# tp.loc[:,'合计千分之（相对开盘）']=np.round((tp.loc[:,'买千分之（相对开盘）']+tp.loc[:,'卖千分之（相对开盘）']),2)
# tp.loc[:,'合计千分之（相对均价）']=np.round((tp.loc[:,'买千分之（相对均价）']+tp.loc[:,'卖千分之（相对均价）']),2)
tp.loc[:,'合计千分之（延迟收益）']=np.round((tp.loc[:,'买千分之（延迟收益）']+tp.loc[:,'卖千分之（延迟收益）']),2)
ret3=tp.stack().reset_index()
ret3.columns=['日期','A属性','B属性','取值']
ret3=ret3[ret3.B属性.isin(['合计千分之（相对开盘）','合计千分之（相对均价）'])]

ret_all=ret_all.append(ret3)

分析名称='指增换仓绩效分析'

tp=ret_all[['日期','A属性','B属性','取值']]
tp=tp[tp.日期>=closew.index[-5]]
tp.loc[:,'分析名称']=分析名称
path='/home/vscode/workspace/work/生产/产出/盘后分析//%s//%s.xlsx'%(分析名称,closew.index[-1].strftime('%Y%m%d'))
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


# In[33]:


tp=ods_all[(ods_all.tm<='0926')&(ods_all.tm>='0924')&(ods_all.TradedVolume>0)&(~ods_all.AccountName.str.contains('Paper'))].drop_duplicates(['日期','Instrument','BuySell']).groupby(['日期','Instrument']).BuySell.count().rename('自成交').reset_index()
tp=tp[tp.自成交>1]
zcj=ods_all[(ods_all.tm<='0926')&(ods_all.tm>='0924')&(ods_all.TradedVolume>0)&(~ods_all.AccountName.str.contains('Paper'))].merge(tp).sort_values(by=['日期','Instrument','BuySell'])


# In[34]:


zcj[zcj.日期=='2023-08-30'].to_excel('自成交.xlsx')


# In[35]:


zcj.Amt.quantile(.7)


# In[36]:


ods_all.loc[:,'r_tm']=ods_all.RequestDateTime.apply(lambda x:x.strftime('%H%M'))
t=ods_all.groupby(['日期','r_tm','ProductName']).RequestDateTime.count().reset_index()
t[t.r_tm=='0924']


# In[37]:


t=ret_all[ret_all.A属性=='合计'].set_index(['日期','B属性'])[['取值']].unstack()['取值']
t.loc[:,['合计千分之（相对均价）','合计千分之（相对开盘）']].tail(20).median()

output[(output.策略.str.contains('合计'))]
tp=output[(output.策略.isin(['hc_szlh','ky_cy','gt_xyss','gl_sz_alpha','hx_xyss','ht_ss']))&(output.日期>=closew.index[-5])]

t1=tp.groupby('策略').apply(lambda x:(x.交易额w*x['千分之（相对均价）']).sum())/tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
t2=tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
t=pd.concat([t1,t2],axis=1)
t.columns=['绩效','交易额w']

t1=tp.groupby('策略').apply(lambda x:(x.交易额w*x['千分之（相对开盘）']).sum())/tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
t2=tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
t=pd.concat([t1,t2],axis=1)
t.columns=['绩效','交易额w']

# ################################绩效临时

# In[45]:


dts=list(closew.index)
dts_tp=[i for i in dts if i>=pd.to_datetime('2023-12-01')]
tp=output[(output.策略.isin(['hc_szlh','ky_cy','gt_xyss','gl_sz_alpha','hx_xyss','ht_ss','ht_mws']))&(output.日期.isin(dts_tp))]
t1=tp.groupby('策略').apply(lambda x:(x.交易额w*x['千分之（相对均价）']).sum())/tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
t2=tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
t=pd.concat([t1,t2],axis=1)
t.columns=['绩效','交易额w']


# ##############################20日绩效

# In[46]:


dts=list(closew.index)
ret_all=[]
for dt in dts[-80:]:
    k=dts.index(dt)
    dts_tp=dts[k-19:k+1]
    
    tp=output[(output.策略.isin(['hc_szlh','ky_cy','gt_xyss','gl_sz_alpha','hx_xyss','ht_ss','ht_mws']))&(output.日期.isin(dts_tp))]
    t1=tp.groupby('策略').apply(lambda x:(x.交易额w*x['千分之（相对均价）']).sum())/tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
    t2=tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
    t=pd.concat([t1,t2],axis=1)
    t.columns=['绩效','交易额w']
    ret_hz=t.reset_index().iloc[:,:2]
    ret_hz.loc[:,'日期']=dt
    ret_all.append(ret_hz)
ret_hz=pd.concat(ret_all)

ret_hz.columns=['B属性','取值','日期']
ret_hz.loc[:,'A属性']=''
ret_hz.取值=np.round(ret_hz.取值,2)
分析名称='换仓绩效20日平均'
tp=ret_hz[['日期','A属性','B属性','取值']]
tp.loc[:,'分析名称']=分析名称
path='/home/vscode/workspace/work/生产/产出/盘后分析//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
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


# ##############################算法绩效

# In[47]:


t1=ret[ret.日期>=closew.index[-10]].groupby('algo')
t2=t1.apply(lambda x:(x.Amt*x.相对均价).sum()/(x.Amt.sum()))
t3=t1.apply(lambda x:x.Amt.sum())//10000
pd.concat([t2,t3],axis=1)


# ##############################5日绩效

# In[48]:


# dts=list(closew.index)
# ret_all=[]
# for dt in dts[-60:]:
#     k=dts.index(dt)
#     dts_tp=dts[k-4:k+1]
    
#     tp=output[(output.策略.isin(['hc_szlh','ky_cy','gt_xyss','gl_sz_alpha','hx_xyss','ht_ss','ht_mws']))&(output.日期.isin(dts_tp))]
#     t1=tp.groupby('策略').apply(lambda x:(x.交易额w*x['千分之（相对均价）']).sum())/tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
#     t2=tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
#     t=pd.concat([t1,t2],axis=1)
#     t.columns=['绩效','交易额w']
#     ret_hz=t.reset_index().iloc[:,:2]
#     ret_hz.loc[:,'日期']=dt
#     ret_all.append(ret_hz)
# ret_hz=pd.concat(ret_all)


# In[49]:


# ret_hz.columns=['B属性','取值','日期']
# ret_hz.loc[:,'A属性']=''
# ret_hz.取值=np.round(ret_hz.取值,2)
# 分析名称='换仓绩效5日平均'
# tp=ret_hz[['日期','A属性','B属性','取值']]
# tp.loc[:,'分析名称']=分析名称
# path='/home/vscode/workspace/work/生产/产出/盘后分析//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
# tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
# tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)

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


# In[50]:


#ods_all[(ods_all.日期==ods_all.日期.max())&(ods_all.tm>='0950')&(ods_all.tm<='0959')]


# In[51]:


#ods_all[(ods_all.日期==ods_all.日期.max())&(ods_all.tm>='0926')&(ods_all.ProductName=='GL_SZ_ALPHA')&(~pd.isna(ods_all.RequestPrice))].sort_values(by=['ProductName','tm'])#.groupby('tm').tm.count()


# In[52]:


# base_index='000985.SH'
# import pandas as pd
# tp=pd.read_parquet('/home/jovyan/data/store/rsync/data_daily/指数成分/%s.parquet'%base_index)
# base_pool=list(tp[tp.date==tp.date.max()].code.str.lower())

# t1=closew.tail(1).T.dropna()
# t2=closew.shape[1]-closew.count()
# t3=[i for i in t1.index if i in t2.index and not i.endswith('.bj')]
# len(t3)

# DATA_PATH = '/home/jovyan/data/store/rsync/'
# stlist=list(pd.read_csv(f'{DATA_PATH}/data_daily/STlist.csv').代码)
# t4=set(t3)-set(base_pool)-set(stlist)
# t4


# ##########################报单时间占比

# In[53]:


ods_all.loc[:,'r_tm']=ods_all.RequestDateTime.apply(lambda x:x.strftime('%H%M'))
t1=ods_all[(ods_all.日期==ods_all.日期.max())].groupby('ProductName').Amt.sum()
t2=ods_all[(ods_all.日期==ods_all.日期.max())&(ods_all.tm>='0940')&(ods_all.tm<='0959')].groupby('ProductName').Amt.sum()


ods_all.loc[:,'r_tm']=ods_all.RequestDateTime.apply(lambda x:x.strftime('%H%M'))
t1=ods_all[(ods_all.日期==pd.to_datetime('20231124'))].groupby('ProductName').Amt.sum()
t2=ods_all[(ods_all.日期==pd.to_datetime('20231124'))&(ods_all.tm>='0940')&(ods_all.tm<='0959')].groupby('ProductName').Amt.sum()


# ##########################T0收益监控

# In[60]:


tp=ods_all[ods_all.T0_MIYUAN==1].groupby(['日期','ProductName','StrategyName','Instrument','BuySell']).apply(lambda x:(x['TradedPrice'].astype(float)*x['Amt'].astype(int)).sum()/x['Amt'].sum())
p=tp.rename('p')#.reset_index()

tp=ods_all[ods_all.T0_MIYUAN==1].groupby(['日期','ProductName','StrategyName','Instrument','BuySell']).apply(lambda x:x['TradedVolume'].sum())
v=tp.rename('v').reset_index()

v=v.groupby(['日期','ProductName','StrategyName','Instrument']).v.mean().reset_index()


# In[61]:


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


# In[62]:


tp.delta.sum()/tp.amt.sum(),tp.amt.sum()/10000


# In[63]:


#ods_all[ods_all.T0_MIYUAN==1].to_parquet('/home/jovyan/work/commons/T0_orders.parquet')
