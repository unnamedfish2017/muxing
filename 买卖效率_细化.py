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


#ods_all[ods_all.日期==ods_all.日期.max()].sort_values(by='TradedVolume')


# In[3]:


ods_all[ods_all.日期==ods_all.日期.max()].ProductName.unique()


# In[4]:


#ods_all[ods_all.日期==ods_all.日期.max()].groupby(['日期','ProductName','StrategyName','BuySell']).Amt.sum()


# In[5]:


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
    p_st=t1['Open']
    avg_p=(t2['AccTurnover']-t1['AccTurnover'])/(t2['AccVolume']-t1['AccVolume'])
    
    try:
        lp=df_sec[df_sec.Time<=92457000].AskPrice01.values[-1]
    except:
        lp=0
    return dt,stk,p_st,avg_p,lp

# ret=[]
# for k in tp_unq.index:
#     v=tp_unq.loc[k,:]
#     dt=v['日期'].strftime('%Y%m%d')
#     stk=v['Instrument'][-6:]+'.'+v['Instrument'][:2].lower()
#     ret.append(get_p(dt,stk,tm_st,tm_et))


# In[6]:


tm_st=93000000
tm_et=93300000
st=('EA','ET','E1','E3','E5')

flag_tm='1010'
tp=ods_all[(ods_all.StrategyName.str.startswith(st))&(ods_all.tm<=flag_tm)&(ods_all.ProductName!='QUANT_PAPER')]
tp=tp[tp.StrategyName!='EAW']
tp.loc[:,'StrategyName']=tp.loc[:,'StrategyName'].apply(lambda x:x.replace('XX2','')+'_').str.cat(tp.ProductName.str.lower())


# In[7]:


集合成交=tp[(tp.tm<='0926')&(tp.tm>='0924')].groupby(['StrategyName','ProductName','BuySell','日期']).Amt.sum().rename('集合成交').reset_index()


# In[8]:


集合成交.StrategyName.unique()


# In[9]:


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


# In[ ]:





# In[10]:


tp_hist=pd.read_parquet('历史标的开盘表现.parquet')
list_hist=list(tp_hist.日期.apply(lambda x:x.strftime('%Y%m%d')).str.cat(tp_hist.Instrument.apply(lambda x:x[-6:]+'.'+x[:2].lower())))


# In[11]:


# tp_hist=pd.DataFrame([],columns=tp_hist.columns)
# list_hist=[]


# In[12]:


t=tp[~tp.OrderRef.str.contains('Virtual')].groupby(['StrategyName','ProductName','BuySell','Instrument','日期','RequestDateTime'])
#t=tp[~tp.OrderRef.str.contains('Virtual')].groupby(['StrategyName','ProductName','BuySell','Instrument','日期'])
t1=(t.Amt.sum()/t.TradedVolume.sum()).rename('Price_traded').reset_index()
t2=t.Amt.sum().rename('Amt').reset_index()
t3=t.TradedVolume.sum().rename('TradedVolume').reset_index()
t4=t.RequestPrice.max().rename('RequestPrice').reset_index()
records=t1.merge(t2,how='left').merge(t3,how='left').merge(t4,how='left')
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


# In[13]:


tp=tp_hist.append(tp).reset_index(drop=True)


# In[14]:


# tm_st=132000000
# tm_et=133000000
# st='Ma'
# bs='Sell'
# tp=ods_all[(ods_all.StrategyName.str.startswith(st))&(ods_all.BuySell==bs)]

ret=records.merge(tp,how='left')
ret.loc[:,'perform_p']=ret.Price_traded/ret.p_st
ret.loc[:,'perform_q']=ret.Price_traded/ret.p_avg
ret=ret.dropna()


# In[15]:


ret[ret.日期==ret.日期.max()].rename(columns={'p_st':'开盘价','p_avg':'均价','perform_p':'相对开盘价','perform_q':'相对均价'}).ProductName.unique()


# In[16]:


ret.loc[ret.BuySell=='Buy','相对开盘价']=1-ret.perform_p
ret.loc[ret.BuySell=='Sell','相对开盘价']=ret.perform_p-1
ret.loc[ret.BuySell=='Buy','相对均价']=1-ret.perform_q
ret.loc[ret.BuySell=='Sell','相对均价']=ret.perform_q-1


# In[17]:


t0=ret.copy()
t0.loc[:,'StrategyName']='合计'
t0.loc[:,'ProductName']='合计'
for 账户 in ret.StrategyName.apply(lambda x:'_'.join(x.split('_')[1:])).unique():
    t1=ret[ret.StrategyName.str.endswith(账户)].copy()
    t1.loc[:,'StrategyName']=账户
    ret=ret.append(t1)

ret=ret.append(t0)
ret=ret.reset_index(drop=True)


# In[18]:


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
#output.to_excel('卖出效率_EIF_%s.xlsx'%str.upper(bs))


# In[19]:


output.loc[output.方向=='Buy',['千分之（相对开盘）','千分之（相对均价）']]=-output.loc[output.方向=='Buy',['千分之（相对开盘）','千分之（相对均价）']]


# In[20]:


output.shape,output.drop_duplicates(['日期','方向','策略','账户']).shape


# In[21]:


时间条件1=(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))<926)&(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))>=924)
时间条件2=(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))<950)&(ret.RequestDateTime.apply(lambda x:int(x.strftime('%H%M')))>=930)

卖价不对=(np.abs(np.round(ret.lp*0.999,2)-ret.RequestPrice.replace('',0).astype(float))>=0.01)
卖价能成交=(ret.lp>0)&(np.round(ret.lp*0.999,2)<=ret.p_st)&(ret.BuySell=='Sell')

买价不对=(np.abs(np.round(ret.lp*1.001,2)-ret.RequestPrice.replace('',0).astype(float))>=0.01)
买价能成交=(ret.lp>0)&(np.round(ret.lp*1.001,2)>=ret.p_st)&(ret.BuySell=='Buy')
统计日期条件=ret.日期>=pd.to_datetime('2023-08-15')


# In[22]:


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


# In[58]:


output


# In[60]:


output.loc[:,'相对均价tmp']=output.loc[:,'千分之（相对均价）']*output.loc[:,'交易额w']
output.loc[:,'相对开盘价tmp']=output.loc[:,'千分之（相对均价）']*output.loc[:,'交易额w']
t=output[output.日期>='2023-08-25'].groupby(['策略','方向','账户'])
t1=(t.相对均价tmp.sum()/t.交易额w.sum())
t1.name='相对均价'
t3=(t.相对开盘价tmp.sum()/t.交易额w.sum())
t3.name='相对开盘价'
t2=t.交易额w.mean()
t=pd.concat([t1,t2,t3],axis=1).reset_index()
t[~t.策略.str.contains('jn')].sort_values(by='相对均价').to_excel('tmp.xlsx')


# In[75]:


t=ret[(ret.日期>='2023-08-25')&(ret.ProductName!='合计')].groupby(['日期','BuySell','code'])
t=pd.concat([t.相对均价.mean(),t.相对开盘价.mean(),t.Amt.sum()],axis=1)


# In[76]:


t.loc[:,'seg']=pd.qcut(t.Amt.rank(),10,labels=range(1,11))
t.groupby('seg').相对均价.mean().plot.bar()


# In[77]:


t.groupby('seg').相对开盘价.mean().plot.bar()


# In[93]:


t[t.seg==10].sample()


# In[74]:


ret[(ret.code=='002347.sz')&(ret.日期==ret.日期.max())]


# In[ ]:




