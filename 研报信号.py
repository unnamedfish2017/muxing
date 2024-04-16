#!/usr/bin/env python
# coding: utf-8

# In[1]:


import warnings
warnings.filterwarnings("ignore")
import pickle,os
import pandas as pd
import numpy as np
path='/home/jovyan/work/commons/data/daily_data/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
for v in ['closew','openw','highw','loww','amtw','WA_names_cn']:
    exec(v+'=data_daily[\''+v+'\']')
import pandas as pd


# In[2]:


df=pd.read_parquet('/home/jovyan/data/store/rsync/factor_data/report_new.parquet')
df=df[(df.reportSubType=='公司深度')&(~pd.isna(df.secCode))]
df.loc[:,'code']=df.loc[:,'secCode'].apply(lambda x:x+'.sh' if x[0]=='6' else x+'.bj' if x[0]=='8' else x+'.sz')
df=df[df.code.isin(closew.columns)]
df.loc[:,'publishDate']=pd.to_datetime(df.publishDate)
df.loc[:,'rptInsertTime']=pd.to_datetime(df.updateTime)
df.loc[:,'date']=df.loc[:,'rptInsertTime']
dts=list(closew.index)
dt=df.date.values[-1]
def next_trd_dt(dt):
    t=np.nan
    try:
        t=[i for i in dts if i>dt][0]
    except:
        pass
    return t
def pre_trd_dt(dt):
    t=np.nan
    try:
        t=[i for i in dts if i<=dt][-1]
    except:
        pass
    return t
df.loc[:,'publishDate_nxt_day']=df.loc[:,'publishDate'].apply(lambda x:next_trd_dt(x))
df.loc[:,'publishDate_pre_day']=df.loc[:,'publishDate'].apply(lambda x:pre_trd_dt(x))
df.loc[:,'rptInsertTime_nxt_day']=df.loc[:,'rptInsertTime'].apply(lambda x:next_trd_dt(x))
tp=df.sort_values(by='date')#.drop_duplicates(subset=['orgName','code'])
tp=tp[tp.date>=pd.to_datetime('20170101')]
tp.loc[:,'公告次日']=tp.loc[:,'rptInsertTime_nxt_day']
tp=tp[tp.publishDate_nxt_day==tp.rptInsertTime_nxt_day]#[tp.publishDate>tp.publishDate_pre_day]#[tp.publishDate<tp.rptInsertTime]
tp.loc[:,'year']=tp.公告次日.apply(lambda x:x.year)
tp.loc[:,'hour']=tp.rptInsertTime.apply(lambda x:x.hour)
ind=(pd.to_datetime(tp.rptInsertTime.apply(lambda x:str(x)[:10]))==tp.publishDate_pre_day)& (tp.rptInsertTime.apply(lambda x:x.hour)<15)
tp=tp[~ind]
strategy_pool=tp


# In[3]:


dfraw1=strategy_pool[['publishDate_pre_day','code']]
dfraw1.columns=['date','code']
dfraw1.loc[:,'信号名称']='券商研报'


# In[4]:


tp1=pd.read_csv('/home/jovyan/data/store/rsync/data_daily//申万行业股票列表2022_renew.csv',encoding='gbk')
#tp=tp[['code','申万一级','申万二级']].rename(columns={'code':'股票代码'})
dfraw1=dfraw1.merge(tp1,how='left').rename(columns={'name':'标的名称','申万一级':'申万一级行业','申万二级':'申万二级行业'})


# In[5]:


def get_base(tkpath,index_code):
    import tushare as ts
    tk=pd.read_csv(tkpath).columns[0]
    ts.set_token(tk)
    pro = ts.pro_api()
    dfbase = pro.index_daily(ts_code=index_code).sort_values(by='trade_date')
    dfbase.trade_date=pd.to_datetime(dfbase.trade_date.astype(str))
    dfbase=dfbase.sort_values(by='trade_date').set_index('trade_date')
    ybase=(dfbase['open'].shift(-6)/dfbase['open'].shift(-1)-1).reset_index()
    ybase.columns=['日期','收益base']
    dfbase=dfbase.reset_index()
    return dfbase
tkpath='/home/jovyan/work/commons/data/daily_data/tk.txt'
base_index='000985.SH'
dfbase=get_base(tkpath,base_index).set_index('trade_date')[['open']]


# In[6]:


closew_ffill=closew.ffill()
openw[pd.isna(openw)]=closew_ffill.shift(1)[pd.isna(openw)]


# In[7]:


for i in [1,3,5,20]:
    tp=openw.shift(-i-1).ffill()/openw.shift(-1).ffill()-1
    tp=tp.stack().rename('%s日收益'%str(i)).reset_index()
    tp0=(dfbase.shift(-i-1)/dfbase.shift(-1)-1).reset_index()
    tp0.columns=['date','bench']
    tp=tp.merge(tp0,how='left')
    tp.loc[:,'%s日超额'%str(i)]=tp.loc[:,'%s日收益'%str(i)]-tp.bench
    del tp['bench']
    dfraw1=dfraw1.merge(tp,how='left')


# In[8]:


clms='信号名称,标的代码,信号日期,信号类型,标的名称,申万一级行业,申万二级行业,1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')


# In[9]:


dfraw1.loc[:,'信号类型']='事件驱动'
dfraw1=dfraw1.rename(columns={'date':'信号日期','code':'标的代码'})[clms]
dfraw1=dfraw1[dfraw1.信号日期<closew.index[-1]]
dfraw1.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')]=np.round(dfraw1.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')]*100,1)


# In[10]:


path='..//产出//信号看板//券商研报.csv'
dfraw1[dfraw1.信号日期>='2023-01-01'].to_csv(path,index=False,encoding='utf-8')


# In[11]:


dfraw1[dfraw1.信号名称=='券商研报'].loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')].median()


# In[12]:


dfraw1[dfraw1.信号名称=='券商研报'].groupby('信号日期')['20日收益'].mean().cumsum().plot()


# In[13]:


import requests

url = "https://61.172.245.225:26829/signal-board/api/v1/public/import/raw"
headers = {
    'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
}
data = {
    'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
}
files = { 'uploadFile': (path.split('/')[-1], open(path, 'r')) }
response = requests.post(url, headers=headers, data=data, files=files,verify=False)
files['uploadFile'][1].close()
response


# In[14]:


dfraw1.信号日期.max()


# In[ ]:




