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
tp=pd.read_parquet('/home/jovyan/data/store/rsync/FinancialData/RebuyData.parquet')
tp=tp.drop_duplicates(['ts_code','ann_date']).rename(columns={'ts_code':'code'})
tp.code=tp.code.str.lower()
dts=[i.strftime('%Y%m%d') for i in closew.index[-20:]]


# In[2]:


t1=(closew>closew.shift(4)).astype(int)
t2=t1.apply(lambda col: col.groupby((col != col.shift()).cumsum()).cumsum(), axis=0)

t1=(closew<closew.shift(4)).astype(int)
t3=t1.apply(lambda col: col.groupby((col != col.shift()).cumsum()).cumsum(), axis=0)
t2=t2.stack().rename('九转卖SetUp')
t3=t3.stack().rename('九转买SetUp')

t2=t2[t2==9].reset_index()
t2.loc[:,'信号名称']='九转卖SetUp'
t3=t3[t3==9].reset_index()
t3.loc[:,'信号名称']='九转买SetUp'
dfraw1=t2[['date','code','信号名称']].append(t3[['date','code','信号名称']])
dfraw1=dfraw1[~dfraw1.code.str.endswith('.bj')]


# In[ ]:





# In[3]:


tp1=pd.read_csv('/home/jovyan/data/store/rsync/data_daily//申万行业股票列表2022_renew.csv',encoding='gbk')
#tp=tp[['code','申万一级','申万二级']].rename(columns={'code':'股票代码'})
dfraw1=dfraw1.merge(tp1,how='left').rename(columns={'name':'标的名称','申万一级':'申万一级行业','申万二级':'申万二级行业'})


# In[4]:


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
dfbase_c=get_base(tkpath,base_index).set_index('trade_date')[['close']]


# In[5]:


closew_ffill=closew.ffill()
openw[pd.isna(openw)]=closew_ffill.shift(1)[pd.isna(openw)]


# In[6]:


for i in [1,3,5,20]:
    tp=openw.shift(-i-1).ffill()/openw.shift(-1).ffill()-1
    tp=tp.stack().rename('%s日收益'%str(i)).reset_index()
    tp0=(dfbase.shift(-i-1)/dfbase.shift(-1)-1).reset_index()
    tp0.columns=['date','bench']
    tp=tp.merge(tp0,how='left')
    tp.loc[:,'%s日超额'%str(i)]=tp.loc[:,'%s日收益'%str(i)]-tp.bench
    del tp['bench']
    dfraw1=dfraw1.merge(tp,how='left')
    
for i in [1,3,5,20]:
    tp=closew.shift(-i).ffill()/closew.ffill()-1
    tp=tp.stack().rename('%s日收益_c'%str(i)).reset_index()
    tp0=(dfbase_c.shift(-i)/dfbase_c-1).reset_index()
    tp0.columns=['date','bench']
    tp=tp.merge(tp0,how='left')
    tp.loc[:,'%s日超额_c'%str(i)]=tp.loc[:,'%s日收益_c'%str(i)]-tp.bench
    del tp['bench']
    dfraw1=dfraw1.merge(tp,how='left')


# In[7]:


dfraw1[dfraw1.信号名称=='九转卖SetUp'][['1日超额','1日超额_c']].mean()


# In[8]:


dfraw1[dfraw1.信号名称=='九转卖SetUp'].groupby('date')[['1日超额','1日超额_c']].mean().cumsum().plot()


# In[9]:


dfraw1[dfraw1.信号名称=='九转卖SetUp'].groupby('date')[['1日超额','1日超额_c']].sum().cumsum().plot()


# In[10]:


t=dfraw1[dfraw1.信号名称=='九转卖SetUp'].groupby(['date'])['1日超额'].agg(['mean','count'])
t.sort_values(by='count')


# In[11]:


clms='信号名称,标的代码,信号日期,信号类型,标的名称,申万一级行业,申万二级行业,1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')


# In[12]:


dfraw1.loc[:,'信号类型']='K线形态'
dfraw1=dfraw1.rename(columns={'date':'信号日期','code':'标的代码'})[clms]
dfraw1=dfraw1[dfraw1.信号日期<closew.index[-1]]
dfraw1.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')]=np.round(dfraw1.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')]*100,1)


# In[13]:


path='..//产出//信号看板//九转信号.csv'
dfraw1[dfraw1.信号日期>='2023-01-01'].to_csv(path,index=False,encoding='utf-8')


# In[14]:


dfraw1[dfraw1.信号名称=='九转买SetUp'].loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')].median()


# In[15]:


dfraw1[dfraw1.信号日期>='2023-01-01'].shape


# In[16]:


dfraw1[dfraw1.信号名称=='九转买SetUp'].groupby('信号日期')['20日收益'].mean().cumsum().plot()


# In[17]:


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


# In[ ]:




