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
dts=[i.strftime('%Y%m%d') for i in closew.index[-20:]]


# In[2]:


root='/home/jovyan/work/workspaces/daily report/次日列表/EIF候选池'
import os
EIF_mls=os.listdir(root)


# In[3]:


dfraw=[]
for v in EIF_mls:
    if v.endswith('.csv') and 'Pool' in v and v.startswith('E') and v.split('.')[0][-8:] in dts:
        tp=pd.read_csv(os.path.join(root,v),header=None)
        if tp.shape[1]>1:
            tp=tp.sort_values(by=[1],ascending=False).iloc[:,:1]
        tp=tp.head(100)
        strategy_name=v.split('Pool')[0]
        tp.columns=['code_sm']
        tp.loc[:,'date']=pd.to_datetime(v.split('.')[0][-8:])
        tp.loc[:,'信号名称']=strategy_name
        dfraw.append(tp)
dfraw=pd.concat(dfraw)
dfraw.loc[:,'code']=dfraw.code_sm.apply(lambda x:x[-6:]+'.'+x[:2].lower())


# In[4]:


tp1=pd.read_csv('/home/jovyan/data/store/rsync/data_daily//申万行业股票列表2022_renew.csv',encoding='gbk')
#tp=tp[['code','申万一级','申万二级']].rename(columns={'code':'股票代码'})
dfraw1=dfraw.merge(tp1,how='left').rename(columns={'name':'标的名称','申万一级':'申万一级行业','申万二级':'申万二级行业'})


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
dfbase=get_base(tkpath,base_index).set_index('trade_date')


# In[6]:


closew_ffill=closew.ffill()
openw[pd.isna(openw)]=closew_ffill.shift(1)[pd.isna(openw)]


# In[7]:


for i in [1,3,5,20]:
    tp=closew.shift(-i)/openw.shift(-1).ffill()-1
    tp=tp.stack().rename('%s日收益'%str(i)).reset_index()
    tp0=(dfbase.shift(-i)['close']/dfbase.shift(-1)['open']-1).reset_index()
    tp0.columns=['date','bench']
    tp=tp.merge(tp0,how='left')
    tp.loc[:,'%s日超额'%str(i)]=tp.loc[:,'%s日收益'%str(i)]-tp.bench
    del tp['bench']
    dfraw1=dfraw1.merge(tp,how='left')


# In[8]:


clms='信号名称,标的代码,信号日期,信号类型,标的名称,申万一级行业,申万二级行业,1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')


# In[9]:


dfraw1.loc[:,'信号类型']='指增信号'
dfraw1=dfraw1.rename(columns={'date':'信号日期','code':'标的代码'})[clms]
dfraw1=dfraw1[dfraw1.信号日期<closew.index[-1]]
dfraw1.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')]=np.round(dfraw1.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')]*100,1)
path='..//产出//信号看板//指增信号.csv'
dfraw1.to_csv(path,index=False,encoding='utf-8')


# In[10]:


import requests

url = "https://61.172.245.225:26829/signal-board/api/v1/public/import/raw"
headers = {
    'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'
}
data = {
    'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
}
files = { 'uploadFile': (path.split('/')[-1], open(path, 'r')) }
response = requests.post(url, headers=headers, data=data, files=files,verify=False)
files['uploadFile'][1].close()
response


# In[11]:


# clms='信号名称,标的代码,信号日期,信号类型,标的名称,申万一级行业,申万二级行业,1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')
# dfraw1.loc[:,'信号类型']='指增信号'
# dfraw1=dfraw1.rename(columns={'date':'信号日期','code':'标的代码'})[clms]
# for dt in dts[70:]:
#     tp=dfraw1[dfraw1.信号日期==pd.to_datetime(dt)]
#     tp.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')]=np.round(tp.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')]*100,1)
#     path='..//产出//信号看板//指增信号.csv'
#     tp.to_csv(path,index=False,encoding='utf-8')
#     import requests

#     url = "https://61.172.245.225:26829/signal-board/api/v1/public/import/raw"
#     headers = {
#         'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
#         'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'
#     }
#     data = {
#         'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
#     }
#     files = { 'uploadFile': (path.split('/')[-1], open(path, 'r')) }
#     response = requests.post(url, headers=headers, data=data, files=files,verify=False)
#     files['uploadFile'][1].close()
#     print(dt,response)


# In[12]:


#dfraw1.groupby(['信号名称']).标的代码.count()


# In[ ]:




