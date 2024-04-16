#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import warnings
import os
warnings.filterwarnings('ignore')


# In[2]:


root='..//..//持仓数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('盘后持仓')])))
df.日期=pd.to_datetime(df.日期.astype(str))

def 获取指增标记(df):
    #ind1=(df.策略.str.contains('EIF')) 
    #ind2=(df.策略.str.startswith(('LPP', 'LKW', 'LYK', 'TF98', 'ALSTM98', 'LPPM', 'GAT')) )
    #ind2=(df.策略.str.startswith(('LPP', 'LPPM', 'GAT')) )
    ind3=(df.策略.str.startswith(('E1', 'EA','E5','ET')) )
    #指增标记=ind1|ind2|ind3
    ind4=(~df.策略.str.startswith(('EAW')) )
    指增标记=ind4&ind3
    return 指增标记
dfraw=df[获取指增标记(df)]
dfraw.loc[:,'account']=''
dfraw.loc[dfraw.账户=='心宿二 开源 KMAX SM','account']='_xx2'
dfraw.loc[dfraw.账户=='狮子量化 华创 TRADEX SM','account']='_hc'
dfraw.loc[dfraw.账户=='狮子丑寅 开源 KMAX SM','account']='_cy'
dfraw.loc[dfraw.账户=='量化PAPER','account']='_pp'
dfraw.loc[dfraw.账户=='狮子alpha 国联 QMT','account']='_gl'
dfraw.loc[dfraw.账户=='金牛 长江 QMT','account']='_jn'
dfraw.loc[dfraw.账户=='金牛丁丑 东吴 QMT','account']='_jndc'
dfraw.loc[:,'策略']=dfraw.loc[:,'策略'].str.cat(dfraw.account)

dfraw=dfraw[(dfraw.持有市值>=1000)&(~dfraw.账户.str.contains('PAPER'))]
dfraw.loc[:,'策略大类']=dfraw.loc[:,'策略'].apply(lambda x:x.split('_')[0])


# In[3]:


import pickle
import pandas as pd
import numpy as np
import os
path='/home/jovyan/work/commons/data/daily_data/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
closew = data_daily['closew'].fillna(method='ffill')
zz=(closew/closew.shift(1)-1).stack().rename('ret').reset_index().rename(columns={'date':'日期'})
dfall=dfraw.merge(zz,how='left').dropna(subset=['ret'])


# In[ ]:





# In[4]:


指数列表={'zz500':'399905.SZ','hs300':'399300.SZ','zz1000':'000852.SH','zzqa':'000985.SH'}
for 基准 in 指数列表.keys():
    dfbase=pd.read_parquet('..//..//持仓数据//%s.parquet'%指数列表[基准])
    if dfbase.index[-1]<df.日期.max():
        get_ipython().run_line_magic('cd', '..//../代码')
        get_ipython().run_line_magic('run', '更新指数基准.py')
        get_ipython().run_line_magic('cd', '..//盘后分析//代码')


# In[5]:


def get(基准,dfall):
    指数列表={'zz500':'399905.SZ','hs300':'399300.SZ','zz1000':'000852.SH','zzqa':'000985.SH'}
    dfbase=pd.read_parquet('..//..//持仓数据//%s.parquet'%指数列表[基准])
    dfbase.loc[:,基准]=dfbase.close.pct_change()
    df=dfall.merge(dfbase.reset_index().rename(columns={'trade_date':'日期'})[['日期',基准]])
    df.loc[:,'alpha']=df.ret-df[基准]
    return df[['日期','策略','alpha']]


# In[6]:


t1=get('zz1000',dfall[dfall.策略.str.startswith('E1')])
t2=get('zzqa',dfall[dfall.策略.str.startswith(('EA','ET'))])
dfall=t1.append(t2)
dfall.loc[:,'flag']=(dfall.alpha>0).astype(int)


# In[7]:


dfall=dfall.groupby(['日期','策略']).flag.agg(['mean','count']).reset_index()
dfall=dfall[dfall['count']>=10]
dfall.loc[:,'日期']=pd.to_datetime(dfall.日期)


# In[8]:


分析名称='指增日频胜率'

dfall.columns=['日期','B属性','取值','']
tp=dfall
tp.loc[:,'A属性']=''
tp.loc[:,'分析名称']=分析名称
tp.loc[:,'取值']=np.round(tp.loc[:,'取值']*100,0)
path='..//产出//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)


# In[9]:


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


# In[10]:


import os
command = 'jupyter nbconvert --to script *.ipynb'
os.system(command)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




