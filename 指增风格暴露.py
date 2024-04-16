#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import warnings
import os
warnings.filterwarnings('ignore')
import datetime
import tushare as ts
import time,os
import pandas as pd
import tushare as ts
tkpath='/home/jovyan/work/commons/data/daily_data/tk.txt'
tk=pd.read_csv(tkpath).columns[0]
ts.set_token(tk)###最新20220607
pro = ts.pro_api()

if int(time.strftime("%H%M",time.localtime()))<=1600:
    lst_day=(datetime.date.today()-datetime.timedelta(days=1)).strftime('%Y%m%d')
else:
    lst_day = (datetime.date.today()).strftime('%Y%m%d')

today = time.strftime("%Y%m%d",time.localtime())
df = pro.query('trade_cal', start_date='20220101', end_date='20301201').sort_values(by='cal_date').reset_index(drop=True)
dts_open=list(df[df.is_open>0].cal_date)
df=df[(df.is_open>0)&(df.cal_date<=lst_day)]
lst_trd_day=str(df.cal_date.values[-1])
nxt_trd_day=dts_open[dts_open.index(lst_trd_day)+1]
import os
import pandas as pd


# In[2]:


import pickle
import pandas as pd
import numpy as np
import os
path='/home/jovyan/work/commons/data/daily_data/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
closew = data_daily['closew'].fillna(method='ffill')


# In[3]:


风格因子=pd.read_parquet('/home/jovyan/data/store/rsync/jq_factor/风格因子.parquet')
风格因子=风格因子[风格因子.date>='2023-01-01']
风格因子列表=list(风格因子.columns[2:])
for v in 风格因子列表:
    print(v,'finished')
    风格因子.loc[:,v]=风格因子[v].groupby(风格因子['date']).rank(pct=True)
风格因子=风格因子[(风格因子.date==风格因子.date.max())&(~风格因子.code.str.endswith('.bj'))]
风格因子.loc[:,'Instrument']=风格因子.code.apply(lambda x:x[-2:].upper()+'SE.S+'+x[:6])
风格因子[风格因子列表]=风格因子[风格因子列表].rank(pct=True)-0.5


# In[4]:


tp=pd.read_csv('/home/jovyan/data/store/rsync/data_daily/申万行业股票列表2022_renew.csv',encoding='gbk')
tp=tp[['code','申万一级','申万二级']]
dummies = pd.get_dummies(tp['申万一级'], prefix='申万一级')
行业因子列表=list(dummies.columns)
行业因子=pd.concat([tp,dummies],axis=1)
t=tp[~tp.code.str.endswith('.bj')].groupby('申万一级').code.count()
pct_base=t/t.sum()


# In[5]:


import os
root='/home/jovyan/work/workspaces/daily report/每日报表/持仓数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('系统_盘后持仓')])))
现有持仓=df[df.日期==pd.to_datetime(lst_trd_day)]
print(lst_trd_day,'持仓已获取',现有持仓.shape)


# In[6]:


dfraw=现有持仓[['账户','策略','code','持有市值']]
dfraw=dfraw.merge(风格因子[['code']+风格因子列表], how='left').merge(行业因子[['code']+行业因子列表], how='left')
dfraw=dfraw[~dfraw.账户.str.contains('PAPER')]


# In[7]:


ret=[dfraw.groupby(['账户','策略']).持有市值.sum()]
for v in 行业因子列表+风格因子列表:
    t=dfraw.groupby(['账户','策略'])
    tt=t.apply(lambda x:(x['持有市值']*x[v].fillna(0)).sum()/x['持有市值'].sum())
    tt.name=v
    ret.append(tt)


# In[ ]:





# In[8]:


ret=pd.concat(ret,axis=1)
ret[ret.持有市值>0]


# In[9]:


ret.持有市值.sum()


# In[10]:


res=(ret * ret['持有市值'].values[:, None]).sum() / ret['持有市值'].sum()
#res[行业因子列表].sort_values().plot.bar()
res[行业因子列表].sort_values()


# In[11]:


print('超低配')
pct=res[行业因子列表]
pct.index=[i.replace('申万一级_','') for i in pct.index]
t=pd.concat([pct,pct_base],axis=1)
t.columns=['pct','pct_base']
t.loc[:,'pct_over_bench']=t.loc[:,'pct']-t.loc[:,'pct_base']
t['pct_over_bench'].sort_values()


# In[12]:


res[风格因子列表].sort_values().plot.bar()
res[风格因子列表].sort_values()


# In[13]:


import pandas as pd
import glob

root='/home/jovyan/work/workspaces/daily report/每日报表/持仓数据'
mls=os.listdir(root)
file_list=[i for i in mls if '系统_盘后持仓' in i]
# 创建一个空DataFrame，用于存储所有数据
result_df = pd.DataFrame()

# 逐个读取匹配的Parquet文件
for file_path in file_list:
    # 使用 pd.read_parquet 读取文件并追加到结果DataFrame
    data = pd.read_parquet(os.path.join(root,file_path))
    result_df = result_df.append(data, ignore_index=True)

# 现在 result_df 包含了所有匹配文件的数据


# In[14]:


历史持仓=result_df[~result_df.账户.str.contains('PAPER')]
历史持仓=历史持仓[历史持仓.策略.astype(str).str.startswith(('EA','E1','ET','E3','E5'))]


# In[15]:


dfraw=历史持仓[['账户','策略','code','持有市值','日期']]
dfraw=dfraw.merge(风格因子[['code']+风格因子列表], how='left').merge(行业因子[['code']+行业因子列表], how='left')
dfraw=dfraw[~dfraw.账户.str.contains('PAPER')]

ret=[dfraw.groupby(['日期']).持有市值.sum()]
for v in 行业因子列表+风格因子列表:
    t=dfraw.groupby(['日期'])
    tt=t.apply(lambda x:(x['持有市值']*x[v].fillna(0)).sum()/x['持有市值'].sum())
    tt.name=v
    ret.append(tt)


# In[16]:


ret=pd.concat(ret,axis=1)
del ret['持有市值']
ret=ret.T


# In[17]:


pct_base


# In[18]:


ret1=ret[ret.index.str.contains('申万')]
ret1.index=[i.replace('申万一级_','') for i in ret1.index]
ret1.loc[:,'base']=pct_base.loc[ret1.index].values
ret1 = ret1.sub(ret1['base'], axis=0)
del ret1['base']
ret1=np.round(ret1*100,1)
ret1=ret1.T


# In[19]:


ret_all=ret1.stack().reset_index()
ret_all.columns=['日期','B属性','取值']
ret_all.loc[:,'A属性']=''

分析名称='指增行业暴露'
tp=ret_all[['日期','A属性','B属性','取值']]
tp.loc[:,'分析名称']=分析名称
path='..//产出//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)


# In[20]:


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


# In[21]:


ret2=ret[~ret.index.str.contains('申万')]
ret2=np.round(ret2*100,1)
ret2=ret2.T
ret_all=ret2.stack().reset_index()
ret_all.columns=['日期','B属性','取值']
ret_all.loc[:,'A属性']=''

分析名称='指增风格暴露'
tp=ret_all[['日期','A属性','B属性','取值']]
tp.loc[:,'分析名称']=分析名称
path='..//产出//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)


# In[22]:


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


# In[ ]:




