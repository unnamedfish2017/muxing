#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[2]:


import os
root='/home/jovyan/work/workspaces/daily report/每日报表/日内预测监控/日内收益'
mls=os.listdir(root)
df=[]
for ml in mls:
    tp=pd.read_parquet(os.path.join(root,ml))
    tp=tp[tp.stk.str.endswith(('.sz','.sh'))]
    tp.loc[:,'date']=pd.to_datetime(ml[:8])
    df.append(tp)
df=pd.concat(df)


# In[3]:


import os
root='/home/jovyan/work/workspaces/daily report/每日报表/日内预测监控/日内预测'
mls=os.listdir(root)
df_pred=[]
for ml in mls:
    if not ml.endswith('.csv'):
        continue
    tp=pd.read_csv(os.path.join(root,ml))
    tp.loc[:,'stk']=tp.instrument.apply(lambda x:x[-6:]+'.'+x[:2].lower())
    tp.loc[:,'date']=pd.to_datetime(ml[:8])
    df_pred.append(tp)
df_pred=pd.concat(df_pred)[['stk','date','pred_y']].drop_duplicates(['date','stk']).reset_index(drop=True)
    


# In[4]:


ret=[]
for k, v in df.merge(df_pred).groupby(['seg','date']):
    ret.append((k[0],k[1],v[['pred_y','ret']].corr().values[0,1]))


# In[5]:


分析名称='日内预测IC'
ret=pd.DataFrame(ret,columns=['B属性','日期','取值'])
ret.loc[:,'A属性']=''
ret.loc[:,'分析名称']=分析名称
ret.loc[:,'取值']=np.round(ret.取值,3)

path='..//产出//%s//%s.xlsx'%(分析名称,ret.日期.max().strftime('%Y%m%d'))
ret.loc[:,'日期']=ret.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
ret[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)


# In[6]:


path.split('/')[-1]


# In[7]:


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


# In[8]:


import os
command = 'jupyter nbconvert --to script *.ipynb'
os.system(command)


# In[ ]:




