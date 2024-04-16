#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import tushare as ts
import time,datetime
tkpath='/home/jovyan/work/commons/data/daily_data/tk.txt'
tk=pd.read_csv(tkpath).columns[0]
ts.set_token(tk)###最新20220607
pro = ts.pro_api()

if int(time.strftime("%H%M",time.localtime()))<=2300:
    lst_day=(datetime.date.today()-datetime.timedelta(days=1)).strftime('%Y%m%d')
else:
    lst_day = (datetime.date.today()).strftime('%Y%m%d')

today = time.strftime("%Y%m%d",time.localtime())
df = pro.query('trade_cal', start_date='20220101', end_date='20301201').sort_values(by='cal_date').reset_index(drop=True)
dts_open=list(df[df.is_open>0].cal_date)
today = time.strftime("%Y%m%d",time.localtime())
df = pro.query('trade_cal', start_date='20220101', end_date='20301201').sort_values(by='cal_date').reset_index(drop=True)
dts_open=list(df[df.is_open>0].cal_date)
df=df[(df.is_open>0)&(df.cal_date<=lst_day)]
lst_trd_day=str(df.cal_date.values[-1])
nxt_trd_day=dts_open[dts_open.index(lst_trd_day)+1]
def modify_dt(dt):
    return pd.to_datetime(dt).strftime('%Y-%m-%d')


# In[2]:


root_data='/home/jovyan/work/workspaces/daily report/实盘模型/每日数据'
df_60d_facts_raw1 = pd.read_parquet(f'{root_data}/%s_60d_daily_facts_small_002.parquet'%modify_dt(lst_trd_day))
df_60d_facts_raw2 = pd.read_parquet(f'{root_data}/%s_60d_daily_facts_small_002.parquet'%modify_dt(nxt_trd_day))


# In[3]:


import numpy as np
tp1=df_60d_facts_raw1.set_index(['instrument','datetime']).stack().reset_index()
tp1.columns=['code','date','factor','value']
tp1=tp1[~tp1.factor.isin(['miyuan.lv2_hy_name','miyuan.hy_name'])]

tp2=df_60d_facts_raw2.set_index(['instrument','datetime']).stack().reset_index()
tp2.columns=['code','date','factor','value_sp']
tp2=tp2[~tp2.factor.isin(['miyuan.lv2_hy_name','miyuan.hy_name'])]

tp=tp1.merge(tp2)
tp.loc[:,'gap']=np.abs(tp.loc[:,'value']-tp.loc[:,'value_sp'])#+(tp.loc[:,'value']/tp.loc[:,'value_sp'].fillna(0).replace(0,1))


# In[4]:


差异=tp[tp.gap>=0.0001].sort_values(by='gap').groupby('factor').gap.agg(['count','median']).reset_index().to_markdown(index=False)
import json
import requests
import joblib
import pandas as pd

def feishu_message(内容):
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/78d4cfc5-0dd3-46f7-8a6a-5abb443c86cb"
    payload_message = {
     "msg_type": "text",
     "content": {
     "text": 内容
        }
    }
    headers = {
     'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload_message))
def feishu_message_muxing(内容):
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/fd58b3a6-5e4b-4fd8-bcb0-804eb0644b21"
    payload_message = {
     "msg_type": "text",
     "content": {
     "text": 内容
        }
    }
    headers = {
     'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload_message))


# In[5]:


feishu_message('QT\n'+差异)


# In[ ]:




