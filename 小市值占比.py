#!/usr/bin/env python
# coding: utf-8

# In[21]:


import datetime
import tushare as ts
import time,os
import pandas as pd
import tushare as ts
import numpy as np
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


# In[22]:


import os
import pandas as pd
root='/home/jovyan/work/workspaces/daily report/每日报表/持仓数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('系统_盘后持仓')])))
现有持仓=df[df.日期==pd.to_datetime(lst_trd_day)]
print(lst_trd_day,'持仓已获取',现有持仓.shape)


# In[32]:


现有持仓[现有持仓.账户=='量化PAPER'].策略.unique()


# In[23]:


tp=pd.reuniquearquet('/home/jovyan/data/store/rsync/data_daily/日线常规指标.parquet')
tp.loc[:,'日期']=pd.to_datetime(tp.loc[:,'trade_date'].astype(str))
tp.loc[:,'mv']=tp.total_mv/10000
tp.loc[:,'code']=tp.ts_code.str.lower()


# In[24]:


现有持仓=现有持仓.merge(tp[['日期','code','mv']],how='left')
#现有持仓=现有持仓[~现有持仓.账户.str.contains('PAPER')]
现有持仓=现有持仓[现有持仓.账户.str.contains('PAPER')]


# In[ ]:





# In[25]:


现有持仓.loc[:,'lt20']=现有持仓.mv.apply(lambda x:1 if x<=20 else 0)
现有持仓.loc[:,'lt50']=现有持仓.mv.apply(lambda x:1 if x<=50 else 0)


# In[26]:


现有持仓[现有持仓.lt50>0].持有市值.sum()/现有持仓.持有市值.sum()


# In[27]:


现有持仓.head()


# In[34]:


t=现有持仓.groupby(['账户','策略'])
t1=t.apply(lambda x:x[x.lt50>0].持有市值.sum())/t.apply(lambda x:x.持有市值.sum())
t2=t.持有市值.sum()//10000
t3=pd.concat([t1,t2],axis=1)
t3.columns=['比例','持有市值']
t3=t3.reset_index().sort_values(by='持有市值',ascending=False)
t3[t3.策略.str.startswith('E')].reset_index(drop=True).head(50)


# In[29]:


t3.to_excel('50亿以上市值占比.xlsx')


# In[ ]:





# In[ ]:




