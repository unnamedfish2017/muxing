#!/usr/bin/env python
# coding: utf-8

# In[53]:


import os
import pandas as pd
root='/home/jovyan/work/workspaces/daily report/每日报表/持仓数据'
mls=os.listdir(root)
ret=[]
for ml in mls:
    if '系统_盘后持仓' in ml:
        df=pd.read_parquet(os.path.join(root,ml))
        ret.append(df)
历史持仓=pd.concat(ret).drop_duplicates()
历史持仓=历史持仓[~历史持仓.账户.str.contains('PAPER')]


# In[54]:


#path='..//产出//信号看板//九转信号.csv'
path='..//产出//信号看板//回购预案.csv'
sig=pd.read_csv(path).rename(columns={'标的代码':'code','信号日期':'日期'})
sig.日期=pd.to_datetime(sig.日期)
df=历史持仓.merge(sig)


# In[55]:


历史持仓


# In[56]:


df.groupby(['信号名称'])['1日超额'].mean()


# In[35]:


df[df.信号名称=='九转卖SetUp'].groupby(['信号名称','日期']).agg({'持有市值':sum,'1日超额':'mean'})


# In[57]:


df0=pd.read_parquet('/home/jovyan/work/commons/tmp/策略池.parquet')
(df0.merge(sig[sig.信号名称=='九转卖SetUp'].rename(columns={'code':'股票代码'})).groupby('日期')['1日超额'].mean().cumsum()/100).plot()


# In[ ]:




