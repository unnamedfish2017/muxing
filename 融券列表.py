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


# In[2]:


# rz=['600777.sh',
# '603008.sh',
# '600846.sh',
# '002536.sz',
# '300456.sz',
# '300223.sz',
# '600094.sh',
# '300075.sz',
# '300377.sz',
# '603985.sh']
rz=[]


# In[3]:


clms=[i for i in closew.columns if not i.endswith('.bj')]
tp=(closew/closew.shift(5))[clms]
tp=tp.rank(axis=1,pct=True,ascending=False)
tp=tp.tail(1).stack().rename('pct_chg').reset_index()
tp=tp[(tp.pct_chg<=0.01)|(tp.code.isin(rz))].reset_index(drop=True)


# In[4]:


tp.to_csv('..//产出//融券列表//融券列表_%s.csv'%closew.index[-1].strftime('%Y%m%d'),index=False)


# In[5]:


df=pd.read_csv('..//素材//融出信息导出_20230915_090051.csv',encoding='gbk')


# In[6]:


df


# In[7]:


def format_stock_code(code):
    # 前面补全0，总共六位
    formatted_code = str(code).zfill(6)
    
    # 添加.sh或.sz
    if code < 600000:
        formatted_code += '.sz'
    else:
        formatted_code += '.sh'
    
    return formatted_code

# 示例：将股票代码数字转换为格式化代码
stock_code = 1
formatted_code = format_stock_code(stock_code)
print(formatted_code)


# In[8]:


df.loc[:,'code']=df.证券代码.apply(lambda x:format_stock_code(x))


# In[9]:


df=tp.merge(df,how='left')


# In[10]:


len(df[df.券源类型=='实时券'].code.unique())


# In[11]:


len(df[df.券源类型=='竞拍券'].code.unique())


# In[12]:


df[df.券源类型=='实时券']


# In[14]:


df[df.委托数量>0]


# In[ ]:




