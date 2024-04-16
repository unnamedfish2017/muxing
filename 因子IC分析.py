#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
import sys,time
sys.path.append('/home/jovyan/work/workspaces/daily report/实盘模型/模型文件/周频策略/')
from EIFBT import *
from sklearn.preprocessing import StandardScaler
import statsmodels.api as sm

n_prd=5

DATA_PATH = '/home/jovyan/data/store/rsync/'
params={
'path':'/home/jovyan/data/store/rsync/data_daily/data_daily.pickle',
'path_':'/home/jovyan/work/commons/data/daily_data/stock_day_n.parquet',

'通联因子路径':f'{DATA_PATH}/factor_data/A股精品因子数据.parquet',
'n_prd':5 
        }

openw,highw,loww,closew,amtw,pavg=载入日线(params['path'],params['path_'])


# In[3]:


df=(openw.shift(-2)/openw.shift(-1)).fillna(0).stack().rename('收益').reset_index()
df=df[~df.code.str.endswith('.bj')]

ind1=(highw==loww).fillna(False)
ind2=np.isnan(closew.shift(1)).fillna(False)
ind=(ind1|ind2).shift(-1).fillna(False)

tp=(openw/closew.shift(1)-1)
tp[ind]=np.nan
tp=tp.stack().rename('ko').reset_index()
df=df.merge(tp,how='left')
tp=(highw/closew.shift(1)-1).stack().rename('kh').reset_index()
df=df.merge(tp,how='left')
tp=(loww/closew.shift(1)-1).stack().rename('kl').reset_index()
df=df.merge(tp,how='left')
tp=(closew/closew.shift(1)-1).stack().rename('kc').reset_index()
df=df.merge(tp,how='left')
tp=(amtw).stack().rename('amt').reset_index()
df=df.merge(tp,how='left')

tp=(highw/highw.shift(1)-1).stack().rename('hh').reset_index()
df=df.merge(tp,how='left')
tp=(loww/loww.shift(1)-1).stack().rename('ll').reset_index()
df=df.merge(tp,how='left')
tp=(openw/openw.shift(1)-1).stack().rename('oo').reset_index()
df=df.merge(tp,how='left')

基础日线因子=['ko','kh','kl','kc','amt','hh','ll','oo']
df.rename(columns={'date':'日期','code':'股票代码'},inplace=True)

import time
t1=time.time()
data_set,因子列表=载入因子数据(df,params,closew=closew)
t2=time.time()
print('耗时:',t2-t1)


# In[4]:


y=(closew.shift(-1)/closew).stack().rename('ret').reset_index()
y=y[~y.code.str.endswith('.bj')].dropna()
y.loc[:,'pct']=y.groupby('date').ret.rank(pct=True)
y=y[(y.pct<=0.995)&(y.pct>=0.005)]
data_set=data_set[data_set.收益>0]
data_set.loc[:,'收益pct']=data_set['收益'].groupby(data_set['日期']).rank(pct=True)


# In[7]:


data_set=data_set[data_set.日期>='2023-01-01']


# In[8]:


通联因子列表=因子列表['通联因子']


# In[11]:


df=data_set.copy()
df.loc[:,通联因子列表]=df.groupby(['股票代码'])[通联因子列表].fillna(method='ffill')


# In[13]:


import pandas as pd
from multiprocessing import Pool

def calculate_corr(group):
    dt, tp = group
    t = tp[通联因子列表+['收益']].corr()[['收益']]
    t.loc[:,'date'] = dt
    return t


# Number of processes to use
num_processes = 20

# Split the DataFrame into groups
groups = list(df.groupby('日期'))

# Initialize a multiprocessing pool
with Pool(num_processes) as pool:
    # Use the pool.map function to calculate correlations in parallel
    ret = pool.map(calculate_corr, groups)

# Concatenate the results into a single DataFrame
result_df = pd.concat(ret)

# Print the resulting DataFrame
print(result_df)


# In[17]:


ret=result_df.reset_index().pivot(index='date',columns='index',values='收益')
t=(ret.mean()/ret.std()).sort_values()


# In[28]:


import matplotlib.pyplot as plt
for v in list(t.index[:5])+list(t.index[-6:-1]):
    print('---------------------------------------------')
    print(v,t.loc[v])
    plt.figure()
    ret[v].plot()
    plt.title(v)
    plt.grid(True)  # 显示网格线
    plt.show()  # 显示图像
    print('\n\n')


# In[19]:


import matplotlib.pyplot as plt


for v in 风格因子列表:
    # 使用Matplotlib绘制图像
    plt.figure()
    x_alpha20[v].plot()
    plt.title(v)
    plt.grid(True)  # 显示网格线
    plt.show()  # 显示图像
    print('\n\n')


# In[25]:


for v in 风格因子:
    plt.figure()
    收益组成raw[v].tail(60).plot()
    plt.title(v)
    plt.grid(True)  # 显示网格线
    plt.show()  # 显示图像
    print('\n\n')


# In[ ]:




