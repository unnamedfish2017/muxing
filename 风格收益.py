#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
def get_parquet(root):
    mls=os.listdir(root)
    ret=[]
    for ml in mls:
        if ml.endswith('.parquet') and ml[:8]>='20240101':
            tp=pd.read_parquet(os.path.join(root,ml))
            ret.append(tp)
    ret=pd.concat(ret)
    return ret
DATA_PATH = '/home/jovyan/data/store/rsync/'
root=f'{DATA_PATH}/factor_data/通联试用数据'
dy1d_factor_ret_all_df=get_parquet(os.path.join(root,'RMFactorRetDaySW21'))
dy1d_factor_ret_all_df.loc[:,'date']=pd.to_datetime(dy1d_factor_ret_all_df['tradeDate'].astype(str))
CNE5_StyleName = ['BETA', 'MOMENTUM', 'SIZE', 'EARNYILD', 'RESVOL', 'GROWTH', 'BTOP', 'LEVERAGE', 'LIQUIDTY', 'SIZENL']


# In[2]:


import pandas as pd
import os
def get_parquet(root):
    mls=os.listdir(root)
    ret=[]
    for ml in mls:
        if ml.endswith('.parquet') and ml[:8]>='20240101':
            tp=pd.read_parquet(os.path.join(root,ml))
            ret.append(tp)
    ret=pd.concat(ret)
    return ret
DATA_PATH = '/home/jovyan/data/store/rsync/'
root=f'{DATA_PATH}/factor_data/通联试用数据'
dy1d_factor_ret_all_df=get_parquet(os.path.join(root,'RMFactorRetDaySW21'))
dy1d_factor_ret_all_df['date']=pd.to_datetime(dy1d_factor_ret_all_df['tradeDate'].astype(str))
tp=dy1d_factor_ret_all_df.set_index('date')
dy1d_factor_ret_all_df.sort_values(by='date').tail(5)


# In[3]:


import matplotlib.pyplot as plt
# 遍历每个列名，并绘制相应的累积求和值图表
for v in ['BETA', 'MOMENTUM', 'SIZE', 'EARNYILD', 'RESVOL', 'GROWTH', 'BTOP', 'LEVERAGE', 'LIQUIDTY', 'SIZENL']:
    # 创建新的图表
    plt.figure(figsize=(10, 6))
    
    # 绘制累积求和值图表
    plt.plot(tp.tail(60)[v].cumsum())
    
    # 添加标题和标签
    plt.title('Cumulative Sum of ' + v)
    plt.xlabel('Date')
    plt.ylabel('Cumulative Sum')
    
    # 显示图表
    plt.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




