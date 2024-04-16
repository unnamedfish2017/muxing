#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


import os
import pandas as pd
root='/home/jovyan/work/workspaces/daily report/每日报表/持仓数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('系统_盘后持仓')])))
现有持仓=df[df.日期==pd.to_datetime(lst_trd_day)]
print(lst_trd_day,'持仓已获取',现有持仓.shape)


# In[3]:


tp=pd.read_parquet('/home/jovyan/data/store/rsync/data_daily/日线常规指标.parquet')
tp.loc[:,'日期']=pd.to_datetime(tp.loc[:,'trade_date'].astype(str))
tp.loc[:,'mv']=tp.total_mv/10000
tp.loc[:,'code']=tp.ts_code.str.lower()


# In[4]:


现有持仓=现有持仓.merge(tp[['日期','code','mv']],how='left')
现有持仓=现有持仓[~现有持仓.账户.str.contains('PAPER')]


# In[114]:


root1='/home/jovyan/work/workspaces/daily report/次日列表//EIF候选池/'
ret=[]
mls=os.listdir(root1)
mls=[i for i in mls if 'ETLMS' in i]
for ml in mls:
    p=os.path.join(root1,ml)
    tp=pd.read_csv(p,header=None)
    tp.columns=['code','score']
    tp.loc[:,'date']=ml.split('.csv')[0][-8:]
    tp.code=tp.code.apply(lambda x:x[-6:]+'.'+x[:2].lower())
    ret.append(tp)
stk_pool=pd.concat(ret).reset_index(drop=True)[['date','code']]


# In[111]:


dts=list(stk_pool.date.unique())


# In[122]:


stk_pool


# In[ ]:





# In[103]:


import pandas as pd
import matplotlib.pyplot as plt
dts_test=dts[-60:-1]
集合=[]
for dt in dts_test:
#     print('==========================================')
#     print(dt)
    tp = list(stk_pool[stk_pool.date == dt].code)
    df = pd.read_parquet(path3 + f'/{dt}.parquet')
    df.loc[:, 'tm'] = df.时间.apply(lambda x: int(x.split(' ')[-1]) // 100)
    tp_data = df[df.代码.isin(tp)].groupby('tm').净买入金额.sum().sort_index()
    一日净流入=tp_data.reset_index()
    一日净流入.loc[:,'date']=pd.to_datetime(dt)
    集合.append(一日净流入)
    tp_data = tp_data.cumsum().reset_index(drop=True)
    
    # 创建新图
    #plt.figure()
    
    # 画图
    #plt.plot(tp_data)
    # 显示图形
    #plt.show()


# In[123]:


import pandas as pd
import multiprocessing

# 处理单个日期的函数
def process_date(dt, path3, stk_pool):
    tp = list(stk_pool[stk_pool.date == dt].code)
    df = pd.read_parquet(path3 + f'/{dt}.parquet')
    df.loc[:, 'tm'] = df.时间.apply(lambda x: int(x.split(' ')[-1]) // 100)
    tp_data = df[df.代码.isin(tp)].groupby('tm').净买入金额.sum().sort_index()
    一日净流入 = tp_data.reset_index()
    一日净流入.loc[:, 'date'] = pd.to_datetime(dt)
    return 一日净流入

if __name__ == '__main__':
    dts_test = dts[-120:-1]
    path3 = '/home/jovyan/data/store/external/MintueData/TransactionData/Data'

    # 使用 multiprocessing 处理每个日期
    with multiprocessing.Pool() as pool:
        集合 = pool.starmap(process_date, [(dt, path3, stk_pool) for dt in dts_test])

    # 集合 是每个日期处理后的结果，可以进行后续的合并或其他操作


# In[124]:


tp=pd.concat(集合).reset_index(drop=True)
tp.净买入金额=tp.净买入金额.cumsum()


# In[125]:


tp.iloc[:,1].plot()


# In[98]:





# In[ ]:




