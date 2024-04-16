#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
path='/home/jovyan/data/store/rsync/jq_factor'
风格因子_hist=pd.read_parquet(os.path.join(path,'风格因子.parquet'))
风格因子列表=list(风格因子_hist.columns[2:])
风格因子列表=[i for i in 风格因子列表 if 'v2' not in i]


# In[2]:


closew = pd.read_pickle('/home/jovyan/work/commons/data/daily_data/data_daily.pickle')['closew'] #前复权量价数据

y=(closew.shift(-1)/closew).stack().rename('ret').reset_index()
y=y[~y.code.str.endswith('.bj')].dropna()
y.loc[:,'pct']=y.groupby('date').ret.rank(pct=True)
y=y[(y.pct<=0.995)&(y.pct>=0.005)]


# In[3]:


df=风格因子_hist.merge(y)
tp=pd.read_csv('/home/jovyan/data/store/rsync/data_daily/申万行业股票列表2022_renew.csv',encoding='gbk')
tp=tp[['code','申万一级','申万二级']]
df=df.merge(tp,how='left')
df=df[df.date>='2022-01-01'].sort_values(by='date')
df.loc[:,风格因子列表]=df.groupby(['code'])[风格因子列表].fillna(method='ffill')
def modify(x):
    # 去除极值（3sigma之外）
    lower_bounds = np.mean(x, axis=0) - 3 * np.std(x, axis=0)
    upper_bounds = np.mean(x, axis=0) + 3 * np.std(x, axis=0)
    for i in range(x.shape[1]):
        x[:, i] = np.clip(x[:, i], lower_bounds[i], upper_bounds[i])

    # 标准化
    scaler = StandardScaler()
    x = scaler.fit_transform(x)

    # 去空值
    x[np.isnan(x)]=0
    return x


# In[4]:


dts=list(df.date.unique())
dummies = pd.get_dummies(df['申万一级'], prefix='申万一级')
行业列表=dummies.columns
# 添加行业哑变量（一级）
def cal(tp):
    dummies = pd.get_dummies(tp['申万一级'], prefix='申万一级')
    tp = pd.concat([tp, dummies], axis=1)
    column_list = 风格因子列表
    x=tp[column_list].astype('float').values
    y=np.log(tp.ret.values)     # 取对数
    x=modify(x)
    x=np.concatenate((x, dummies[行业列表].values), axis=1)
    import statsmodels.api as sm
    result = sm.OLS(y, x, hasconst=False).fit()
    return result.rsquared,result.params,tp.date.head(1).item()

import time
import multiprocessing
t1=time.time()
r2_l = []
p_l = []
dt_l=[]
pool = multiprocessing.Pool()  # 创建进程池

# 使用进程池的 map 方法进行并行计算
results = pool.map(cal, [tp for dt, tp in df.groupby('date')])

# 将计算结果分别存储到 r2_l 和 p_l 列表中
for r2, p, t in results:
    r2_l.append(r2)
    p_l.append(p)
    dt_l.append(t)

pool.close()  # 关闭进程池
pool.join()  # 等待所有进程执行完毕

# 打印结果
print('耗时',time.time()-t1)


# In[5]:


收益组成=pd.DataFrame(p_l,index=dt_l,columns=list(风格因子列表)+list(行业列表))


# In[6]:


收益组成=np.round(收益组成*100,2)
收益组成raw=收益组成.copy()


# In[7]:


风格因子=['residual_volatility', 'size', 'beta', 'momentum', 'growth',       'earnings_yield', 'leverage', 'book_to_price_ratio', 'liquidity',       'non_linear_size']
#ret=收益组成[风格因子].stack().reset_index()
ret=收益组成[风格因子].rolling(20).mean().stack().reset_index()
ret.columns=['日期','B属性','取值']
ret=ret[ret.日期>='2023-01-01']


# In[8]:


收益组成[风格因子].mean()/收益组成[风格因子].std()


# In[9]:


分析名称='test'
#ret=pd.DataFrame(ret,columns=['B属性','日期','取值'])
ret.loc[:,'A属性']=''
ret.loc[:,'分析名称']=分析名称
ret.loc[:,'取值']=np.round(ret.取值,3)

path='..//产出//%s//%s.xlsx'%(分析名称,ret.日期.max().strftime('%Y%m%d'))
ret.loc[:,'日期']=ret.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
ret[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)

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


#收益组成[风格因子].rolling(20).mean()['momentum'].plot()


# In[11]:


#收益组成[风格因子].rolling(20).mean()['size'].plot()


# In[12]:


#收益组成[风格因子].rolling(20).mean()['growth'].plot()


# In[13]:


#收益组成[风格因子].rolling(20).mean()['earnings_yield'].plot()


# In[14]:


#收益组成[风格因子].rolling(20).mean()['residual_volatility'].plot()


# In[15]:


#收益组成[风格因子].rolling(20).mean()['liquidity'].plot()


# In[16]:


rchange=pd.DataFrame(p_l,index=dt_l,columns=list(风格因子列表)+list(行业列表))
alpha20 = rchange.rolling(window=20).sum()
std20 = rchange.rolling(window=20).std()
alpha_std_20 = alpha20/std20/np.sqrt(20)
x_alpha20 = alpha_std_20.rolling(window=5).mean()

alpha30 = rchange.rolling(window=30).sum()
std30 = rchange.rolling(window=30).std()
alpha_std_30 = alpha30/std30/np.sqrt(30)
x_alpha30 = alpha_std_30.rolling(window=20).mean()


# In[ ]:





# In[ ]:





# In[17]:


# #t=(1-rchange[['growth']]).cumprod()
# t=(1+rchange[['growth']]).cumprod()
# dfbase=t
# dfbase.columns=['close']

# N=20
# fee_pct=0.001
# dfbase.loc[:,'rm_m']=dfbase['close'].fillna(method='ffill').rolling(N,min_periods=1).apply(lambda x:cal_rm(x))
# dfbase.loc[:,'pct']=dfbase['close'].pct_change()
# dfbase.loc[:,'pct_']=0
# ind=(dfbase.close>dfbase.rm_m).shift(1).fillna(False)
# ind_=ind&(~ind.shift(1).astype(bool))
# dfbase.loc[ind,'pct_']=dfbase.loc[(dfbase.close>dfbase.rm_m).shift(1).fillna(False),'pct']
# dfbase.loc[ind_,'pct_']=dfbase.loc[ind_,'pct_']-fee_pct
# (dfbase[['pct','pct_']].fillna(0)+1).cumprod().plot()


# In[18]:


def cal_rm(x):
    if len(x)<5:
        return x[-1]
    import numpy as np
    import statsmodels.api as sm

    # 构建时间序列
    t = np.arange(len(x))

    # 构建设计矩阵
    X = np.column_stack((t, np.ones(len(t))))

    # 使用RLM进行中位数稳健回归
    model = sm.RLM(x, X, M=sm.robust.norms.TukeyBiweight())
    results = model.fit()
    y_fit = results.params[1] + results.params[0]*t
    return y_fit[-1]


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


# In[20]:


for v in 风格因子:
    plt.figure()
    收益组成raw[v].tail(60).plot()
    plt.title(v)
    plt.grid(True)  # 显示网格线
    plt.show()  # 显示图像
    print('\n\n')


# In[21]:


np.abs(收益组成raw.tail(1)[风格因子列表].mean()-收益组成raw.tail(60)[风格因子列表].mean()).sort_values(ascending=False)


# In[22]:


(收益组成raw.tail(1)[风格因子列表].mean()-收益组成raw.tail(60)[风格因子列表].mean()).sort_values(ascending=False)


# In[23]:


np.abs(收益组成raw.tail(1)[行业列表].mean()-收益组成raw.tail(60)[行业列表].mean()).sort_values(ascending=False)


# In[24]:


(收益组成raw.tail(1)[行业列表].mean()-收益组成raw.tail(60)[行业列表].mean()).sort_values(ascending=False)


# In[ ]:




