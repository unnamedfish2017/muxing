#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import warnings
import os
warnings.filterwarnings('ignore')


# In[67]:


root='..//..//持仓数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('盘后持仓')])))
df.日期=pd.to_datetime(df.日期.astype(str))

def 获取指增标记(df):
    #ind1=(df.策略.str.contains('EIF')) 
    #ind2=(df.策略.str.startswith(('LPP', 'LKW', 'LYK', 'TF98', 'ALSTM98', 'LPPM', 'GAT')) )
    #ind2=(df.策略.str.startswith(('LPP', 'LPPM', 'GAT')) )
    ind3=(df.策略.str.startswith(('E1', 'EA','E5','ET')) )
    #指增标记=ind1|ind2|ind3
    ind4=(~df.策略.str.startswith(('EAW')) )
    指增标记=ind4&ind3
    return 指增标记
dfraw=df[获取指增标记(df)]
dfraw.loc[:,'account']=''
dfraw.loc[dfraw.账户=='心宿二 开源 KMAX SM','account']='_xx2'
dfraw.loc[dfraw.账户=='狮子量化 华创 TRADEX SM','account']='_hc'
dfraw.loc[dfraw.账户=='狮子丑寅 开源 KMAX SM','account']='_cy'
dfraw.loc[dfraw.账户=='量化PAPER','account']='_pp'
dfraw.loc[dfraw.账户=='狮子alpha 国联 QMT','account']='_gl'
dfraw.loc[dfraw.账户=='金牛 长江 QMT','account']='_jn'
dfraw.loc[dfraw.账户=='金牛丁丑 东吴 QMT','account']='_jndc'
dfraw.loc[:,'策略']=dfraw.loc[:,'策略'].str.cat(dfraw.account)

dfraw=dfraw[(dfraw.持有市值>=1000)&(~dfraw.账户.str.contains('PAPER'))]
dfraw.loc[:,'策略大类']=dfraw.loc[:,'策略'].apply(lambda x:x.split('_')[0])


# In[68]:


import pickle
import pandas as pd
import numpy as np
import os
path='/home/jovyan/work/commons/data/daily_data/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
closew = data_daily['closew'].fillna(method='ffill')
zz=(closew/closew.shift(1)-1).stack().rename('ret').reset_index().rename(columns={'date':'日期'})
dfall=dfraw.merge(zz,how='left').dropna(subset=['ret'])


# In[29]:


风格因子=pd.read_parquet('/home/jovyan/data/store/rsync/jq_factor/风格因子.parquet')
风格因子=风格因子[风格因子.date>='2023-01-01']
风格因子列表=list(风格因子.columns[2:])
for v in 风格因子列表:
    print(v,'finished')
    风格因子.loc[:,v]=风格因子[v].groupby(风格因子['date']).rank(pct=True)
#风格因子.loc[:,'Instrument']=风格因子.code.apply(lambda x:x[-2].upper()+'SE.S+'+x[:6])


# In[72]:


dfall=dfall.merge(风格因子.rename(columns={'date':'日期'}),how='left')


# In[74]:


tp=(dfall.groupby(['日期','策略'])[风格因子列表].mean()-0.5).stack().reset_index()
tp.columns=['日期','B属性','A属性','取值']


# In[75]:


分析名称='指增风格暴露'

tp.loc[:,'分析名称']=分析名称
tp.loc[:,'取值']=np.round(tp.loc[:,'取值']*100,0)
path='..//产出//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)


# In[76]:


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


# In[77]:


import os
command = 'jupyter nbconvert --to script *.ipynb'
os.system(command)


# In[83]:


原始数据=(dfall.groupby(['日期','策略'])[风格因子列表].median()-0.5).stack().reset_index()
原始数据.columns=['日期','策略','风格','暴露']


# In[84]:


原始数据.策略.unique()


# In[85]:


原始数据[(原始数据.策略=='EARF98A_xx2')&(原始数据.风格=='size')].set_index('日期').sort_index()[['暴露']].plot()


# In[86]:


原始数据[(原始数据.策略=='EALG98A_xx2')&(原始数据.风格=='size')].set_index('日期').sort_index()[['暴露']].plot()


# In[93]:


(dfall[(dfall.策略=='EARF98A_xx2')&(dfall.日期=='2023-08-01')]['size']-0.5).hist()


# In[94]:


(dfall[(dfall.策略=='EALG98A_xx2')&(dfall.日期=='2023-08-01')]['size']-0.5).hist()


# In[102]:


import matplotlib.pyplot as plt

# 创建包含多个图的画布
fig, axes = plt.subplots(nrows=len(风格因子列表), ncols=1, figsize=(8, 6*len(风格因子列表)))

# 循环处理每个风格因子
for i, v in enumerate(风格因子列表):
    # 计算并绘制每个风格因子的直方图
    (dfall[(dfall.策略=='EALG98A_xx2')&(dfall.日期=='2023-08-01')][v]-0.5).hist(ax=axes[i])
    axes[i].set_title(v)

# 调整子图之间的间距和布局
plt.tight_layout()

# 显示所有图
plt.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




