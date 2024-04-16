#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')


# In[ ]:


get_ipython().run_cell_magic('capture', '', '%cd ..//../代码\n%run 下载原始数据.py\n%run 托管数据预处理.py\n%run 每日持仓和资金预处理.py\n%cd ..//盘后分析//代码')


# In[3]:


get_ipython().run_cell_magic('HTML', '', '<style type="text/css">\ntable.dataframe td, table.dataframe th {\nborder: 1px  black solid !important;\n  color: black !important;}')


# In[4]:


root='持仓数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('盘后权益')])))
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
dfraw.loc[:,'策略']=dfraw.loc[:,'策略'].str.cat(dfraw.account)

dfraw=dfraw[(dfraw.持有市值>=100000)&(~dfraw.账户.str.contains('PAPER'))]
dfraw.loc[:,'策略大类']=dfraw.loc[:,'策略'].apply(lambda x:x.split('_')[0])

root='原始数据_托管'
mls=os.listdir(root)
tp=pd.read_csv(os.path.join(root,max([i for i in mls if i.startswith('全部_')])))
detailraw = tp[获取指增标记(tp)]

detailraw.loc[detailraw.账户=='心宿二 开源 KMAX SM','account']='_xx2'
detailraw.loc[detailraw.账户=='狮子量化 华创 TRADEX SM','account']='_hc'
detailraw.loc[detailraw.账户=='狮子丑寅 开源 KMAX SM','account']='_cy'
detailraw.loc[detailraw.账户=='量化PAPER','account']='_pp'
detailraw.loc[:,'策略']=detailraw.loc[:,'策略'].str.cat(detailraw.account)


# In[5]:


基准='zz1000'
指数列表={'zz500':'399905.SZ','hs300':'399300.SZ','zz1000':'000852.SH'}
dfbase=pd.read_parquet('..//..//持仓数据//%s.parquet'%指数列表[基准])
if dfbase.index[-1]<df.日期.max():
    get_ipython().run_line_magic('cd', './代码')
    get_ipython().run_line_magic('run', '更新指数基准.py')
    get_ipython().run_line_magic('cd', '..')
dfbase.loc[:,基准]=dfbase.close.pct_change()
df=dfraw.merge(dfbase.reset_index().rename(columns={'trade_date':'日期'})[['日期',基准]])
df.loc[:,'alpha']=df.当日收益率-df[基准]

df.sort_values(by=['日期','账户','策略'],inplace=True)
df.loc[:,'wk']=df.日期.apply(lambda x:x.week)
df.loc[:,'mon']=df.日期.apply(lambda x:x.month)
def f1w(tp):
    tp=tp[(tp.账户==tp.账户.values[-1])&(tp.策略==tp.策略.values[-1])]
    wks=list(tp.wk.unique())
    return (tp[tp.wk.isin(wks[-1:])]['alpha'].fillna(0)+1).cumprod().values[-1]-1
def f2w(tp):
    tp=tp[(tp.账户==tp.账户.values[-1])&(tp.策略==tp.策略.values[-1])]
    wks=list(tp.wk.unique())
    return (tp[tp.wk.isin(wks[-2:])]['alpha'].fillna(0)+1).cumprod().values[-1]-1
def fm(tp):
    tp=tp[(tp.账户==tp.账户.values[-1])&(tp.策略==tp.策略.values[-1])]
    return (tp[tp.mon==tp.mon.values[-1]]['alpha'].fillna(0)+1).cumprod().values[-1]-1
ret_2w=[]
ret_1m=[]
ret_1w=[]
for i in range(df.shape[0]):
    ret_1w.append(f1w(df.head(i+1)))
    ret_2w.append(f2w(df.head(i+1)))
    ret_1m.append(fm(df.head(i+1)))
df.loc[:,'近2周超额']=ret_2w
df.loc[:,'本月超额']=ret_1m
df.loc[:,'本周超额']=ret_1w
df[['当日收益率','alpha','本周超额','近2周超额','本月超额']]=np.round(df[['当日收益率','alpha','本周超额','近2周超额','本月超额']]*100,1)
df=df[(df.持有市值>100000)&(~df.账户.str.contains('PAPER'))]


# In[6]:


分析名称='指增策略超额分析'
tp=df.rename(columns={'alpha':'当日超额'})[df.日期==df.日期.max()][['日期','账户','策略','当日收益率','当日超额','本周超额','近2周超额','本月超额']]
tp=tp.set_index(['日期','策略'])[['当日收益率','当日超额','本周超额','近2周超额','本月超额']].stack().reset_index()
tp.columns=['日期','A属性','B属性','取值']
tp.loc[:,'分析名称']=分析名称
path='..//产出//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)


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

