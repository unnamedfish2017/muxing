#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import warnings
import os
warnings.filterwarnings('ignore')


# In[2]:


import datetime
import tushare as ts
import time
import pandas as pd
import tushare as ts
tkpath='/home/jovyan/work/commons/data/daily_data/tk.txt'
tk=pd.read_csv(tkpath).columns[0]
ts.set_token(tk)###最新20220607
pro = ts.pro_api()
if int(time.strftime("%H%M",time.localtime()))<=1600:
    lst_day=(datetime.date.today()-datetime.timedelta(days=1)).strftime('%Y%m%d')
else:
    lst_day = (datetime.date.today()).strftime('%Y%m%d')
today = time.strftime("%Y%m%d",time.localtime())
df_ = pro.query('trade_cal', start_date='20220101', end_date='20301201').sort_values(by='cal_date').reset_index(drop=True)
dts_open=list(df_[df_.is_open>0].cal_date)
df_=df_[(df_.is_open>0)&(df_.cal_date<=lst_day)]
lst_trd_day=str(df_.cal_date.values[-1])
nxt_trd_day=dts_open[dts_open.index(lst_trd_day)+1]


# In[3]:


root='..//..//原始数据_托管'
mls=os.listdir(root)
df=pd.read_csv(os.path.join(root,max([i for i in mls if i.startswith('全部_')])))

df.买入日期=pd.to_datetime(df.买入日期.astype(str))


dfraw=df[df.策略.str.startswith(('MaMlMix','MaMt'))]
dfraw.loc[:,'account']=''
dfraw.loc[dfraw.账户=='心宿二 开源 KMAX SM','account']='_xx2'
dfraw.loc[dfraw.账户=='狮子量化 华创 TRADEX SM','account']='_hc'
dfraw.loc[dfraw.账户=='狮子丑寅 开源 KMAX SM','account']='_cy'
dfraw.loc[dfraw.账户=='金牛 长江 QMT','account']='_cjjn'
dfraw.loc[dfraw.账户=='金牛alpha 长江 QMT','account']='_cjjnalpha'
dfraw.loc[dfraw.账户=='金牛丁丑 东吴 QMT','account']='_dwdc'
dfraw.loc[dfraw.账户=='量化PAPER','account']='_pp'
dfraw.loc[dfraw.账户=='狮子alpha 国联 QMT','account']='_gl'
dfraw.loc[dfraw.账户=='狮子alpha 国联 QMT','account']='_gl'
dfraw.loc[:,'策略']=dfraw.loc[:,'策略'].str.cat(dfraw.account)

dfraw=dfraw[(~dfraw.账户.str.contains('PAPER'))]
dfraw.loc[:,'策略大类']=dfraw.loc[:,'策略'].apply(lambda x:x.split('_')[0])

dfraw.loc[:,'wk']=dfraw.买入日期.apply(lambda x:x.week)
dfraw.loc[:,'mon']=dfraw.买入日期.apply(lambda x:x.month)
wks=sorted(list(dfraw.wk.unique()))
最近日期=dfraw.买入日期.max()


df[(df.策略.str.startswith('MaMlMix'))&(df.账户=='心宿二 开源 KMAX SM')]

ret_all=[]
for cl,dfraw_ in dfraw.groupby('策略'):
    def cal_tp(tp,v):
        t=pd.Series([tp.总收益.sum()/tp.买入金额.sum(),(tp.收益率>0).astype(int).mean(),len(tp)],index=['收益率','胜率','信号数'])
        t.name=v
        t.loc['信号数']=int(t.loc['信号数'])
        t.loc['胜率']=np.round(t.loc['胜率'],2)
        t.loc['收益率']=np.round(t.loc['收益率'],3)*100
        return t
    
    def get_ret(dfraw):
        ret=[]
        tp=dfraw[dfraw.买入日期==最近日期]
        ret.append(cal_tp(tp,'当日'))

        tp=dfraw[dfraw.wk==dfraw.wk.max()]
        ret.append(cal_tp(tp,'当周'))

        tp=dfraw[dfraw.wk.isin(wks[-2:])]
        ret.append(cal_tp(tp,'近2周'))

        tp=dfraw[dfraw.mon==dfraw.mon.max()]
        ret.append(cal_tp(tp,'当月'))

        ret=pd.concat(ret,axis=1).stack().reset_index()
        ret.columns=['类别','周期','取值']
        ret.loc[:,'B属性']=ret.类别.str.cat(ret.周期,sep='_')
        ret.loc[:,'A属性']=cl
        return ret
    ret=get_ret(dfraw_)
    ret_all.append(ret)
ret_all=pd.concat(ret_all)
ret_all.loc[:,'日期']=df.买入日期.max()

分析名称='MA收益胜率分析'
tp=ret_all[['日期','A属性','B属性','取值']]
tp.loc[:,'分析名称']=分析名称
path='..//产出//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)


# In[10]:


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


import os
command = 'jupyter nbconvert --to script *.ipynb'
os.system(command)

ret_all[ret_all.A属性=='MaMlMix_xx2']



tp=pd.read_csv('..//..//原始数据_托管//全部_%s.csv'%lst_trd_day)
tp=tp[tp.策略.isin(['MaAl1315','MaAl13'])]


res=None
tp=tp[tp.平仓率==1][tp.账户.str.contains('轩辕十四')][tp.买入日期>='2023-10-17']
账户列表=list(tp.账户.unique())
for v in 账户列表:
    clms=['买入数量','买入价格','卖出价格','收益率','总收益']
    t=tp[tp.账户==v][['买入日期','代码','名称','策略']+clms]
    dict_clms={}
    for vv in clms:
        dict_clms[vv]=v+'_'+vv
    t=t.rename(columns=dict_clms)
    if res is None:
        res=t
    else:
        res=res.merge(t,how='outer')



tp.groupby(['账户','策略']).总收益.sum()



res.to_excel('..//产出//MA.xlsx')


# In[17]:


tp.groupby(['买入日期','账户']).代码.count()


# In[24]:


import sys
sys.path.append('/home/jovyan/work/workspaces/daily report/实盘模型/模型文件/周频策略')
from EIFBT import *
DATA_PATH = '/home/jovyan/data/store/rsync/'
params={
'path':'/home/jovyan/data/store/rsync/data_daily/data_daily.pickle',
'path_':'/home/jovyan/work/commons/data/daily_data/stock_day_n.parquet',
        }
openw,highw,loww,closew,amtw,pavg=载入日线(params['path'],params['path_'])
t=closew.rolling(12).mean().shift(1)
t1=t.stack().rename('MA13').reset_index()
t=closew.rolling(14).mean().shift(1)
t2=t.stack().rename('MA15').reset_index()
t=openw
t3=t.stack().rename('open').reset_index()


# In[37]:


def format_stock_code(stock_code):
    # 补全为6位数字，前面用0补齐
    formatted_code = f"{int(stock_code):06d}"  # 将字符串转换为整数，然后进行整数格式化
    
    # 根据代码分为上证和深证
    exchange = "sh" if stock_code.startswith("6") else "sz"
    
    # 加上.sh或.sz后缀
    formatted_code_with_suffix = f"{formatted_code}.{exchange}"
    
    return formatted_code_with_suffix

tp=pd.read_csv('..//..//原始数据_托管//全部_%s.csv'%lst_trd_day)
tp=tp[tp.策略.isin(['MaAl13'])]
tp=tp[tp.平仓率==1][tp.账户.str.contains('轩辕十四')]
tp.loc[:,'date']=pd.to_datetime(tp.买入日期)
tp.loc[:,'code']=tp.代码.apply(lambda x:format_stock_code(str(x)))


# In[38]:


tp=tp.merge(t1,how='left')
tp=tp.merge(t2,how='left')
tp=tp.merge(t3,how='left')
tp.loc[:,'flag']=(tp.open>tp.MA13)&((tp.MA13>tp.MA15)&(tp.买入价格>tp.MA15)|(tp.MA15>tp.open))
tp.loc[:,'均线']=tp.flag.apply(lambda x:13 if x else 15)
tp.groupby('均线').收益率.agg(['mean','count'])
del tp['flag']


# In[35]:


tp.groupby('均线').收益率.agg(['mean','count'])


# In[43]:


tp.to_excel('..//产出//MA13_details_xyss.xlsx')


# In[23]:


tp.账户.unique()


# In[39]:


tp.groupby(['账户','均线']).收益率.agg(['mean','count'])


# In[42]:


tp.sort_values(by='买入金额')


# In[ ]:




