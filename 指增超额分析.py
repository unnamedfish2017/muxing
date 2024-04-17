#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import warnings
import os,pickle
warnings.filterwarnings('ignore')

DATA_PATH = '/home/vscode/workspace/data/store/rsync'
root_sc='/home/vscode/workspace/work/生产/素材'
root_save='/home/vscode/workspace/work/生产/产出/盘后分析'


# In[2]:


path=f'{DATA_PATH}/data_daily/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
for v in ['closew','openw','highw','loww','amtw']:
    exec(v+'=data_daily[\''+v+'\']')


# In[3]:


# %cd /home/jovyan/work/workspaces/daily report/每日报表/代码
# %run 托管数据预处理
# %run 每日持仓和资金预处理.py
# %cd /home/jovyan/work/workspaces/daily report/每日报表/盘后分析/代码


# In[4]:


#df[(df.日期==df.日期.max())&(df.策略=='ETLM88S_cy')]


# In[5]:


root=f'{root_sc}/托管数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('盘后权益')])))
df.日期=pd.to_datetime(df.日期.astype(str))

def 获取指增标记(df):
    #ind1=(df.策略.str.contains('EIF')) 
    #ind2=(df.策略.str.startswith(('LPP', 'LKW', 'LYK', 'TF98', 'ALSTM98', 'LPPM', 'GAT')) )
    #ind2=(df.策略.str.startswith(('LPP', 'LPPM', 'GAT')) )
    ind3=(df.策略.str.startswith(('E1', 'EA','E5','ET','E3','SIS')) )
    #指增标记=ind1|ind2|ind3
    ind4=(~df.策略.str.startswith(('EAW')).fillna(False) )
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
dfraw.loc[dfraw.账户=='金牛alpha 长江 QMT','account']='_jnalpha'
dfraw.loc[dfraw.账户=='金牛丁丑 东吴 QMT','account']='_jndc'
dfraw.loc[dfraw.账户=='狮子量化 华泰 MATIC','account']='_ht'
dfraw.loc[dfraw.账户=='木卫十 华泰 MATIC','account']='_mws'
dfraw.loc[dfraw.账户=='轩辕十四 国君 ATX','account']='_gtja'
dfraw.loc[dfraw.账户=='轩辕十四 华西 QMT','account']='_hx'

dfraw.loc[dfraw.账户=='壬午 国君 DMA','account']='_rwdma'
dfraw.loc[dfraw.账户=='双鱼 国君 DMA','account']='_sydma'
dfraw.loc[dfraw.账户=='射手 华泰 MATIC','account']='_htss'
dfraw.loc[dfraw.账户=='木卫十 华泰 MATIC','account']='_htmws'
dfraw.loc[dfraw.账户=='量化中性 开源 KMAX','account']='_kylhzx'

dfraw.loc[:,'策略']=dfraw.loc[:,'策略'].str.cat(dfraw.account)

dfraw=dfraw[(dfraw.持有市值>=100000)&(~dfraw.账户.str.contains('PAPER'))]
dfraw.loc[:,'策略大类']=dfraw.loc[:,'策略'].apply(lambda x:x.split('_')[0])

root=f'{root_sc}/托管数据'
mls=os.listdir(root)
tp=pd.read_csv(os.path.join(root,max([i for i in mls if i.startswith('全部_')])))
detailraw = tp[获取指增标记(tp)]

detailraw.loc[detailraw.账户=='心宿二 开源 KMAX SM','account']='_xx2'
detailraw.loc[detailraw.账户=='狮子量化 华创 TRADEX SM','account']='_hc'
detailraw.loc[detailraw.账户=='狮子丑寅 开源 KMAX SM','account']='_cy'
detailraw.loc[detailraw.账户=='量化PAPER','account']='_pp'
detailraw.loc[:,'策略']=detailraw.loc[:,'策略'].str.cat(detailraw.account)


# In[6]:


#dfraw[dfraw.日期==dfraw.日期.max()][dfraw.账户.str.contains('开源')]


# In[7]:


dfraw.loc[:,'累计资金变化']=dfraw.groupby(['账户','策略']).累计资金.diff()
dfraw=dfraw[np.abs(dfraw.累计资金变化)<100000]


# In[8]:


dfraw_=dfraw.copy()
def 记录合计(dfraw_,v):
    dfraw_=dfraw_.groupby(['日期'])['累计资金',  '盘后现金', '持有市值', '盘后权益', '盘前权益',
           '当日收益'].sum().reset_index().sort_values(by='日期')
    dfraw_.loc[:,'账户']=v
    dfraw_.loc[:,'策略']=v
    dfraw_.loc[:,'account']=''
    dfraw_.loc[:,'策略大类']=''
    dfraw_.loc[:,'当日收益率']=dfraw_.当日收益/(dfraw_.盘后权益-dfraw_.当日收益)
    dfraw_.loc[:,'当日收益率_剔除仓位变化']=dfraw_.当日收益/(dfraw_.持有市值.shift(1).fillna(np.nan))
    dfraw_=dfraw_[['日期', '账户', '策略', '累计资金',  '盘后现金', '持有市值', '盘后权益', '盘前权益',
           '当日收益', '当日收益率', '当日收益率_剔除仓位变化', 'account', '策略大类']]
    return dfraw_
dfraw=dfraw.append(记录合计(dfraw_,'合计'))


# In[9]:


for v in list(dfraw.account.unique()):
    if v=='':
        continue
    dfraw_=dfraw[dfraw.account==v]
    dfraw=dfraw.append(记录合计(dfraw_,'合计_'+v))


# In[10]:


df.策略.unique()


# In[11]:


指数列表={'zz500':'399905.SZ','hs300':'399300.SZ','zz1000':'000852.SH','zzqa':'000985.SH'}
for 基准 in 指数列表.keys():
    dfbase=pd.read_parquet(f'{root_sc}/指数基准/%s.parquet'%指数列表[基准])
#     if dfbase.index[-1]<df.日期.max():
#         %cd ..//../代码
#         %run 更新指数基准.py
#         %cd ..//盘后分析//代码


dts=list(closew.index)
dfraw=dfraw[dfraw.日期>=dts[-60]]



def get(基准):
    指数列表={'zz500':'399905.SZ','hs300':'399300.SZ','zz1000':'000852.SH','zzqa':'000985.SH'}
    dfbase=pd.read_parquet(f'{root_sc}/指数基准/%s.parquet'%指数列表[基准])
    dfbase.loc[:,基准]=dfbase.close.pct_change()
    df=dfraw.merge(dfbase.reset_index().rename(columns={'trade_date':'日期'})[['日期',基准]])
#     df.loc[:,'alpha']=df['当日收益率_剔除仓位变化']-df[基准]
#     df.loc[:,'alpha0']=df.当日收益率_剔除仓位变化
    df.loc[:,'alpha']=df['当日收益率']-df[基准]
    df.loc[:,'alpha0']=df.当日收益率

    df.sort_values(by=['日期','账户','策略'],inplace=True)
    df.loc[:,'wk']=df.日期.apply(lambda x:x.week)
    df.loc[:,'mon']=df.日期.apply(lambda x:x.month)
    def f1w(tp,alpha='alpha',收益=False):
        tp=tp[(tp.账户==tp.账户.values[-1])&(tp.策略==tp.策略.values[-1])]
        wks=list(tp.wk.unique())
        if 收益:
            return (tp[tp.wk.isin(wks[-1:])]['当日收益'].fillna(0)).cumsum().values[-1]
        return (tp[tp.wk.isin(wks[-1:])][alpha].fillna(0)+1).cumprod().values[-1]-1
    def f2w(tp,alpha='alpha',收益=False):
        tp=tp[(tp.账户==tp.账户.values[-1])&(tp.策略==tp.策略.values[-1])]
        wks=list(tp.wk.unique())
        if 收益:
            return (tp[tp.wk.isin(wks[-2:])]['当日收益'].fillna(0)).cumsum().values[-1]
        return (tp[tp.wk.isin(wks[-2:])][alpha].fillna(0)+1).cumprod().values[-1]-1
    def fm(tp,alpha='alpha',收益=False):
        tp=tp[(tp.账户==tp.账户.values[-1])&(tp.策略==tp.策略.values[-1])]
        if 收益:
            return (tp[tp.mon==tp.mon.values[-1]]['当日收益'].fillna(0)).cumsum().values[-1]
        return (tp[tp.mon==tp.mon.values[-1]][alpha].fillna(0)+1).cumprod().values[-1]-1
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
    
    ret_2w=[]
    ret_1m=[]
    ret_1w=[]
    for i in range(df.shape[0]):
        ret_1w.append(f1w(df.head(i+1),alpha=None,收益=True))
        ret_2w.append(f2w(df.head(i+1),alpha=None,收益=True))
        ret_1m.append(fm(df.head(i+1),alpha=None,收益=True))
    df.loc[:,'近2周实际盈亏']=ret_2w
    df.loc[:,'本月实际盈亏']=ret_1m
    df.loc[:,'本周实际盈亏']=ret_1w
    
    ret_2w=[]
    ret_1m=[]
    ret_1w=[]
    for i in range(df.shape[0]):
        ret_1w.append(f1w(df.head(i+1),'alpha0'))
        ret_2w.append(f2w(df.head(i+1),'alpha0'))
        ret_1m.append(fm(df.head(i+1),'alpha0'))
    df.loc[:,'近2周收益']=ret_2w
    df.loc[:,'本月收益']=ret_1m
    df.loc[:,'本周收益']=ret_1w
    
    def cal_dd(x):  
        t = (x.fillna(0) + 1).cumprod()  
        return t / t.rolling(len(t)).max() - 1  
    def con_dd(x):
        ret=[]
        for i in x.fillna(0).values:
            if len(ret)==0:
                if i>=0:
                    ret.append(0)
                else:
                    ret.append(1)
            else:
                if i>=0:
                    ret.append(0)
                else:
                    ret.append(ret[-1]+1)
        return ret
    def con_dd_pct(x):
        ret=[]
        for i in x.fillna(0).values:
            if len(ret)==0:
                if i>=0:
                    ret.append(0)
                else:
                    ret.append(i)
            else:
                if i>=0:
                    ret.append(0)
                else:
                    ret.append(ret[-1]+i)
        return ret

    def calculate_drawdown_days(x):
        max_idx_in_window = np.argmax(x) # 滚动窗口内最大值的位置
        current_idx = len(x)-1  # 当前日期的位置
        drawdown_days = current_idx - max_idx_in_window  # 计算回撤天数
        return drawdown_days

    def cal_dd_days(x):  
        t = (x.fillna(0) + 1).cumprod()  
        return t.rolling(len(t)).apply(calculate_drawdown_days, raw=True)

    df.loc[:,'超额当前回撤']=df.groupby(['策略','账户'])['alpha'].transform(lambda x: cal_dd(x))
    df.loc[:,'超额当前回撤天数']=df.groupby(['策略','账户'])['alpha'].transform(lambda x: cal_dd_days(x))
    df.loc[:,'超额连续回撤天数']=df.groupby(['策略','账户'])['alpha'].transform(lambda x: con_dd(x))
    df.loc[:,'超额连续回撤']=df.groupby(['策略','账户'])['alpha'].transform(lambda x: con_dd_pct(x))
    
    df.loc[:,'持有市值w']=np.round(df.持有市值/10000,1)
    df[['当日收益率','alpha','本周超额','近2周超额','本月超额']]=np.round(df[['当日收益率','alpha','本周超额','近2周超额','本月超额']]*100,1)
    df[['本周收益','近2周收益','本月收益']]=np.round(df[['本周收益','近2周收益','本月收益']]*100,1)
    df=df[(df.持有市值>300000)&(~df.账户.str.contains('PAPER'))]
    return df
#{'zz500':'399905.SZ','hs300':'399300.SZ','zz1000':'000852.SH'}    
基准='zz1000'
df1=get(基准)
df1=df1[df1.策略.str.startswith('E1')]

基准='zzqa'
df2=get(基准)
df2=df2[df2.策略.str.startswith(('EA','ET','合计','SIS'))]

基准='zz500'
df3=get(基准)
df3=df3[df3.策略.str.startswith(('E5'))]

基准='hs300'
df4=get(基准)
df4=df4[df4.策略.str.startswith(('E3'))]


df=df1.append(df2).append(df3).append(df4).reset_index(drop=True)#.rename(columns={'当日收益率_剔除仓位变化':'当日收益率'})


df=df[df.日期>=dts[-20]]



df[['超额当前回撤','超额连续回撤']]=np.round(df[['超额当前回撤','超额连续回撤']]*100,1)


df[df.策略=='SISUBA_rwdma']


#df.to_excel('策略超额汇总.xlsx',index=False)



分析名称='指增策略超额分析'
分析列=['持有市值w','当日收益率','当日超额','本周收益','本周超额','近2周收益','近2周超额','本月收益','本月超额','超额当前回撤','超额当前回撤天数','超额连续回撤','超额连续回撤天数']
tp=df.rename(columns={'alpha':'当日超额'})[df.日期==df.日期.max()][['日期','账户','策略']+分析列]
tp=tp.set_index(['日期','策略'])[分析列].stack().reset_index()
tp.columns=['日期','A属性','B属性','取值']
tp.loc[:,'分析名称']=分析名称
path=f'{root_save}//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)


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


# dts_tp=list(df[df.日期>='2023-05-01'].日期.unique())
# for dt in dts_tp:
#     分析名称='指增策略超额分析'
#     分析列=['持有市值w','当日收益率','当日超额','本周收益','本周超额','近2周收益','近2周超额','本月收益','本月超额','超额当前回撤','超额当前回撤天数','超额连续回撤','超额连续回撤天数']
#     tp=df.rename(columns={'alpha':'当日超额'})[df.日期==dt][['日期','账户','策略']+分析列]
#     tp=tp.set_index(['日期','策略'])[分析列].stack().reset_index()
#     tp.columns=['日期','A属性','B属性','取值']
#     tp.loc[:,'分析名称']=分析名称
#     path='..//产出//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
#     tp.loc[:,'日期']=tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
#     tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)
    
#     import requests

#     url = "https://61.172.245.225:26829/profit-mgt/api/v1/posthours-analysis/import"
#     payload = {}
#     files = [('file',
#               (path.split('/')[-1], open(path, 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))]
#     headers = {
#         'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
#         "User-Agent": "curl/7.78.0"
#                }
#     response = requests.request("POST", url, headers=headers, data=payload, files=files, verify=False)
#     print(response.text)



tp=df.rename(columns={'alpha':'当日超额'})[df.日期==df.日期.max()][['日期','账户','策略']+分析列].reset_index(drop=True)


tp.loc[:,'基准']=tp.loc[:,'策略'].apply(lambda x:x[:2])
tp.loc[:,'算法']=tp.loc[:,'策略'].apply(lambda x:x[2:4])


# In[27]:


import requests
#root='..//output'
root=f'{root_save}//汇总_指增超额'
filename='汇总_指增超额.xlsx'
path=os.path.join(root,filename)

import random
#tp.columns='类型 策略 当日收益率 当日超额 本周超额 近2周超额 本月超额'.split(' ')
#tp.columns=list(range(len(tp.columns)))
tp.to_excel(path,index=False)

#url="https://61.172.245.225:26829/profit-mgt/internal/excel/import/strategy-profit-table"
url="https://61.172.245.225:26829/profit-mgt/internal/excel/summaries/profit"

payload={}
files=[
('file',(filename,open(path,'rb'),'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
]
headers = {
    'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
    "User-Agent": "curl/7.78.0"
           }

response = requests.request("POST", url, headers=headers, data=payload, files=files, verify=False)
print(response.text)


# In[28]:


clms='日期,账户,策略,持有市值w,当日收益率,当日超额,当日实际盈亏,本周收益,本周超额,本周实际盈亏,近2周收益,近2周超额,近2周实际盈亏,本月收益,本月超额,本月实际盈亏,基准,算法'.split(',')
df.loc[:,'基准']=df.策略.apply(lambda x:x[:2])
df.loc[:,'算法']=df.策略.apply(lambda x:x[2:4])
path='..//产出//汇总_指增超额//%s.xlsx'%closew.index[-1].strftime('%Y%m%d')
dfout=df.rename(columns={'alpha':'当日超额','当日收益':'当日实际盈亏'})[clms]
clms_=[i for i in clms if '实际盈亏' in i]
dfout[clms_]=np.round(dfout[clms_]/10000,2)
dfout=dfout[~dfout.策略.str.contains('合计')].reset_index(drop=True)
#dfout.loc[:,'日期']=dfout.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
dfout.to_excel(path)

url="https://61.172.245.225:26829/profit-mgt/api/v1/posthours-analysis/excess-returns-table/import"
payload={}
files=[
('file',(filename,open(path,'rb'),'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
]
headers = {
    'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
    "User-Agent": "curl/7.78.0"
           }

response = requests.request("POST", url, headers=headers, data=payload, files=files, verify=False)
print(response.text)

dfout.算法.unique()
dfout[dfout.策略=='SISIG_sydma']

