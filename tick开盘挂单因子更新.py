#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import sys,os
from scipy import stats
sys.path.append('..')
import pickle
import pandas as pd
import numpy as np
DATA_PATH = '/home/vscode/workspace/data/store/rsync'

path=f'{DATA_PATH}/data_daily/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
closew=data_daily['closew']
dts=list(closew.index)

def get_(stk,dt):
    dt=str(dt)
    y=dt[:4]
    m=dt[4:6]
    d=dt[6:]
    path='/home/vscode/workspace/data/store/stock/data_/second_3'+'/year='+str(y)+'/month='+str(m)+'/date='+str(d)+'/'+stk.lower()+'.parquet'


    if not os.path.exists(path):
        return None
    df_sec=pd.read_parquet(path, engine='pyarrow').reset_index(drop=True)
    df_sec=df_sec[df_sec.Open>0]
    t1=df_sec[df_sec.Time<=92600000].drop_duplicates(subset=['Date'],keep='last')
    t1.loc[:,'seg']='早上'
    t2=df_sec[df_sec.Time>=130000000].drop_duplicates(subset=['Date'],keep='first')
    t2.loc[:,'seg']='下午'
    df=t1.append(t2)
    for k in [1,5,10]:
        df.loc[:,'BidAmt_'+str(k)]=(df[['BidPrice'+'0'*(2-len(str(i)))+str(i) for i in range(1,k+1)]]*df[['BidVolume'+'0'*(2-len(str(i)))+str(i) for i in range(1,k+1)]].values).sum(axis=1)
        df.loc[:,'BidVol_'+str(k)]=(df[['BidVolume'+'0'*(2-len(str(i)))+str(i) for i in range(1,k+1)]]).values.sum(axis=1)
        df.loc[:,'BidPrice_'+str(k)]=df.loc[:,'BidAmt_'+str(k)]/df.loc[:,'BidVol_'+str(k)]

        df.loc[:,'AskAmt_'+str(k)]=(df[['AskPrice'+'0'*(2-len(str(i)))+str(i) for i in range(1,k+1)]]*df[['AskVolume'+'0'*(2-len(str(i)))+str(i) for i in range(1,k+1)]].values).sum(axis=1)
        df.loc[:,'AskVol_'+str(k)]=(df[['AskVolume'+'0'*(2-len(str(i)))+str(i) for i in range(1,k+1)]]).sum(axis=1)
        df.loc[:,'AskPrice_'+str(k)]=df.loc[:,'AskAmt_'+str(k)]/df.loc[:,'AskVol_'+str(k)]

        df.loc[:,'BA_Amt_'+str(k)]=df.loc[:,'BidAmt_'+str(k)]/df.loc[:,'AskAmt_'+str(k)]
        df.loc[:,'BA_Vol_'+str(k)]=df.loc[:,'BidVol_'+str(k)]/df.loc[:,'AskVol_'+str(k)]

    df.loc[:,'TotolBidAmt']=df.loc[:,'TotalBidVolume']*df.loc[:,'BidAvgPrice']
    df.loc[:,'TotolAskAmt']=df.loc[:,'TotalAskVolume']*df.loc[:,'AskAvgPrice']
    df.loc[:,'BA_Amt']=df.loc[:,'TotolBidAmt']/df.loc[:,'TotolAskAmt']
    df.loc[:,'BA_Vol']=df.loc[:,'TotalBidVolume']/df.loc[:,'TotalAskVolume']
    clms=['Code', 'Date', 'seg', 'BidAmt_1', 'BidVol_1', 'BidPrice_1', 'AskAmt_1',
       'AskVol_1', 'AskPrice_1', 'BA_Amt_1', 'BA_Vol_1', 'BidAmt_5',
       'BidVol_5', 'BidPrice_5', 'AskAmt_5', 'AskVol_5', 'AskPrice_5',
       'BA_Amt_5', 'BA_Vol_5', 'BidAmt_10', 'BidVol_10', 'BidPrice_10',
       'AskAmt_10', 'AskVol_10', 'AskPrice_10', 'BA_Amt_10', 'BA_Vol_10',
       'TotolBidAmt', 'TotolAskAmt', 'BA_Amt', 'BA_Vol']
    return dt,stk,df[clms]


df=pd.read_parquet('/home/jovyan/work/commons/data//factor_data//开盘挂单因子.parquet')
dt_=str(df.date.max())
dts_add=[i.strftime('%Y%m%d') for i in dts if i>=pd.to_datetime(dt_)]

cs=[]
for stk in list(closew.columns):
    for dt in dts_add:
        cs.append((stk,dt))

import numpy as np
数据={}
from multiprocessing import Queue, Process, Pool
import os

def add(传入):
    global 数据
    if 传入 is None:
        return
    dt=传入[0]
    stk=传入[1]
    try:
        数据[stk+'_'+dt]=传入[2]
    except:
        #print(dt,'error')
        pass
    return

def get_pool(n=20,cs=[]):
    p = Pool(n) # 设置进程池的大小
    for v in cs:
        p.apply_async(get_, (v[0],v[1],),callback=add)   #维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去

    p.close() # 关闭进程池
    p.join()
get_pool(30,cs)
tp=[v for k,v in 数据.items()]
df_add=pd.concat(tp).reset_index(drop=True)
df_add.rename(columns={'Code':'code','Date':'date'},inplace=True)
df_add.loc[:,'date']=pd.to_datetime(df_add.loc[:,'date'].astype(str))

def modify(df_all):
    tp=pd.read_csv('/home/vscode/workspace/data/store/rsync/data_daily//申万行业股票列表2022_renew.csv',encoding='gbk')
    tp=tp[['code','申万一级','申万二级']].fillna('')
    df_all=df_all.merge(tp,how='left')

    t0=df_all.groupby('date')
    t1=(t0.TotolBidAmt.sum()/t0.TotolAskAmt.sum()).rename('BA_Amt_dp').reset_index()
    t2=t1.set_index('date')['BA_Amt_dp'].rolling(21).apply(lambda a:np.argsort(np.argsort(a.values))[-1]*5).rename('BA_Amt_dp_qt').reset_index()
    df_all=df_all.merge(t1,how='left').merge(t2,how='left')

    def get_hy_qt(df_all,nm1,nm2,nm3):
        t0=df_all.groupby(['date',nm1])
        t1=(t0.TotolBidAmt.sum()/t0.TotolAskAmt.sum()).rename(nm2).reset_index()
        t2=[]
        for v,t in t1.groupby(nm1):
            t.loc[:,nm3]=t.sort_values(by='date')[nm2].rolling(21).apply(lambda a:np.argsort(np.argsort(a.values))[-1]*5).rename(nm3)
            t2.append(t)
        t2=pd.concat(t2)
        return t2

    nm1='申万二级'
    nm2='BA_Amt_hy2'
    nm3='BA_Amt_hy2_qt'
    t2=get_hy_qt(df_all,nm1,nm2,nm3)
    df_all=df_all.merge(t2,how='left')

    nm1='申万一级'
    nm2='BA_Amt_hy1'
    nm3='BA_Amt_hy1_qt'
    t2=get_hy_qt(df_all,nm1,nm2,nm3)
    df_all=df_all.merge(t2,how='left')

    for v in ['申万一级','申万二级']:
        del df_all[v]
    return df_all
df_tp=(df[df.date.isin(dts[-100:])][df_add.columns]).append(df_add).reset_index(drop=True)
df_add=modify(df_tp)
df_add=df_add[df_add.date.isin(dts_add)]

df_all=df.append(df_add)
df_all=df_all.drop_duplicates(subset=['code','date','seg']).reset_index(drop=True)
df_all.to_parquet('/home/jovyan/work/commons/data//factor_data//开盘挂单因子.parquet')