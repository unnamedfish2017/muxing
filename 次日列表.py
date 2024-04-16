#!/usr/bin/env python
# coding: utf-8

# In[1]:

DATA_PATH = '/home/vscode/workspace/data/store/rsync'
root_sc='/home/vscode/workspace/work/生产/素材'
root_save='/home/vscode/workspace/work/生产/产出/票池/其他'

import warnings
warnings.filterwarnings("ignore")
# from 载入数据3 import *
# data_daily=loadData2()
# for v in ['closew','openw','highw','loww','amtw']:
#     exec(v+'=data_daily[\''+v+'\']')
import pickle
import pandas as pd
import numpy as np
path=f'{DATA_PATH}/data_daily/data_daily.pickle'
with open(path, 'rb') as f:
    # The protocol version used is detected automatically, so we do not
    # have to specify it.
    data_daily = pickle.load(f)
for v in ['closew','openw','highw','loww','amtw']:
    exec(v+'=data_daily[\''+v+'\']')
N=17
MA_L=closew.rolling(N-1,min_periods=N-3).mean().shift(1)
MA_N=closew.rolling(N,min_periods=N-3).mean().shift(1)
BP=MA_L.copy()
BP[MA_L>openw]=openw[MA_L>openw]
T=30
K=0.5
S11=closew>closew.shift(T)*(1+K)
S12=amtw>0
S1=S11.fillna(False) & S12.fillna(False)
S_DOWN=(loww<MA_L) & (loww>=MA_L).shift(1)
S_UP=(closew>closew.rolling(N,min_periods=N-2).mean())&(closew<=closew.rolling(N,min_periods=N-2).mean()).shift(1)
跳空收阳=(loww>closew.shift(1)) & (closew>openw)
zdt=pd.DataFrame([],index=closew.index,columns=closew.columns)
zz=closew/closew.shift(1)-1
Z1=(-0.101<zz) & (zz<-0.099) | (-0.201<zz) & (zz<-0.199) | (-0.051<zz) & (zz<-0.049)
Z1=Z1 & (loww==closew)

Z2=(0.101>zz) & (zz>0.099) | (0.201>zz) & (zz>0.199) | (0.051>zz) & (zz>0.049)
Z2=Z2 & (highw==closew)

zdt[Z1]=-1
zdt[Z2]=1
zdt=zdt.fillna(0)
upmax20=pd.DataFrame([],index=closew.index,columns=closew.columns)
clms=[i for i in closew.columns if i.startswith('68')]
upmax20[clms]=1
clms=[i for i in closew.columns if i.startswith('300') or i.startswith('301')]
ind=[i for i in upmax20.index if i>=pd.to_datetime('2020-08-24')]
upmax20.loc[ind,clms]=1
upmax20=upmax20.fillna(0)
zz_h=highw/closew.fillna(method='ffill').shift(1)-1
盘中涨停含一字板=((zz_h>0.099) & (upmax20==0) | (zz_h>0.199) & (upmax20>0) )
盘中涨停=盘中涨停含一字板 & (loww<highw) ##剔除1字板
未封板=盘中涨停&(closew<highw)
dts=list(pd.to_datetime(closew.index))
stks=list(closew.columns)
names_cn=data_daily['WA_names_cn'].values

行业=pd.read_csv(f'{root_sc}/申万行业股票列表2022_renew.csv',encoding='gbk').drop_duplicates()
行业.code=行业.code.str.lower()
行业.rename(columns={'code':'股票代码'},inplace=True)


# In[2]:


import datetime

import tushare as ts
import time
import pandas as pd
import tushare as ts
tkpath=f'{root_sc}/tk.txt'
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


# In[3]:


stlist = pro.bak_basic(trade_date=lst_trd_day, fields='trade_date,ts_code,name')
stlist['l1'] = stlist['name'].apply(lambda x:x[:2])
stlist['l2'] = stlist['name'].apply(lambda x:x[:3])
data = stlist[(stlist['l1']=='ST' )| (stlist['l2']=='*ST')|(stlist['l2']=='S*S')]
stlist=list(data.ts_code.str.lower())


# In[5]:


amtw3=amtw.rolling(3,min_periods=1).mean()
tp=amtw3.iloc[-1,:]/10
tp=tp[tp<=1000]
stlist=list(set(stlist+list(tp.index)))

#%%capture
from ftplib import FTP
import time

class MFTP(FTP):
    def makepasv(self):
        host,port = super().makepasv()
        return (self.host,port)
    def download(self, remotepath, localpath=None):
        if '\\' in remotepath:
            filename = remotepath.split('\\')[-1]
            remotedir = remotepath.rsplit('\\',1)[0]
        elif '/' in remotepath:
            filename = remotepath.split('/')[-1]
            remotedir = remotepath.rsplit('/',1)[0]
        else:
            filename = remotepath
            remotedir = ''
        n = 0
        while filename not in self.nlst(remotedir):
            print('error: file not exists on ftp server')
            time.sleep(1)
            n+=1
            if n>3:
                return

        if localpath==None:
            localpath = filename
        bufsize = 1024
        fp = open(localpath, 'wb')
        self.retrbinary('RETR ' + remotepath, fp.write, bufsize)
        self.set_debuglevel(0)
        fp.close()
    def upload(self, remote_path, local_path):
        fp = open(local_path, "rb")
        buf_size = 1024
        self.storbinary("STOR {}".format(remote_path), fp, buf_size)
        fp.close()
import os
#ftp=MFTP()
# ftp.set_debuglevel(2)
# root='..//实盘信号//涨停'
# #ftp.connect("1.15.74.234",5270)
# ftp.connect("1.15.74.234",5270)
# # ftp.connect("192.168.31.77",57681)
# ftp.login("miyuan","miyuan@802")
# ftp.cwd('/signals')
# for ml in ftp.nlst(''):
#     #ml_new=ml[:8]+'.csv'
#     if ml not in os.listdir(root) and ml.endswith('.csv'):
#         ftp.download(ml.encode('utf-8').decode('latin1'),os.path.join(root,ml))


# In[7]:


tp=[]
for N in [13,15,17,19]:
    tmp=closew.rolling(N-1,min_periods=N-3).mean()
    t=pd.DataFrame((closew/tmp).iloc[-1,:].rename('前一日收盘距离均线')).reset_index()
    t=t[(t.前一日收盘距离均线>1)&(~t.code.str.endswith('.bj'))]
    t.loc[:,'最大涨跌幅20']=t.code.apply(lambda x:True if int(x[:2]) in (68,78,30) else False)
    t=t[((t.最大涨跌幅20)&(t.前一日收盘距离均线<=1.2))|(t.前一日收盘距离均线<=1.1)]
    t.loc[:,'均线']=N
    t.loc[:,'date']=dts[-1]
    tp.append(t)
pd.concat(tp).reset_index(drop=True).to_parquet(f'{root_save}//回调池_'+dts[-1].strftime('%Y%m%d')+'_MIX.parquet',index=False)


# In[8]:


近期信号=盘中涨停.shift(1)&((openw<closew.shift(1)*0.995)&(openw>closew.shift(1)*0.98))
近期信号=近期信号[近期信号].tail(5).stack().reset_index()
近期信号=近期信号.drop_duplicates(subset='code')
近期信号.columns=['近期日期','股票代码','近期出现']


# In[9]:


#tp.merge(近期信号,how='left')


# In[10]:


dts=list(pd.to_datetime(closew.index))
i=len(dts)-1
tp=盘中涨停.loc[dts[i],:]
tp=pd.DataFrame(tp[tp])
tp.loc[:,'股票名称']=np.nan
tp.loc[:,'当日回落']=np.nan
for stk in tp.index:
    try:
        name=data_daily['WA_names_cn'].loc[stk,'name']
    except:
        name=''
    tp.loc[stk,'股票名称']=name
    tp.loc[stk,'当日回落']=closew.loc[dts[i],stk]/highw.loc[dts[i],stk]
    tp.loc[stk,'前一日涨停']=(highw.loc[dts[i],stk]==closew.loc[dts[i],stk]).astype(int)
    tp.loc[stk,'前三天触及涨停数']=盘中涨停含一字板.astype(int).rolling(3).sum().loc[dts[i],stk]
tp=tp.reset_index()
tp.loc[:,'日期']=dts[i].strftime('%Y%m%d')

tp=tp['code,前一日涨停,日期,股票名称,当日回落,前三天触及涨停数'.split(',')]
tp.columns='股票代码,前一日涨停,日期,股票名称,当日回落,前三天触及涨停数'.split(',')
tp=tp[tp.当日回落>=0.96].reset_index(drop=True)
tp=tp.merge(行业[['股票代码','申万一级','申万二级']],how='left').merge(近期信号,how='left')
#tp.to_csv('..//次日列表//zhangting_'+dts[i].strftime('%Y%m%d')+'.csv',index=False,encoding='gbk')
tp.to_csv(f'{root_save}//zhangting_'+dts[i].strftime('%Y%m%d')+'_原始.csv',index=False,encoding='gbk')
