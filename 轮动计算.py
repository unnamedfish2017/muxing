#!/usr/bin/env python
# coding: utf-8

# In[1]:


from ftplib import FTP
import time,datetime
root_sc='/home/vscode/workspace/work/生产/素材'
DATA_PATH = '/home/vscode/workspace/data/store/rsync'
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
            if n>2:
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
ftp=MFTP()
ftp.set_debuglevel(2)

#ftp.connect("42.192.118.237",57681)
# ftp.connect("42.192.118.237",5270)
# ftp.login("miyuan","miyuan@802")

# ftp.connect("42.192.118.237",5277)
# ftp.login("miyuan","miyuan@8.02")
ftp.connect("1.15.74.234",5277)
ftp.login("miyuan","miyuan@8.02")

ftp.cwd('/industry')
ftp.download('data2.xlsm',f'{root_sc}/轮动原始数据/申万指数.xlsm')
ftp.download('data2.xlsm',f'{root_sc}/轮动原始数据/行业指数//data2_%s.xlsm'%datetime.datetime.now().strftime('%Y%m%d'))
ftp.close()


# In[2]:


import pandas as pd
import numpy as np
import datetime
import time
import math
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")
close = pd.read_excel(f'{root_sc}/轮动原始数据/申万指数.xlsm',sheet_name='收盘价', header=[0,1,2])
close = close.droplevel([1,2],axis=1)
close.columns=close.columns.astype(str)
close.set_index(close.columns[0],inplace=True)
code = pd.read_excel(f'{root_sc}/轮动原始数据/申万指数.xlsm',sheet_name='code', header=None)
close.columns=close.columns.astype(str)
close.index.name='date'
close.index=pd.to_datetime(close.index)

close.to_parquet(f'{DATA_PATH}/data_daily/close_ths.parquet')


# In[3]:


import pickle
warnings.filterwarnings("ignore")
path=f'{DATA_PATH}/data_daily/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
for v in ['closew']:
    exec(v+'=data_daily[\''+v+'\']')


# In[4]:


if not close.index[-1]==closew.index[-1]:
    行业轮动数据未更新


# In[5]:


ST个股 = pd.read_excel(f'{root_sc}/轮动原始数据/申万指数.xlsm',sheet_name='ST', header=[0])
ST个股.head()
ST个股.loc[:,'代码']=ST个股.loc[:,'证券代码'].str.lower()
ST个股.to_csv(f'{DATA_PATH}/data_daily/STlist/%s.csv'%close.index[-1].strftime('%Y%m%d'),index=False)
ST个股.to_csv(f'{DATA_PATH}/data_daily/STlist.csv',index=False)


# In[6]:


个股 = pd.read_excel(f'{root_sc}/轮动原始数据/申万指数.xlsm',sheet_name='个股', header=[0])
个股.head()
个股.columns=['code','name','申万一级','申万二级']
个股.code=个股.code.str.lower()
个股.to_csv(f'{root_sc}/申万行业股票列表2022_renew.csv',encoding='gbk')


# In[7]:


import tushare as ts
import pandas as pd
tkpath=f'{root_sc}/tk.txt'
tk=pd.read_csv(tkpath).columns[0]
ts.set_token(tk)
pro = ts.pro_api()
# df1 = pro.index_classify(level='L1', src='SW2021')
# index_code = df1.index_code.tolist()

# ret = []
# for i in index_code:
#     index_data = pro.sw_daily(ts_code= i)
#     ret.append(index_data)
# df_2021 = pd.concat(ret).reset_index(drop = True)

# df2 = pro.index_classify(level='L1', src='SW2014')
# index_code = df2.index_code.tolist()

# ret = []
# for i in index_code:

#     index_data = pro.sw_daily_2014(ts_code= i)

#     ret.append(index_data)
# df_2014 = pd.concat(ret).reset_index(drop = True)

# data_sw = pd.concat([df_2014,df_2021]).drop_duplicates(keep = 'last').sort_values(['ts_code','trade_date'],ascending = [True,True])


# In[8]:


# df1 = pro.index_classify(level='L2', src='SW2021')
# index_code = df1.index_code.tolist()

# ret = []
# for i in index_code:
#     index_data = pro.sw_daily(ts_code= i)
#     ret.append(index_data)
# df_2021 = pd.concat(ret).reset_index(drop = True)

# df2 = pro.index_classify(level='L2', src='SW2014')
# index_code = df2.index_code.tolist()

# ret = []
# for i in index_code:

#     index_data = pro.sw_daily_2014(ts_code= i)

#     ret.append(index_data)
# df_2014 = pd.concat(ret).reset_index(drop = True)

# data_sw_lv2 = pd.concat([df_2014,df_2021]).drop_duplicates(keep = 'last').sort_values(['ts_code','trade_date'],ascending = [True,True])
# data_sw_lv2.to_parquet('data_sw_lv2.parquet')


# In[9]:


# data_sw.to_parquet('data_sw.parquet')
# data_sw_lv2=pd.read_parquet('data_sw_lv2.parquet')
# data_sw=data_sw.append(data_sw_lv2).drop_duplicates(subset=['ts_code','trade_date'])


# In[10]:


t1=close.stack().rename('close').reset_index().rename(columns={'level_1':'code'})
# data_sw.loc[:,'date']=pd.to_datetime(data_sw.trade_date.astype(str))
# data_sw.loc[:,'code']=data_sw.ts_code.apply(lambda x:x.replace('SI','SL'))
# t2=data_sw[['date','code','close']]
# close=t1.append(t2).drop_duplicates(subset=['date','code'],keep='last').pivot(index='date',columns='code',values='close').sort_index()
close=t1.drop_duplicates(subset=['date','code'],keep='last').pivot(index='date',columns='code',values='close').sort_index()
change=(close.pct_change())*100
change.columns=change.columns.astype(str)


# In[11]:


df_300 = pro.index_daily(ts_code='399300.SZ')
df_300=df_300.set_index('trade_date')[['pct_chg']].rename(columns={'pct_chg':'000300.SH'})
df_300=df_300.sort_index()
df_300.index=pd.to_datetime(df_300.index)
df_300=df_300.loc[close.index]
df_300.index.name='date'


# In[12]:


change_300 = df_300
change_300s = pd.concat([change_300]*change.shape[1], axis=1)
change_300s.columns = change.columns
rchange = change - change_300s

alpha20 = rchange.rolling(window=20).sum()
std20 = rchange.rolling(window=20).std()
alpha_std_20 = alpha20/std20/np.sqrt(20)
x_alpha20 = alpha_std_20.rolling(window=5).mean()

alpha30 = rchange.rolling(window=30).sum()
std30 = rchange.rolling(window=30).std()
alpha_std_30 = alpha30/std30/np.sqrt(30)
x_alpha30 = alpha_std_30.rolling(window=20).mean()

code_all=list([i for i in x_alpha20.columns])
code_hy1=[i for i in code_all if i.startswith('801') and i.endswith('0.SL')]
code_hy2=[i for i in code_all if i.startswith('801') and i.endswith('.SL') and i not in code_hy1]


# In[13]:


t1=x_alpha20.stack().rename('xalpha20').reset_index()
t1.columns=['date','hy_code','xalpha20']

t2=x_alpha20.diff(2).stack().rename('xalpha20_inc2').reset_index()
t2.columns=['date','hy_code','xalpha20_inc2']

t3=x_alpha30.stack().rename('xalpha30').reset_index()
t3.columns=['date','hy_code','xalpha30']

t4=x_alpha30.diff(2).stack().rename('xalpha30_inc2').reset_index()
t4.columns=['date','hy_code','xalpha30_inc2']

t5=(close/close.shift(1)-1).stack().rename('pct_chg').reset_index()
t5.columns=['date','hy_code','pct_chg']

t=t1.merge(t2,how='left').merge(t3,how='left').merge(t4,how='left').merge(t5,how='left')


# In[14]:


def get_dd(tp):
    pre_i=0
    pre_h=0
    out=[(0,0)]
    for i in range(1,len(tp)):
        if tp[i-1]<=0 and tp[i]>0:
            pre_i=i
            pre_h=tp[i]
        pre_h=max(pre_h,tp[i])
        if pre_h==tp[i]:
            pre_i=i
        out.append([i-pre_i,tp[i]-pre_h])
    return pd.DataFrame(out)[1].values

def get_dd_n(tp):
    pre_i=0
    pre_h=0
    out=[(0,0)]
    for i in range(1,len(tp)):
        if tp[i-1]<=0 and tp[i]>0:
            pre_i=i
            pre_h=tp[i]
        pre_h=max(pre_h,tp[i])
        if pre_h==tp[i]:
            pre_i=i
        out.append([i-pre_i,tp[i]-pre_h])
    return pd.DataFrame(out)[0].values
t6=x_alpha20.apply(lambda x:get_dd_n(x))
t7=x_alpha20.apply(lambda x:get_dd(x))
t8=x_alpha30.apply(lambda x:get_dd_n(x))
t9=x_alpha30.apply(lambda x:get_dd(x))

t6=t6.stack().rename('alpha20_ddn').reset_index()
t6.columns=['date','hy_code','alpha20_ddn']

t7=t7.stack().rename('alpha20_dd').reset_index()
t7.columns=['date','hy_code','alpha20_dd']

t8=t8.stack().rename('alpha30_ddn').reset_index()
t8.columns=['date','hy_code','alpha30_ddn']

t9=t9.stack().rename('alpha30_dd').reset_index()
t9.columns=['date','hy_code','alpha30_dd']

t=t1.merge(t2,how='left').merge(t3,how='left').merge(t4,how='left').merge(t5,how='left').merge(t6,how='left').merge(t7,how='left').merge(t8,how='left').merge(t9,how='left')


# In[15]:


for v in t.columns[2:6]:
    t.loc[t.hy_code.isin(code_hy1),v+'_1rk']=t.loc[t.hy_code.isin(code_hy1)].groupby('date')[v].rank().values
    t.loc[t.hy_code.isin(code_hy2),v+'_2rk']=t.loc[t.hy_code.isin(code_hy2)].groupby('date')[v].rank().values


# In[16]:


tp=pd.read_excel(f'{root_sc}/轮动原始数据/申万指数.xlsm',sheet_name='收盘价', header=[0,1]).columns
tp=[[i[0],i[1]] for i in tp[1:]]
t.merge(pd.DataFrame(tp).rename(columns={0:'hy_code',1:'hy_name'}),how='left').to_parquet('/home/vscode/workspace/work/生产/产出/行业轮动/行业因子.parquet')


# In[17]:


# path_to='/home/jovyan/work/commons/data/industry_data'
# path_from='./'
# import shutil,os
# for ml in ['行业因子.parquet','申万指数.xlsm']:
#     source=os.path.join(path_from,ml)
#     target=os.path.join(path_to,ml)
#     shutil.copy(source, target)



# df = pro.fund_basic(market='E')
# df=df[pd.isna(df.delist_date)].sort_values(by='delist_date',ascending=False)
# df[(df.invest_type=='被动指数型')|(df.name.str.contains('ETF'))].reset_index(drop=True).to_csv('场内etf列表.csv',encoding='gbk',index=False)

