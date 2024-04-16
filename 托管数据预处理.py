#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pickle
import pandas as pd
import datetime
import numpy as np

DATA_PATH = '/home/vscode/workspace/data/store/rsync'
root_sc='/home/vscode/workspace/work/生产/素材'
root_save='/home/vscode/workspace/work/生产/产出/票池/其他'

path=f'{DATA_PATH}/data_daily/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
closew=data_daily['closew'].fillna(method='ffill')
tp=data_daily['WA_names_cn'].reset_index()
tp.loc[:,'代码']=tp.code.apply(lambda x:'SHSE.S+'+x[:6] if x[-2:]=='sh' else 'SZSE.S+'+x[:6] if x[-2:]=='sz' else '')
tp.loc[:,'收盘价']=closew.loc[:,tp.code].iloc[-1,:].values
收盘价=tp
lst_trd_day=closew.index[-1].strftime('%Y%m%d')


# In[2]:


from ftplib import FTP
import os
import utils
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
            if n>5:
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


# In[3]:


#%%capture
import os
ftp=MFTP()
ftp.encoding='utf-8'
ftp.set_debuglevel(2)
root=f'{root_sc}/托管数据'
#ftp.connect("1.15.74.234",57681)
# ftp.connect("192.168.31.77",57681)
# ftp.connect("1.15.74.234",5270) #33
# ftp.login("miyuan","miyuan@802")

ftp.connect("1.15.74.234",5277)
ftp.login("miyuan","miyuan@8.02")

券商list=[('华创','HC'),('开源','KY'),('国联','QMT'),('东吴','DW'),('华泰','HT'),('国君','GT')]

for (券商,标记) in 券商list:
    ftp.cwd('/托管服务器/%sStrategyCache/DailyExport'%券商)
    mls=ftp.nlst()
    for ml in mls:
        mlto=None
        if ml.endswith('.csv'):
            mlto=os.path.join(root,ml.encode('latin1').decode('utf-8'))[:-4]+'_%s.csv'%标记
        elif ml.endswith('.xlsx'):
            mlto=os.path.join(root,ml.encode('latin1').decode('utf-8'))[:-5]+'_%s.xlsx'%标记
        #if not (os.path.exists(mlto) and os.path.getsize(mlto)>=1000):
        if mlto is not None and not (os.path.exists(mlto)):
            ftp.download(ml,mlto)

import requests,json
def feishu_message(内容):
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/78d4cfc5-0dd3-46f7-8a6a-5abb443c86cb"
    payload_message = {
     "msg_type": "text",
     "content": {
     "text": 内容
        }
    }
    headers = {
     'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload_message))
def feishu_message_tongbu(内容):
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/c9aa3dcb-eebe-4ca8-952d-6b969a9a2166"
    payload_message = {
     "msg_type": "text",
     "content": {
     "text": 内容
        }
    }
    headers = {
     'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload_message))


mls=os.listdir(root)
def get_ml(v,dt,prd):
    if v in ['Deposit_Withdrawal_History','Orders']:
        return dt+'_'+v+'_'+prd+'.csv'
    elif v in ['export_position','export_strategy']:
        return v+'_'+dt+'_'+prd+'.xlsx'
for prd in ['KY','HC','GT','QMT','HT','DW']:
    for v in ['export_position','export_strategy']+['Deposit_Withdrawal_History','Orders']:
        dt=closew.index[-1].strftime('%Y%m%d')
        dt_pre=closew.index[-2].strftime('%Y%m%d')
        path1=get_ml(v,dt,prd)
        path2=get_ml(v,dt_pre,prd)
        if not (os.path.exists(os.path.join(root,path1)) and os.path.exists(os.path.join(root,path2))):
            feishu_message('QT 文件缺失 %s'%path1)
            feishu_message_tongbu('QT 文件大小存疑 %s'%path1)
            continue
        f1=os.path.join(root,path1)
        f2=os.path.join(root,path2)
        bl=(os.path.getsize(f1)/float(os.path.getsize(f2)))
        if os.path.getsize(f1)<100 or os.path.getsize(f2)<100 or bl>1.5 or bl<0.2:
            feishu_message('QT 文件大小存疑 %s'%path1)
            feishu_message_tongbu('QT 文件大小存疑 %s'%path1)



import pickle
import pandas as pd
import numpy as np
import os,pickle
import pandas as pd

from 报表函数 import *
ios_all,ods_all=get_records(root)

ios_all.to_csv(os.path.join(root,'出入金汇总.csv'),index=False)
ods_all.to_csv(os.path.join(root,'交易汇总.csv'),index=False)
ods_all_raw=ods_all.copy()

ods_all=ods_all[ods_all.TradedPrice.astype(float)>0].sort_values(by='DealDateTime').reset_index(drop=True)
ods_all[['TradedVolume','RequestVolume']]=ods_all[['TradedVolume','RequestVolume']].astype(int)
ods_all[['TradedPrice']]=ods_all[['TradedPrice']].astype(float)
ods_all.loc[:,'TradedAmount']=ods_all.loc[:,'TradedVolume']*ods_all.loc[:,'TradedPrice']
ods_all.loc[:,'代码']=ods_all.loc[:,'Instrument']



ods_all.AccountName.unique()

ods_all.StrategyName.unique()

策略映射=pd.read_csv(f'{root_sc}/策略映射.csv',encoding='gbk').set_index('实际策略名').to_dict()['跟踪策略名']
账户=pd.read_csv(f'{root_sc}/账户.csv',encoding='gbk').set_index('实际账户名').to_dict()['跟踪账户名']
tp=pd.DataFrame.from_dict(策略映射,orient='index').reset_index()
tp.columns=['StrategyName','策略']
ods_all=ods_all.merge(tp,how='left')

tp=pd.DataFrame.from_dict(账户,orient='index').reset_index()
tp.columns=['AccountName','账户']
ods_all=ods_all.merge(tp,how='inner')   #####只统计这些账户的表现

ods_all.loc[:,'日期']=pd.to_datetime(ods_all.DealDateTime.apply(lambda x:x.strftime('%Y%m%d')))



ods_all[pd.isna(ods_all.策略)].StrategyName.unique()



t=ods_all[ods_all.BuySell=='Buy'].groupby(['日期','代码','策略','账户'])
tp=pd.concat([t.TradedVolume.sum().rename('买入数量'),t.TradedAmount.sum().rename('买入金额')],axis=1).reset_index()
tp.loc[:,'ind']=tp.账户.str.cat(tp.策略,sep='-').str.cat(tp.代码,sep='-')
tp.loc[:,'持仓']=tp.loc[:,'买入数量']
tp.loc[:,'卖出数量']=0
tp.loc[:,'卖出金额']=0
tp.loc[:,'平仓收益']=0
tp.loc[:,'浮动收益']=0
tp.loc[:,'最后操作日期']=tp.loc[:,'日期']
tp.loc[:,'买入价格']=tp.买入金额/tp.买入数量


# In[17]:


tp.策略.unique()


# In[18]:


记录_={}
for k in tp.index:
    记录_.setdefault(tp.loc[k,'ind'],[]).append(tp.loc[k].to_dict())


# In[19]:


存疑记录=[]
卖出记录=ods_all[ods_all.BuySell=='Sell']
卖出记录.loc[:,'ind']=卖出记录.账户.str.cat(卖出记录.策略,sep='-').str.cat(卖出记录.代码,sep='-')
记录=[]
for k in 卖出记录.index:
    v=卖出记录.loc[k].to_dict()
    while v['TradedVolume']>0:
        #记录_[v['ind']]

        if v['ind'] not in 记录_ or 记录_[v['ind']]==[] or v['日期']<记录_[v['ind']][0]['日期']:
            #print(v)
            存疑记录.append(v)
            break

        if v['TradedVolume']<=记录_[v['ind']][0]['持仓']:
            成交数量=v['TradedVolume']
            记录_[v['ind']][0]['卖出数量']+=成交数量
            记录_[v['ind']][0]['持仓']-=成交数量
            记录_[v['ind']][0]['卖出金额']+=成交数量*v['TradedPrice']
            记录_[v['ind']][0]['平仓收益']+=成交数量*(v['TradedPrice']-记录_[v['ind']][0]['买入价格'])
            记录_[v['ind']][0]['最后操作日期']=v['日期']
            v['TradedVolume']=0
        elif v['TradedVolume']>记录_[v['ind']][0]['持仓']:
            成交数量=记录_[v['ind']][0]['持仓']
            记录_[v['ind']][0]['卖出数量']+=成交数量
            记录_[v['ind']][0]['持仓']-=成交数量
            记录_[v['ind']][0]['卖出金额']+=成交数量*v['TradedPrice']
            记录_[v['ind']][0]['平仓收益']+=成交数量*(v['TradedPrice']-记录_[v['ind']][0]['买入价格'])
            记录_[v['ind']][0]['最后操作日期']=v['日期']
            v['TradedVolume']-=成交数量
            
            
        if  记录_[v['ind']][0]['持仓']==0:
            记录.append(记录_[v['ind']].pop(0))
for v in 记录_.keys():
    for vv in 记录_[v]:
        记录.append(vv)
记录=pd.DataFrame(记录).merge(收盘价,how='left')      


# In[20]:


tt=pd.DataFrame(存疑记录)
tt=tt[tt.ProductName=='KY_CY']


#ods_all.groupby(['日期','ProductName']).Instrument.count().unstack().to_csv('交易笔数.csv')


# In[24]:


#pd.DataFrame(存疑记录).to_excel('存疑记录.xlsx')


# In[25]:


记录.merge(收盘价,how='left').groupby('策略').平仓收益.sum()


# In[26]:


记录.merge(收盘价,how='left').groupby('策略').买入金额.sum()


# In[27]:


# ods_all[ods_all.Instrument=='SHSE.S+688084'].groupby(['ProductName','BuySell']).TradedVolume.sum()
# t=ods_all[ods_all.Instrument=='SZSE.CB+123125'].head(30)
# t.loc[:,'v']=t.TradedVolume*t.BuySell.apply(lambda x:1 if x=='Buy' else -1)
# t.set_index('日期')['v'].cumsum()


# In[28]:


记录.loc[:,'浮动收益']=记录.loc[:,'持仓']*(记录.收盘价-记录.买入价格).fillna(0)
记录.rename(columns={'日期':'买入日期','name':'名称'},inplace=True)
记录.loc[:,'总收益']=记录.平仓收益+记录.浮动收益
记录.loc[:,'收益率']=记录.总收益/记录.买入金额
记录.loc[:,'平仓率']=记录.卖出数量/记录.买入数量
记录.loc[:,'卖出价格']=记录.卖出金额/记录.卖出数量
记录.loc[:,'市值']=记录.持仓*记录.收盘价.fillna(0)
记录.loc[pd.isna(记录.名称),'名称']=记录.loc[pd.isna(记录.名称),'代码']
path=f'{root}/全部_%s.csv'%lst_trd_day
记录=记录[~记录.代码.str.contains('CFFEX')]
记录=记录[记录.代码!='SZSE.S+2673']
记录.代码=记录.代码.apply(lambda x: str(int(x[-6:])))
记录['买入日期,代码,名称,策略,账户,买入数量,卖出数量,买入金额,卖出金额,买入价格,卖出价格,持仓,市值,平仓收益,浮动收益,总收益,收益率,最后操作日期,平仓率'.split(',')].to_csv(path,index=False)


# In[29]:



dts=list(closew.index)
本金={}
for dt in dts[-20:]:
    本金[dt]=ios_all[ios_all.SaveTime<=dt+datetime.timedelta(seconds=3600*9.5)].groupby(['ProductName','StrategyName']).Amount.sum()

