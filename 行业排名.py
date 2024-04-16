#!/usr/bin/env python
# coding: utf-8

# In[66]:


#############更新数据
from ftplib import FTP
import time,datetime
import shutil
root='/home/jovyan/data/store/rsync/data_daily'

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

ftp.connect("42.192.118.237",5277)
ftp.login("miyuan","miyuan@8.02")
ftp.cwd('/industry')
ftp.download('data2.xlsm',root+'/申万指数.xlsm')
shutil.copy(root+'/申万指数.xlsm',root+'/行业指数//data2_%s.xlsm'%datetime.datetime.now().strftime('%Y%m%d'))
ftp.close()

import pandas as pd
import numpy as np
import datetime
import time
import warnings
warnings.filterwarnings("ignore")
close = pd.read_excel(root+'/申万指数.xlsm',sheet_name='收盘价', header=[0,1,2])
close = close.droplevel([1,2],axis=1)
close.columns=close.columns.astype(str)
close.set_index(close.columns[0],inplace=True)
code = pd.read_excel(root+'/申万指数.xlsm',sheet_name='code', header=None)
close.columns=close.columns.astype(str)
close.index.name='date'
close.index=pd.to_datetime(close.index)

tp=pd.read_excel(root+'/申万指数.xlsm',sheet_name='收盘价', header=[0,1]).columns
tp=[[i[0],i[1]] for i in tp[1:]]
指数名称=pd.DataFrame(tp).rename(columns={0:'hy_code',1:'hy_name'})


# In[67]:


指数名称[(指数名称.hy_code.str.startswith('801'))&(~指数名称.hy_name.str.endswith('Ⅱ'))].sort_values(by='hy_code')


# In[68]:


个股 = pd.read_excel(root+'/申万指数.xlsm',sheet_name='个股', header=[0])
个股.columns=['code','name','申万一级','申万二级']
一级行业列表=list(个股.申万一级.unique())
一级行业=指数名称[指数名称.hy_name.isin(一级行业列表)]


# In[69]:


clms=一级行业.set_index('hy_code').to_dict()['hy_name']
close=close[list(一级行业.hy_code.values)].rename(columns=clms)


# In[70]:


L=250
S=5
rk_L=close.ffill().pct_change(L).tail(1).T.rank()
rk_L.columns=['250日排名']
rk_S=close.ffill().pct_change(S).tail(1).T.rank()
rk_S.columns=['5日排名']


# In[97]:


root='..//产出//行业涨幅排名'
排名=pd.concat((rk_L,rk_S),axis=1).sort_values(by='250日排名')
排名.loc[:,'排名变化']=排名['250日排名']-排名['5日排名']
save_path=os.path.join(root,'行业涨幅排名_%s.xlsx'%close.index[-1].strftime('%Y%m%d'))


# In[98]:


import sys
sys.path.append('/home/jovyan/work/workspaces/daily report/分析代码2.0/脚本/')
from  定时函数 import *
flag=os.path.exists(save_path)
排名.to_excel(save_path)
if not flag:
    group_id='oc_a6f7e857014839f1b3900bff9072ee97'
    飞书发送(save_path,group_id)


# In[ ]:





# In[ ]:




