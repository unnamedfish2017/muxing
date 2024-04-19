#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datetime
import tushare as ts
import time
import pandas as pd
import tushare as ts
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


# In[2]:


import os
root='/home/jovyan/work/commons/pools/'
mls=os.listdir(root)
file_paths=[]
pool_dict={}
for ml in mls:
    print(ml)
    p=os.path.join(root,ml)
    fpath=os.listdir(p)
    for f in fpath:
        if nxt_trd_day in f:
            print(f)
            file_paths.append(os.path.join(p,f))
            tp=pd.read_csv(os.path.join(p,f),header=None)
            if tp.shape[1]==4:
                tp[1]=tp[1].apply(lambda x:x[-6:]).apply(lambda x:'SHSE.S+'+x if x.startswith('6') else 'SZSE.S+'+x)
                tp[[1,2]].to_csv(os.path.join(p,f),index=False,header=None)
            with open(os.path.join(p,f), 'r') as file:
                tp = file.read()
            pool_dict[f]=tp


# In[3]:


import pickle
with open('pool_dict.pkl', 'wb') as f:
    pickle.dump(pool_dict, f)


# In[4]:


from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# 加密函数
def encrypt_bytes(key, data):
    cipher = AES.new(key, AES.MODE_EAX)
    nonce = cipher.nonce
    ciphertext, tag = cipher.encrypt_and_digest(data)
    return nonce + ciphertext + tag

# 解密函数
def decrypt_bytes(key, data):
    nonce = data[:16]
    ciphertext = data[16:-16]
    tag = data[-16:]
    cipher = AES.new(key, AES.MODE_EAX, nonce)
    plaintext = cipher.decrypt_and_verify(ciphertext, tag)
    return plaintext


# In[5]:


import hashlib
input_file='pool_dict.pkl'
encrypted_file='pool_dict_e.pkl'
key='miyuan_ylc'
key=hashlib.sha256(key.encode()).digest()
with open(input_file, 'rb') as file:
    model_bytes = file.read()

if os.path.exists(encrypted_file):
    os.remove(encrypted_file)
t=encrypt_bytes(key, model_bytes)
with open(encrypted_file,'wb') as file:
    file.write(t)


# In[6]:


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


# In[8]:


import os
ftp=MFTP()
ftp.set_debuglevel(2)
root='./'

ftp.connect("1.15.74.234",5277)
ftp.login("miyuan","miyuan@8.02")

path='pool_dict_e.pkl'
root_='Pools'
ftp.cwd(root_.encode('utf-8').decode('latin1'))
ftp.upload(path.encode('utf-8').decode('latin1'),path)


# In[9]:


import os
command = 'jupyter nbconvert --to script 上传票池ftp.ipynb'
os.system(command)


# In[ ]:




