#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[1]:


import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

DATA_PATH = '/home/vscode/workspace/data/store/rsync'
root_sc='/home/vscode/workspace/work/生产/素材'
root_save='/home/vscode/workspace/work/生产/产出/票池/其他'

path=f'{DATA_PATH}/data_daily/stock_day_n.parquet'
df_raw=pd.read_parquet(path)
df_raw=df_raw[df_raw.date>=20221001]
df_raw.loc[:,'date']=pd.to_datetime(df_raw.date.astype(str))
closew_raw=df_raw[['code','date','close']].pivot(index='date',columns='code',values='close').fillna(method='ffill').stack().rename('close').reset_index()


# In[4]:


import pickle
import pandas as pd
import numpy as np
import os,pickle
import pandas as pd
path=f'{DATA_PATH}/data_daily/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
closew = data_daily['closew'].fillna(method='ffill')
lst_trd_day=closew.index[-1].strftime('%Y%m%d')
root=f'{root_sc}/托管数据'

from 报表函数 import *
ios_all,ods_all=get_records(root)

ios_all.to_parquet(os.path.join(root,'出入金汇总.parquet'))
ods_all.to_parquet(os.path.join(root,'交易汇总.parquet'))



import pandas as pd
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
import os
import io

def save_F(data,key,path):
    def aes_encrypt(data, key):
        backend = default_backend()
        cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=backend)
        encryptor = cipher.encryptor()

        # Convert DataFrame to bytes and pad to a multiple of the block size
        serialized_data = io.BytesIO()
        data.to_pickle(serialized_data)
        serialized_data.seek(0)

        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(serialized_data.read()) + padder.finalize()

        # Encrypt the padded data
        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

        return encrypted_data
    encrypted_data = aes_encrypt(data, key)
    with open(path, 'wb') as file:
        file.write(encrypted_data)

# def read_F(data,key,path):
#     def aes_decrypt(encrypted_data, key):
#         backend = default_backend()
#         cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=backend)
#         decryptor = cipher.decryptor()

#         # Decrypt the data and then unpad
#         decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
#         unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
#         unpadded_data = unpadder.update(decrypted_data) + unpadder.finalize()

#         # Convert the unpadded data back to DataFrame
#         decrypted_df = pd.read_pickle(io.BytesIO(unpadded_data))

#         return decrypted_df

#     # Save encrypted data to a file


#     # Read encrypted data from a file
#     with open(path, 'rb') as file:
#         encrypted_data_read = file.read()

#     # Decrypt data and print
#     decrypted_df = aes_decrypt(encrypted_data_read, key)
#     return decrypted_df

# key=b'\xc9\xc6M\xda\xf8g\xda\x85\x07\x18&\xc0\x8c\xc4nB\xcfx*l>nC\x19\xdc\xb3)L#\xc4\x8e\xa6'
# root_save='/home/jovyan/work/commons/records'
# path=os.path.join(root_save,'ios_all.bin')
# save_F(ios_all,key,path)

# path=os.path.join(root_save,'ods_all.bin')
# save_F(ods_all,key,path)


策略映射=pd.read_csv(f'{root_sc}/策略映射.csv',encoding='gbk').set_index('实际策略名').to_dict()['跟踪策略名']
账户=pd.read_csv(f'{root_sc}/账户.csv',encoding='gbk').set_index('实际账户名').to_dict()['跟踪账户名']

tp=pd.DataFrame.from_dict(策略映射,orient='index').reset_index()
tp.columns=['StrategyName','策略']
ods_all=ods_all.merge(tp,how='left')
ios_all=ios_all.merge(tp,how='left')

tp=pd.DataFrame.from_dict(账户,orient='index').reset_index()
tp.columns=['AccountName','账户']
ods_all=ods_all.merge(tp,how='inner')   #####只统计这些账户的表现
ios_all=ios_all.merge(tp,how='inner')   #####只统计这些账户的表现

ios_all.loc[:,'date']=ios_all.SaveTime.dt.date.values
ios_all.loc[:,'le930']=(ios_all.SaveTime.apply(lambda x:x.strftime('%H%m'))<='0930').values
ods_all.loc[:,'日期']=ods_all.DealDateTime.dt.date.values
ios_all.loc[:,'累计资金']=ios_all.sort_values(by='SaveTime').groupby(['账户','策略']).Amount.cumsum()
ods_all=ods_all.sort_values(by='DealDateTime')
for v in ['TradedPrice','TradedVolume']:
    ods_all[v]=ods_all[v].astype(float)



ods_all=ods_all[~((ods_all.DealDateTime=='2023-03-03 00:00:00')&(ods_all.StrategyName=='EIF'))].reset_index(drop=True)




mls=os.listdir(root)
lst_trd_day=closew.index[-1].strftime('%Y%m%d')
def get_(keyword,account,dt):
    try:
        path=max([i for i in mls if keyword in i and i.endswith(account+'.xlsx') and dt in i])
        path=os.path.join(root,path)
        return path
    except:
        return None

明细all=pd.DataFrame([])
for keyword in ['position']:
    for account in ['KY','QMT','HC','GT','DW']:
        path=get_(keyword,account,lst_trd_day)
        if path:
            tp=pd.read_excel(path,sheet_name='策略明细表').ffill()
            tp.loc[:,'账户名称']=tp.loc[:,'账户名称'].replace('Atx.Stock.Trade','SHSE.Stock.Atx_GT_XYSS')
            tp.loc[:,'产品']=tp.loc[:,'产品'].replace('GJ_XYSS','GT_XYSS')
            明细all=明细all.append(tp)
        else:
            print(account,'miss')
tp=pd.DataFrame.from_dict(策略映射,orient='index').reset_index()
tp.columns=['策略名称','策略']
明细all=明细all.merge(tp,how='left')
明细all.loc[pd.isna(明细all.策略)&(明细all.策略.str.endswith('XX2')),'策略']=明细all.loc[pd.isna(明细all.策略)&(明细all.策略.str.endswith('XX2')),'策略名称'].apply(lambda x:x.replace('XX2',''))
明细all.loc[pd.isna(明细all.策略),'策略']=明细all.loc[pd.isna(明细all.策略),'策略名称']

tp=pd.DataFrame.from_dict(账户,orient='index').reset_index()
tp.columns=['账户名称','账户']

明细all=明细all.merge(tp,how='inner')   #####只统计这些账户的表现
明细all=明细all[['策略','账户','交易标的','量','机构名称']]
明细all.columns=['策略','账户','Instrument','累计量','标的名称']

明细all.loc[:,'日期']=pd.to_datetime(lst_trd_day)
明细all.loc[:,'code']=明细all.Instrument.apply(lambda x:x[-6:]+'.'+x[:2].lower())
明细all=明细all.merge(closew.iloc[-1,:].T.rename('close').reset_index(),how='left')
明细all.loc[:,'持有市值']=明细all.close*明细all.累计量
明细all.to_parquet(f'{root}//系统_盘后持仓_%s.parquet'%closew.index[-1].strftime('%Y%m%d'))




import time
import pandas as pd
from multiprocessing import Pool, cpu_count

def process_date(dt):
    现金_all = []
    for keyword in ['position']:
        for account in ['KY', 'QMT', 'HC', 'GT','DW']:
            path = get_(keyword, account, dt.strftime('%Y%m%d'))
            if path:
                tp = pd.read_excel(path, sheet_name='策略汇总表').ffill()
                tp.loc[:, '账户名称'] = tp.loc[:, '账户名称'].replace('Atx.Stock.Trade', 'SHSE.Stock.Atx_GT_XYSS')
                tp.loc[:, '产品'] = tp.loc[:, '产品'].replace('GJ_XYSS', 'GT_XYSS')
                tp.loc[:, '日期'] = dt
                现金_all.append(tp)
            else:
                print(account, 'miss')

    tp = pd.DataFrame.from_dict(策略映射, orient='index').reset_index()
    tp.columns = ['策略名称', '策略']
    现金_all = pd.concat(现金_all).merge(tp, how='left')

    tp = pd.DataFrame.from_dict(账户, orient='index').reset_index()
    tp.columns = ['账户名称', '账户']
    现金_all = 现金_all.merge(tp, how='inner')

    现金_all = 现金_all[['日期', '策略', '账户', '可用资产']]
    return 现金_all

def parallel_process_date(dt):
    try:
        return process_date(dt)
    except Exception as e:
        print(f"Error processing date {dt}: {str(e)}")

t1 = time.time()
dts=list(closew.index)
dts_range = dts[-60:]

# Adjust the number of processes based on your machine's capacity
num_processes = min(cpu_count(), 6)

with Pool(num_processes) as pool:
    现金_all_list = pool.map(parallel_process_date, dts_range)

盘后现金 = pd.concat(现金_all_list).rename(columns={'可用资产':'盘后现金'})
t2 = time.time()
print(t2 - t1)


# In[12]:


系统_盘后持仓=[]
dts=list(closew.index)
for dt in dts[-70:]:
    try:
        tp=pd.read_parquet(f'{root}/系统_盘后持仓_%s.parquet'%dt.strftime('%Y%m%d'))
        系统_盘后持仓.append(tp)
    except:
        print(f'{root}/系统_盘后持仓_%s.parquet'%dt.strftime('%Y%m%d')+' loss')
系统_盘后持仓=pd.concat(系统_盘后持仓)


# In[ ]:


统计日期=pd.DataFrame([i for i in closew.index if i >=pd.to_datetime('2023-01-01')],columns=['日期'])
统计日期.loc[:,'盘前时间']=统计日期.日期+ pd.Timedelta(hours=9,minutes=25)
统计日期.loc[:,'收盘时间']=统计日期.日期+ pd.Timedelta(hours=15,minutes=0)
盘前资金=[]
for (i,j),data in ios_all.groupby(['账户','策略']):
    #print((i,j))
    t1=pd.merge_asof(统计日期.rename(columns={'盘前时间':'SaveTime'}),data,on='SaveTime')
    t2=pd.merge_asof(统计日期.rename(columns={'收盘时间':'SaveTime'}),data,on='SaveTime')
    盘前资金.append(t1)
    盘前资金.append(t2)
盘前资金=pd.concat(盘前资金)
盘前资金=盘前资金.sort_values(by='累计资金',ascending=False).drop_duplicates(['日期','ProductName','StrategyName','AccountName'])




盘后现金=盘后现金.merge(盘前资金[['日期','策略','账户','累计资金']],how='left')


盘后权益=盘后现金.merge(系统_盘后持仓.groupby(['日期','账户','策略']).持有市值.sum().fillna(0).reset_index(),how='left')
盘后权益.loc[:,'盘后权益']=盘后权益.盘后现金.fillna(0)+盘后权益.持有市值.fillna(0)

盘后权益.loc[:,'盘前权益']=盘后权益.groupby(['账户','策略']).盘后权益.shift(1).fillna(0)+盘后权益.groupby(['账户','策略']).累计资金.diff(1).fillna(0)
盘后权益.loc[盘后权益.盘前权益==0,'盘前权益']=盘后权益.loc[盘后权益.盘前权益==0,'累计资金']
盘后权益.loc[:,'当日收益']=盘后权益.盘后权益-盘后权益.盘前权益
盘后权益.loc[:,'当日收益率']=盘后权益.当日收益/(盘后权益.盘后权益-盘后权益.当日收益)
盘后权益.loc[:,'当日收益率_剔除仓位变化']=盘后权益.当日收益/(盘后权益.groupby(['账户','策略']).持有市值.shift(1).fillna(np.nan))
盘后权益.to_parquet(f'{root}//盘后权益_%s.parquet'%closew.index[-1].strftime('%Y%m%d'))


# In[ ]:


import os

mls=os.listdir(root)
lst_trd_day=closew.index[-1].strftime('%Y%m%d')
def get_(keyword,account,dt):
    try:
        path=max([i for i in mls if keyword in i and i.endswith(account+'.xlsx') and dt in i])
        path=os.path.join(root,path)
        return path
    except:
        return None
权益=pd.DataFrame([])
for keyword in ['position']:
    for account in ['KY','QMT','HC','GT','DW']:
        path=get_(keyword,account,lst_trd_day)
        if path:
            tp=pd.read_excel(path,sheet_name='策略汇总表').ffill()
            tp.loc[:,'账户名称']=tp.loc[:,'账户名称'].replace('Atx.Stock.Trade','SHSE.Stock.Atx_GT_XYSS')
            tp.loc[:,'产品']=tp.loc[:,'产品'].replace('GJ_XYSS','GT_XYSS')
            权益=权益.append(tp)
权益=权益.reset_index(drop=True)
权益.astype(str).to_parquet(f'{root}/权益_%s.parquet'%closew.index[-1].strftime('%Y%m%d'))

