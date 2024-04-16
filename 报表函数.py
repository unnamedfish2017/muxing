#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import os

def MaxDrawdown(return_list):
    i = np.argmax((np.maximum.accumulate(return_list) - return_list))
    if i == 0:
        return 0
    j = np.argmax(return_list[:i])  # 开始位置
    drawdown_max = return_list[j] - return_list[i]
    drawdown_rate = (return_list[j] - return_list[i]) / return_list[j]
    drawdown_tian = i - j
#     return drawdown_rate, drawdown_max, drawdown_tian, j, i
    return drawdown_max

def max_drawdown(x):
    drawdown_rate_ls = []
    for i in range(1,len(x)+1,1):
        data = x[:i]
        drawdown_rate = MaxDrawdown(data)
        drawdown_rate_ls.append(drawdown_rate)
    return drawdown_rate_ls

def 连续亏损天数(x):
    list_value = []
    ls_zf = np.where(x>0,0,1)
    for i in range(1,len(ls_zf)+1,1):
        ls = ls_zf[:i]
        if ls[-1]==0:
            list_value.append(0)
        else:
            ls_r = list(ls[::-1])
            if 0 not in ls_r:
                list_value.append(len(ls_r))
            else:
                list_value.append(ls_r.index(0))
    return list_value

def 最大连续亏损天数(x):
    values = []
    ls_value = 连续亏损天数(x)
    for i in range(1,len(ls_value)+1,1):
        data = ls_value[:i]
        value = max(data)
        values.append(value)
    return values

def 当周收益(df):
    values = []
    for i in range(1,len(df)+1,1):
        df['本周起始日期'] = df['买入日期'].apply(lambda x:x - timedelta(days=x.weekday()) )
        data = df[:i]
        df_dt = data[(data.买入日期>=data.iloc[-1].本周起始日期)]
        value = df_dt['单日盈亏额'].sum()
        values.append(value)
    return values

def 当月收益(df):
    values = []
    for i in range(1,len(df)+1,1):
        df['本月起始日期'] = df['买入日期'].apply(lambda x:datetime.datetime(x.year, x.month, 1))
        data = df[:i]
        df_dt = data[(data.买入日期>=data.iloc[-1].本月起始日期)]
        value = df_dt['单日盈亏额'].sum()
        values.append(value)
    return values

def 当周总买入金额(df):
    values = []
    for i in range(1,len(df)+1,1):
        df['本周起始日期'] = df['买入日期'].apply(lambda x:x - timedelta(days=x.weekday()) )
        data = df[:i]
        df_dt = data[(data.买入日期>=data.iloc[-1].本周起始日期)]
        value = df_dt['总买入金额'].sum()
        values.append(value)
    return values

def 当周总买入个数(df):
    values = []
    for i in range(1,len(df)+1,1):
        df['本周起始日期'] = df['买入日期'].apply(lambda x:x - timedelta(days=x.weekday()) )
        data = df[:i]
        df_dt = data[(data.买入日期>=data.iloc[-1].本周起始日期)]
        value = df_dt['当日买入股票数量'].sum()
        values.append(value)
    return values

def get_records_bak(root):
    import os
    ods_all=[]
    ios_all=[]

    for v in ['KY','HC','QMT','GT']:
        mls=[i for i in os.listdir(root) if i.endswith(v+'.csv')]
        mls1=[i for i in mls if 'Deposit' in i]
        mls1=sorted(mls1)[::-1][::1]+sorted(mls1)[:1]
        for i in mls1:
            ios=pd.read_csv(os.path.join(root,i))
            ios_all.append(ios)

    for v in ['KY','HC','QMT','GT']:
        mls=[i for i in os.listdir(root) if i.endswith(v+'.csv')]
        mls2=[i for i in mls if 'Orders' in i]
        mls2=sorted(mls2)[::-1][::1]+sorted(mls2)[:1]
        for i in mls2:
            df=pd.read_csv(os.path.join(root,i), sep='$')
            if len(df)==0 or i[:8]<'20230822':
                continue
            clms=df.columns[0].split(',')[:-1]+['OrderRef']#+'OrderRef,LocalID,SysID'.split(',')
            #ods=df.iloc[:,0].str.split(',',expand=True).iloc[:,:len(clms)]
            ods=pd.DataFrame(df.iloc[:,0].apply(lambda x:x.split(',')[:len(clms)-1]+[','.join(x.split(',')[len(clms)-1:])]).to_list())
            ods.columns=clms
            ods.loc[:,'rk']=ods.groupby(['ProductName','AccountName','BuySell','Instrument','StrategyName','TradedVolume','DealDateTime']).RequestDateTime.rank(method='first')
            ods.loc[:,'source']=i
            
            tp=df.iloc[:,0].astype(str).str.contains('T0_MIYUAN').astype(int)
            ods.loc[:,'T0_MIYUAN']=tp.values

            ods_all.append(ods)
            for v in ['DealDateTime', 'RequestDateTime']:
                ods[v]=pd.to_datetime(ods[v])

            #print(ods[(ods.Instrument=='SHSE.S+603690')&(ods.StrategyName=='E1LG90TNI')&(ods.ProductName=='HC_SZLH')].OrderRef.unique())
                
    for v in ['KY','HC','QMT','GT']:
        mls=[i for i in os.listdir(root) if i.endswith('.csv') and 'hist_order' in i and v in i]

        for i in mls:
            df=pd.read_csv(os.path.join(root,i), sep='$')
            if len(df)==0:
                continue
            clms=df.columns[0].split(',')[:-1]+['OrderRef']#+'OrderRef,LocalID,SysID'.split(',')
            #ods=df.iloc[:,0].str.split(',',expand=True).iloc[:,:len(clms)]
            ods=pd.DataFrame(df.iloc[:,0].apply(lambda x:x.split(',')[:len(clms)-1]+[','.join(x.split(',')[len(clms)-1:])]).to_list())
            ods.columns=clms
            ods.loc[:,'rk']=ods.groupby(['ProductName','AccountName','BuySell','Instrument','StrategyName','TradedVolume','DealDateTime']).RequestDateTime.rank(method='first')
            ods.loc[:,'source']=i
            
            tp=df.iloc[:,0].astype(str).str.contains('T0_MIYUAN').astype(int)
            ods.loc[:,'T0_MIYUAN']=tp.values
            
            ods_all.append(ods)
            for v in ['DealDateTime', 'RequestDateTime']:
                ods[v]=pd.to_datetime(ods[v])

    ods_all_bak=pd.concat(ods_all)            
    ods_all=pd.concat(ods_all).drop_duplicates(['ProductName','AccountName','BuySell','Instrument','StrategyName','TradedVolume','DealDateTime','rk'])
    ods_all=ods_all[(ods_all.TradedPrice.astype(float)>0)|(ods_all.OrderRef=='Virtual.Open')|(ods_all.OrderRef=='ExDividend Virtual.Close')                    |(ods_all.OrderRef=='ExRight Virtual.Open')|(ods_all.OrderRef.str.contains('MIYUAN'))]
    
    #print(ods_all[(ods_all.Instrument=='SHSE.S+603690')&(ods_all.StrategyName=='E1LG90TNI')&(ods_all.ProductName=='HC_SZLH')].OrderRef.unique())
    ios_all=pd.concat(ios_all).drop_duplicates()
    ios_all.SaveTime=pd.to_datetime(ios_all.SaveTime)

    ios_all=ios_all.sort_values('SaveTime')
    ods_all=ods_all.sort_values('DealDateTime')
    
    ods_all.loc[:,'AccountName']=ods_all.loc[:,'AccountName'].replace('Atx.Stock.Trade','SHSE.Stock.Atx_GT_XYSS')
    ods_all.loc[:,'ProductName']=ods_all.loc[:,'ProductName'].replace('GJ_XYSS','GT_XYSS')

    ios_all.loc[:,'AccountName']=ios_all.loc[:,'AccountName'].replace('Atx.Stock.Trade','SHSE.Stock.Atx_GT_XYSS')
    ios_all.loc[:,'ProductName']=ios_all.loc[:,'ProductName'].replace('GJ_XYSS','GT_XYSS')
    
    ios_all=ios_all[~((~ios_all.StrategyName.astype(str).str.endswith('XX2'))&(ios_all.AccountName=='SHSE.Stock.KMAX_XINXIU2'))].reset_index(drop=True)
    return ios_all,ods_all


def get_records_raw(root):
    import os
    ods_all=[]
    ios_all=[]

    for v in ['KY','HC','QMT','GT']:
        mls=[i for i in os.listdir(root) if i.endswith(v+'.csv')]
        mls1=[i for i in mls if 'Deposit' in i]
        mls1=sorted(mls1)[::-1][::30]+sorted(mls1)[:1]
        for i in mls1:
            ios=pd.read_csv(os.path.join(root,i))
            ios.loc[:,'source']=i
            ios_all.append(ios)

    for v in ['KY','HC','QMT','GT']:
        mls=[i for i in os.listdir(root) if i.endswith(v+'.csv')]
        mls2=[i for i in mls if 'Orders' in i]
        mls2=sorted(mls2)[::-1][::30]+sorted(mls2)[:1]
        for i in mls2:
            df=pd.read_csv(os.path.join(root,i), sep='$')
            if len(df)==0:
                continue
            clms=df.columns[0].split(',')[:-1]+['OrderRef']#+'OrderRef,LocalID,SysID'.split(',')
            #ods=df.iloc[:,0].str.split(',',expand=True).iloc[:,:len(clms)]
            ods=pd.DataFrame(df.iloc[:,0].apply(lambda x:x.split(',')[:len(clms)-1]+[','.join(x.split(',')[len(clms)-1:])]).to_list())
            ods.columns=clms
            ods.loc[:,'rk']=ods.groupby(['ProductName','AccountName','BuySell','Instrument','StrategyName','TradedVolume','DealDateTime']).RequestDateTime.rank(method='first')
            ods.loc[:,'source']=i

            ods_all.append(ods)
            for v in ['DealDateTime', 'RequestDateTime']:
                ods[v]=pd.to_datetime(ods[v])
    ods_all_bak=pd.concat(ods_all)            
    ods_all=pd.concat(ods_all).drop_duplicates(['ProductName','AccountName','BuySell','Instrument','StrategyName','TradedVolume','DealDateTime','rk'])
    #ods_all=ods_all[(ods_all.TradedPrice.astype(float)>0)|(ods_all.OrderRef=='Virtual.Open')]
    ios_all=pd.concat(ios_all).drop_duplicates()
    ios_all.SaveTime=pd.to_datetime(ios_all.SaveTime)

    ios_all=ios_all.sort_values('SaveTime')
    ods_all=ods_all.sort_values('DealDateTime')
    
    ods_all.loc[:,'AccountName']=ods_all.loc[:,'AccountName'].replace('Atx.Stock.Trade','SHSE.Stock.Atx_GT_XYSS')
    ods_all.loc[:,'ProductName']=ods_all.loc[:,'ProductName'].replace('GJ_XYSS','GT_XYSS')

    ios_all.loc[:,'AccountName']=ios_all.loc[:,'AccountName'].replace('Atx.Stock.Trade','SHSE.Stock.Atx_GT_XYSS')
    ios_all.loc[:,'ProductName']=ios_all.loc[:,'ProductName'].replace('GJ_XYSS','GT_XYSS')
    
    ios_all=ios_all[~((~ios_all.StrategyName.astype(str).str.endswith('XX2'))&(ios_all.AccountName=='SHSE.Stock.KMAX_XINXIU2'))].reset_index(drop=True)
    return ios_all,ods_all

def get_records(root):
    import os
    ods_all=[]
    ios_all=[]

    ios_all_hist=pd.read_parquet(os.path.join(root,'出入金汇总.parquet'))
    ods_all_hist=pd.read_parquet(os.path.join(root,'交易汇总.parquet'))

    for v in ['KY','HC','QMT','GT']:
        mls=[i for i in os.listdir(root) if i.endswith(v+'.csv')]
        mls1=[i for i in mls if 'Deposit' in i]
        mls1=sorted(mls1)[::-1][::1]+sorted(mls1)[:1]
        for i in mls1:
            ios=pd.read_csv(os.path.join(root,i))
            ios_all.append(ios)

    for v in ['KY','HC','QMT','GT']:
        mls=[i for i in os.listdir(root) if i.endswith(v+'.csv')]
        mls2=[i for i in mls if 'Orders' in i]
        mls2=sorted(mls2)[::-1][::1]+sorted(mls2)[:1]
        mls2=mls2[:10]
        for i in mls2:
            df=pd.read_csv(os.path.join(root,i), sep='$')
            if len(df)==0 or i[:8]<'20230822':
                continue
            clms=df.columns[0].split(',')[:-1]+['OrderRef']#+'OrderRef,LocalID,SysID'.split(',')
            #ods=df.iloc[:,0].str.split(',',expand=True).iloc[:,:len(clms)]
            ods=pd.DataFrame(df.iloc[:,0].apply(lambda x:x.split(',')[:len(clms)-1]+[','.join(x.split(',')[len(clms)-1:])]).to_list())
            ods.columns=clms
            ods.loc[:,'rk']=ods.groupby(['ProductName','AccountName','BuySell','Instrument','StrategyName','TradedVolume','DealDateTime']).RequestDateTime.rank(method='first')
            ods.loc[:,'source']=i

            tp=df.iloc[:,0].astype(str).str.contains('T0_MIYUAN').astype(int)
            ods.loc[:,'T0_MIYUAN']=tp.values

            ods_all.append(ods)
            for v in ['DealDateTime', 'RequestDateTime']:
                ods[v]=pd.to_datetime(ods[v])

    ods_all_bak=pd.concat(ods_all)            
    ods_all=pd.concat(ods_all).drop_duplicates(['ProductName','AccountName','BuySell','Instrument','StrategyName','TradedVolume','DealDateTime','rk'])
    ods_all=ods_all[(ods_all.TradedPrice.astype(float)>0)|(ods_all.OrderRef=='Virtual.Open')|(ods_all.OrderRef=='ExDividend Virtual.Close')                    |(ods_all.OrderRef=='ExRight Virtual.Open')|(ods_all.OrderRef.str.contains('MIYUAN'))]

    #print(ods_all[(ods_all.Instrument=='SHSE.S+603690')&(ods_all.StrategyName=='E1LG90TNI')&(ods_all.ProductName=='HC_SZLH')].OrderRef.unique())
    ios_all=pd.concat(ios_all).drop_duplicates()
    ios_all.SaveTime=pd.to_datetime(ios_all.SaveTime)

    ios_all=ios_all.sort_values('SaveTime')
    ods_all=ods_all.sort_values('DealDateTime')

    ods_all.loc[:,'AccountName']=ods_all.loc[:,'AccountName'].replace('Atx.Stock.Trade','SHSE.Stock.Atx_GT_XYSS')
    ods_all.loc[:,'ProductName']=ods_all.loc[:,'ProductName'].replace('GJ_XYSS','GT_XYSS')

    ios_all.loc[:,'AccountName']=ios_all.loc[:,'AccountName'].replace('Atx.Stock.Trade','SHSE.Stock.Atx_GT_XYSS')
    ios_all.loc[:,'ProductName']=ios_all.loc[:,'ProductName'].replace('GJ_XYSS','GT_XYSS')

    ios_all=ios_all[~((~ios_all.StrategyName.astype(str).str.endswith('XX2'))&(ios_all.AccountName=='SHSE.Stock.KMAX_XINXIU2'))].reset_index(drop=True)

    filter_set = ods_all_hist[['DealDateTime', 'ProductName']].drop_duplicates()
    filter_set=set(zip(filter_set['DealDateTime'], filter_set['ProductName']))
    filtered_df = ods_all[~ods_all[['DealDateTime', 'ProductName']].apply(tuple, axis=1).isin(filter_set)]
    ods_all=ods_all_hist.append(filtered_df)
    return ios_all,ods_all


# In[ ]:





# In[3]:


if __name__ == "__main__":
    import time
    t1=time.time()
    root='..//原始数据_托管'
    ios_all,ods_all=get_records_bak(root)
    print(time.time()-t1)
    ios_all.to_parquet(os.path.join(root,'出入金汇总.parquet'))
    ods_all.to_parquet(os.path.join(root,'交易汇总.parquet'))
    
    t1=time.time()
    root='..//原始数据_托管'
    ios_all2,ods_all2=get_records(root)
    print(time.time()-t1)
    
    print(ios_all2.shape,ios_all.shape,ods_all.shape,ods_all2.shape)
    
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

    def read_F(key,path):
        def aes_decrypt(encrypted_data, key):
            backend = default_backend()
            cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=backend)
            decryptor = cipher.decryptor()

            # Decrypt the data and then unpad
            decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
            unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
            unpadded_data = unpadder.update(decrypted_data) + unpadder.finalize()

            # Convert the unpadded data back to DataFrame
            decrypted_df = pd.read_pickle(io.BytesIO(unpadded_data))

            return decrypted_df

        # Save encrypted data to a file


        # Read encrypted data from a file
        with open(path, 'rb') as file:
            encrypted_data_read = file.read()

        # Decrypt data and print
        decrypted_df = aes_decrypt(encrypted_data_read, key)
        return decrypted_df

    key=b'\xc9\xc6M\xda\xf8g\xda\x85\x07\x18&\xc0\x8c\xc4nB\xcfx*l>nC\x19\xdc\xb3)L#\xc4\x8e\xa6'
    root_save='/home/jovyan/work/commons/records'
    path=os.path.join(root_save,'ios_all.bin')
    save_F(ios_all,key,path)

    path=os.path.join(root_save,'ods_all.bin')
    save_F(ods_all,key,path)


# In[ ]:


# def save_F(df,key,path):
#     import pandas as pd
#     from cryptography.fernet import Fernet
#     cipher_suite = Fernet(key)
#     # 创建一个示例数据框架
#     # 加密数据框架中的每个元素
#     encrypted_df = df.applymap(lambda x: cipher_suite.encrypt(str(x).encode()))
#     # 将加密后的数据框架保存到 Parquet 文件
#     encrypted_df.to_parquet(path)
#     # 将密钥保存到文件（确保安全存储密钥）
# def read_F(path,key):
#     # 读取加密的 Parquet 文件
#     import pandas as pd
#     from cryptography.fernet import Fernet
#     import pyarrow.parquet as pq
#     cipher_suite = Fernet(key)
#     encrypted_table = pq.read_table(path)
#     encrypted_df = encrypted_table.to_pandas()

#     # 解密数据框架中的每个元素
#     decrypted_df = encrypted_df.applymap(lambda x: cipher_suite.decrypt(x).decode())

#     return decrypted_df
# path='/home/jovyan/work/commons/records/ios_all.parquet'
# save_F(ios_all,key,path)

# path='/home/jovyan/work/commons/records/ods_all.parquet'
# save_F(ods_all,key,path)


# In[ ]:





# In[ ]:




