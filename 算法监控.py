#!/usr/bin/env python
# coding: utf-8

# <font color=red size=5 face=雅黑>**1.加载日线数据**</font>

# In[1]:


import pickle
import pandas as pd
import numpy as np
import os,pickle
import pandas as pd
path='/home/jovyan/data/store/rsync/data_daily/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
closew = data_daily['closew'].fillna(method='ffill')
lst_trd_day=closew.index[-1].strftime('%Y%m%d')


# <font color=red size=5 face=雅黑>**2.加载实盘成交数据**</font>

# In[2]:


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
path=os.path.join(root_save,'ods_all.bin')
ods_all=read_F(key,path)

ods_all.loc[:,'日期']=pd.to_datetime(ods_all.DealDateTime.apply(lambda x:x.date()))
ods_all.loc[:,'TradedVolume']=ods_all.loc[:,'TradedVolume'].astype(float)
ods_all.loc[:,'Amt']=ods_all.loc[:,'TradedPrice'].astype(float)*ods_all.loc[:,'TradedVolume'].astype(float)
ods_all.loc[:,'tm']=ods_all.DealDateTime.apply(lambda x:x.strftime('%H%M'))
ods_all.loc[:,'tm2']=ods_all.RequestDateTime.apply(lambda x:x.strftime('%H%M'))

############################################################################################限定分析范围
st=('EA','ET','E1','E3','EMIG','EMFA','EMHS','EMAL','EMHS')
flag_tm='1010'
tp=ods_all[(ods_all.StrategyName.str.startswith(st))&(ods_all.tm<=flag_tm)&(ods_all.ProductName!='QUANT_PAPER')]
tp=tp[tp.StrategyName!='EAW']
tp.loc[:,'StrategyName']=tp.loc[:,'StrategyName'].apply(lambda x:x.replace('XX2','')+'_').str.cat(tp.ProductName.str.lower())

t=tp[~tp.OrderRef.str.contains('Virtual')].groupby(['StrategyName','ProductName','BuySell','Instrument','日期','RequestDateTime','tm2'])
#t=tp[~tp.OrderRef.str.contains('Virtual')].groupby(['StrategyName','ProductName','BuySell','Instrument','日期'])
t1=(t.Amt.sum()/t.TradedVolume.sum()).rename('Price_traded').reset_index()
t2=t.Amt.sum().rename('Amt').reset_index()
t3=t.TradedVolume.sum().rename('TradedVolume').reset_index()
t4=t.RequestPrice.max().rename('RequestPrice').reset_index()
records=t1.merge(t2,how='left').merge(t3,how='left').merge(t4,how='left')
records.loc[:,'月份']=records.日期.apply(lambda x:x.strftime('%Y%m'))


# In[3]:


records.日期.max()


# <font color=blue size=4 face=雅黑>**算法字段示例**</font>

# In[4]:


ods_all[ods_all.OrderRef.str.contains('T0_MIYUAN')].OrderRef.sample().values


# In[5]:


# tm_st=93000000
# tm_et=93300000
# st=('EA','ET','E1','E3','EMIG','EMFA','EMHS','EMAL','EMHS')

# flag_tm='1010'
# tp=ods_all[(ods_all.StrategyName.str.startswith(st))&(ods_all.tm<=flag_tm)&(ods_all.ProductName!='QUANT_PAPER')]
# tp=tp[tp.StrategyName!='EAW']
# tp.loc[:,'StrategyName']=tp.loc[:,'StrategyName'].apply(lambda x:x.replace('XX2','')+'_')\
# .str.cat(tp.ProductName.str.lower())
# 集合成交=tp[(tp.tm<='0926')&(tp.tm>='0924')].groupby(['StrategyName','ProductName','BuySell','日期']).Amt.sum().rename('集合成交').reset_index()

# t0=集合成交.copy()
# t0.loc[:,'StrategyName']='合计'
# t0.loc[:,'ProductName']='合计'
# for 账户 in 集合成交.StrategyName.apply(lambda x:'_'.join(x.split('_')[1:])).unique():
#     t1=集合成交[集合成交.StrategyName.str.endswith(账户)].copy()
#     t1.loc[:,'StrategyName']=账户
#     集合成交=集合成交.append(t1)
# 集合成交=集合成交.append(t0)
# 集合成交=集合成交.reset_index(drop=True)
# 集合成交=集合成交.groupby(['StrategyName', 'ProductName', 'BuySell', '日期'])['集合成交'].sum().rename('集合成交').reset_index()


# <font color=red size=5 face=雅黑>**3.多进程计算市场表现示例**</font>

# In[6]:


tm_st=93000000
tm_et=93300000

def get_p(dt,stk,tm_st,tm_et):
    dt=str(dt)
    y=dt[:4]
    m=dt[4:6]
    d=dt[6:]
    path='/home/jovyan/data/store/stock/data_/second_3'+'/year='+str(y)+'/month='+str(m)+'/date='+str(d)+'/'+stk.lower()+'.parquet'

    df_sec=pd.read_parquet(path, engine='pyarrow').reset_index(drop=True)
    t1=df_sec[df_sec.Time>=tm_et].iloc[0,:].to_dict()
    t2=df_sec[df_sec.Time>=tm_st].iloc[0,:].to_dict()
    p_st=t1['Open']
    avg_p=(t2['AccTurnover']-t1['AccTurnover'])/(t2['AccVolume']-t1['AccVolume'])
    
    try:
        lp=df_sec[df_sec.Time<=92457000].AskPrice01.values[-1]
    except:
        lp=0
    return dt,stk,p_st,avg_p,lp


# In[7]:


tp_hist=pd.read_parquet('历史标的开盘表现.parquet')
# list_hist=list(tp_hist.日期.apply(lambda x:x.strftime('%Y%m%d')).str.cat(tp_hist.Instrument.apply(lambda x:x[-6:]+'.'+x[:2].lower())))

# # tp_hist=pd.DataFrame([],columns=tp_hist.columns)
# # list_hist=[]
# tp_unq=records[['日期','Instrument']].drop_duplicates()

# cs=[]
# for k in tp_unq.index:
#     v=tp_unq.loc[k,:]
#     dt=v['日期'].strftime('%Y%m%d')
#     stk=v['Instrument'][-6:]+'.'+v['Instrument'][:2].lower()
#     if dt+stk not in list_hist:
#         cs.append((dt,stk))

# #cs=cs[:30]
# import numpy as np
# 数据={}
# from multiprocessing import Queue, Process, Pool
# import os

# def add(传入):
#     global 数据
#     if 传入 is None:
#         return
#     dt=传入[0]
#     stk=传入[1]
#     try:
#         数据[stk+'_'+dt]=传入
#     except:
#         #print(dt,'error')
#         pass
#     return

# def get_pool(n=20,cs=[]):
#     p = Pool(n) # 设置进程池的大小
#     for v in cs:
#         p.apply_async(get_p, (v[0],v[1],tm_st,tm_et,),callback=add)   #维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
#     p.close() # 关闭进程池
#     p.join()
# get_pool(30,cs)
# tp=[v for k,v in 数据.items()]
# tp=pd.DataFrame(tp,columns=['date','code','p_st','p_avg','lp'])
# tp.loc[:,'日期']=pd.to_datetime(tp.date.astype(str))
# tp.loc[:,'Instrument']=tp.code.apply(lambda x:'SHSE.S+'+x[:6] if x.endswith('.sh') else 'SZSE.S+'+x[:6])
# tp_hist.append(tp).reset_index(drop=True).to_parquet('历史标的开盘表现.parquet')
# tp_hist=tp_hist.append(tp).reset_index(drop=True)


# <font color=red size=5 face=雅黑>**4.分析相对市场表现**</font>

# In[8]:


ret=records.merge(tp_hist,how='left')
ret.loc[:,'perform_p']=ret.Price_traded/ret.p_st
ret.loc[:,'perform_q']=ret.Price_traded/ret.p_avg
ret=ret.dropna()


# In[9]:


ret.loc[ret.BuySell=='Buy','相对开盘价']=1-ret.perform_p
ret.loc[ret.BuySell=='Sell','相对开盘价']=ret.perform_p-1
ret.loc[ret.BuySell=='Buy','相对均价']=1-ret.perform_q
ret.loc[ret.BuySell=='Sell','相对均价']=ret.perform_q-1


# In[10]:


ret=ret[ret.tm2>='0930']


# In[11]:


t0=ret.copy()
t0.loc[:,'StrategyName']='合计'
t0.loc[:,'ProductName']='合计'
for 账户 in ret.StrategyName.apply(lambda x:'_'.join(x.split('_')[1:])).unique():
    t1=ret[ret.StrategyName.str.endswith(账户)].copy()
    t1.loc[:,'StrategyName']=账户
    ret=ret.append(t1)

ret=ret.append(t0)
ret=ret.reset_index(drop=True)


# In[12]:


output=[]
for v,tp in ret.groupby(['StrategyName','BuySell','日期','ProductName']):
    #print(v,tp.Amt.sum(),(tp.perform_q*tp.Amt).sum()/tp.Amt.sum())

    output.append((v[0],v[1],v[2],v[3],tp.Amt.sum(),                   ((tp.Price_traded-tp.p_st)*tp.TradedVolume).sum()/(tp.p_st*tp.TradedVolume).sum(),                   ((tp.Price_traded-tp.p_avg)*tp.TradedVolume).sum()/(tp.p_avg*tp.TradedVolume).sum(),                  )
                 )
output=pd.DataFrame(output,columns=['策略','方向','日期','账户','交易额w','交易效率p','交易效率q'])
output.loc[:,'千分之（相对开盘）']=np.round((output.交易效率p)*1000,2)
output.loc[:,'千分之（相对均价）']=np.round((output.交易效率q)*1000,2)

output=output[output.交易额w>0]

output.loc[:,'交易额w']=np.round(output.loc[:,'交易额w']/10000,1).astype(int)
output.sort_values(by=['交易额w'],ascending=False,inplace=True)
output.loc[output.方向=='Buy',['千分之（相对开盘）','千分之（相对均价）']]=-output.loc[output.方向=='Buy',['千分之（相对开盘）','千分之（相对均价）']]
#output.to_excel('卖出效率_EIF_%s.xlsx'%str.upper(bs))


# In[13]:


ret_all1=output[output.方向=='Buy'].set_index(['日期','策略']).sort_values(by='交易额w')[[ '交易额w','千分之（相对开盘）','千分之（相对均价）']].stack().reset_index()
ret_all1.columns=['日期','A属性','B属性','取值']
ret_all1.B属性=ret_all1.B属性.apply(lambda x:'买'+x)
ret_all2=output[output.方向=='Sell'].set_index(['日期','策略']).sort_values(by='交易额w')[[ '交易额w','千分之（相对开盘）','千分之（相对均价）']].stack().reset_index()
ret_all2.columns=['日期','A属性','B属性','取值']
ret_all2.B属性=ret_all2.B属性.apply(lambda x:'卖'+x)
ret_all=ret_all1.append(ret_all2)

tp=ret_all.pivot(index=['日期','A属性'],columns='B属性',values='取值').fillna(0)
tp.loc[:,'总金额']=tp.买交易额w+tp.卖交易额w
tp.loc[:,'合计千分之（相对开盘）']=np.round((tp.loc[:,'买千分之（相对开盘）']*tp.loc[:,'买交易额w']+tp.loc[:,'卖千分之（相对开盘）']*tp.loc[:,'卖交易额w'])/tp.总金额*1.,2)
tp.loc[:,'合计千分之（相对均价）']=np.round((tp.loc[:,'买千分之（相对均价）']*tp.loc[:,'买交易额w']+tp.loc[:,'卖千分之（相对均价）']*tp.loc[:,'卖交易额w'])/tp.总金额*1.,2)

ret3=tp.stack().reset_index()
ret3.columns=['日期','A属性','B属性','取值']
ret3=ret3[ret3.B属性.isin(['合计千分之（相对开盘）','合计千分之（相对均价）'])]

ret_all=ret_all.append(ret3)

分析名称='算法绩效分析'

tp=ret_all[['日期','A属性','B属性','取值']]
tp=tp[tp.日期>=closew.index[-5]]
tp.loc[:,'分析名称']=分析名称
path='..//产出//%s//%s.xlsx'%(分析名称,closew.index[-1].strftime('%Y%m%d'))
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


# In[14]:


dts=list(closew.index)
ret_all=[]
for dt in dts[-80:]:
    k=dts.index(dt)
    dts_tp=dts[k-19:k+1]
    
    tp=output[(output.策略.isin(['hc_szlh','ky_cy','gt_xyss','gl_sz_alpha','hx_xyss','ht_ss','ht_mws']))&(output.日期.isin(dts_tp))]
    t1=tp.groupby('策略').apply(lambda x:(x.交易额w*x['千分之（相对均价）']).sum())/tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
    t2=tp.groupby('策略').apply(lambda x:(x.交易额w).sum())
    t=pd.concat([t1,t2],axis=1)
    t.columns=['绩效','交易额w']
    ret_hz=t.reset_index().iloc[:,:2]
    ret_hz.loc[:,'日期']=dt
    ret_all.append(ret_hz)
ret_hz=pd.concat(ret_all)

ret_hz.columns=['B属性','取值','日期']
ret_hz.loc[:,'A属性']=''
ret_hz.取值=np.round(ret_hz.取值,2)
分析名称='算法绩效20日平均'
tp=ret_hz[['日期','A属性','B属性','取值']]
tp.loc[:,'分析名称']=分析名称
path='..//产出//%s//%s.xlsx'%(分析名称,tp.日期.max().strftime('%Y%m%d'))
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


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




