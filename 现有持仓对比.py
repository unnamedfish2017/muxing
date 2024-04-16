#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import datetime
import tushare as ts
import time,os
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
df = pro.query('trade_cal', start_date='20220101', end_date='20301201').sort_values(by='cal_date').reset_index(drop=True)
dts_open=list(df[df.is_open>0].cal_date)
df=df[(df.is_open>0)&(df.cal_date<=lst_day)]
lst_trd_day=str(df.cal_date.values[-1])
nxt_trd_day=dts_open[dts_open.index(lst_trd_day)+1]
import pandas as pd
root='/home/jovyan/work/workspaces/daily report/每日报表/持仓数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('盘后持仓')])))
现有持仓=df[df.日期==pd.to_datetime(lst_trd_day)]
print(lst_trd_day,'持仓已获取')


# In[2]:


root='/home/jovyan/work/workspaces/daily report/每日报表/原始数据_托管'
mls=os.listdir(root)


# In[3]:


keyword='position'
account='KY'
def get_(keyword,account,dt):
    root='/home/jovyan/work/workspaces/daily report/每日报表/原始数据_托管'
    try:
        path=max([i for i in mls if keyword in i and i.endswith(account+'.xlsx') and dt in i])
        path=os.path.join(root,path)
        return path
    except:
        return None


# In[4]:


策略映射=pd.read_csv('/home/jovyan/work/workspaces/daily report/每日报表/代码/策略映射.csv',encoding='gbk').set_index('实际策略名').to_dict()['跟踪策略名']
账户=pd.read_csv('/home/jovyan/work/workspaces/daily report/每日报表/代码/账户.csv',encoding='gbk').set_index('实际账户名').to_dict()['跟踪账户名']


# In[5]:


明细all=pd.DataFrame([])
for keyword in ['position']:
    for account in ['KY','QMT','HC']:
        path=get_(keyword,account,lst_trd_day)
        if path:
            tp=pd.read_excel(path,sheet_name='策略明细表').ffill()
            明细all=明细all.append(tp)
        else:
            print(account,'miss')


# In[6]:


tp=pd.DataFrame.from_dict(策略映射,orient='index').reset_index()
tp.columns=['策略名称','策略']
明细all=明细all.merge(tp,how='left')

tp=pd.DataFrame.from_dict(账户,orient='index').reset_index()
tp.columns=['账户名称','账户']
明细all=明细all.merge(tp,how='inner')   #####只统计这些账户的表现


# In[7]:


明细all=明细all[['策略','账户','交易标的','量','机构名称']]
明细all.columns=['策略','账户','Instrument','系统量','标的名称']


# In[8]:


差异=明细all.merge(现有持仓,how='outer')
差异=差异[差异.策略.str.startswith(('EA','E1','ET'))]
差异=差异[差异.系统量!=差异.累计量]


# In[9]:


差异.策略.unique()


# In[10]:


差异.groupby(by=['账户','策略']).持有市值.sum()


# In[11]:


#差异[(差异.策略=='E1LG90TNI')&(差异.账户=='狮子量化 华创 TRADEX SM')]
差异[(差异.策略=='E1LG90TNI')&(差异.账户!='量化PAPER')]


# In[12]:


root


# In[13]:


import sys
sys.path.append('/home/jovyan/work/workspaces/daily report/每日报表/代码/')
from 报表函数 import *
ios_all,ods_all=get_records(root)


# In[14]:


ios_all,ods_all=get_records(root)


# In[15]:


def get_records(root):
    import os
    ods_all=[]
    ios_all=[]

    for v in ['KY','HC','QMT']:
        mls=[i for i in os.listdir(root) if i.endswith(v+'.csv')]
        mls1=[i for i in mls if 'Deposit' in i]
        mls1=sorted(mls1)[::-1][::30]+sorted(mls1)[:1]
        for i in mls1:
            ios=pd.read_csv(os.path.join(root,i))
            ios_all.append(ios)

    for v in ['KY','HC','QMT']:
        mls=[i for i in os.listdir(root) if i.endswith(v+'.csv')]
        mls2=[i for i in mls if 'Orders' in i]
        mls2=sorted(mls2)[::-1][::30]+sorted(mls2)[:1]
        for i in mls2:
            df=pd.read_csv(os.path.join(root,i), sep='$')
            if len(df)==0 or i[:8]<'20230822':
                continue
            clms=df.columns[0].split(',')[:-1]+['OrderRef']#+'OrderRef,LocalID,SysID'.split(',')
            ods=df.iloc[:,0].str.split(',',expand=True).iloc[:,:len(clms)]
            ods.columns=clms
            ods.loc[:,'rk']=ods.groupby(['ProductName','AccountName','BuySell','Instrument','StrategyName','TradedVolume','DealDateTime']).RequestDateTime.rank(method='first')
            ods.loc[:,'source']=i

            ods_all.append(ods)
            for v in ['DealDateTime', 'RequestDateTime']:
                ods[v]=pd.to_datetime(ods[v])
            print(ods[(ods.Instrument=='SZSE.S+300858')&(ods.StrategyName=='EATF98TXX2')&(ods.ProductName=='KY_CY')])
                
    for v in ['KY','HC','QMT']:
        mls=[i for i in os.listdir(root) if i.endswith('.csv') and 'hist_order' in i and v in i]

        for i in mls:
            df=pd.read_csv(os.path.join(root,i), sep='$')
            if len(df)==0:
                continue
            clms=df.columns[0].split(',')[:-1]+['OrderRef']#+'OrderRef,LocalID,SysID'.split(',')
            ods=df.iloc[:,0].str.split(',',expand=True).iloc[:,:len(clms)]
            ods.columns=clms
            ods.loc[:,'rk']=ods.groupby(['ProductName','AccountName','BuySell','Instrument','StrategyName','TradedVolume','DealDateTime']).RequestDateTime.rank(method='first')
            ods.loc[:,'source']=i

            ods_all.append(ods)
            for v in ['DealDateTime', 'RequestDateTime']:
                ods[v]=pd.to_datetime(ods[v])
                
    ods_all_bak=pd.concat(ods_all)            
    ods_all=pd.concat(ods_all).drop_duplicates(['ProductName','AccountName','BuySell','Instrument','StrategyName','TradedVolume','DealDateTime','rk'])
    ods_all=ods_all[(ods_all.TradedPrice.astype(float)>0)|(ods_all.OrderRef=='Virtual.Open')|(ods_all.OrderRef=='ExDividend Virtual.Close')                    |(ods_all.OrderRef=='ExRight Virtual.Open')]
    
    #print(ods_all[(ods_all.Instrument=='SHSE.S+603690')&(ods_all.StrategyName=='E1LG90TNI')&(ods_all.ProductName=='HC_SZLH')].OrderRef.unique())
    ios_all=pd.concat(ios_all).drop_duplicates()
    ios_all.SaveTime=pd.to_datetime(ios_all.SaveTime)

    ios_all=ios_all.sort_values('SaveTime')
    ods_all=ods_all.sort_values('DealDateTime')
    return ios_all,ods_all


# In[16]:


ods_all=ods_all.sort_values(by='DealDateTime').reset_index(drop=True)
ods_all[['TradedVolume','RequestVolume']]=ods_all[['TradedVolume','RequestVolume']].astype(int)
ods_all[['TradedPrice']]=ods_all[['TradedPrice']].astype(float)
ods_all.loc[:,'TradedAmount']=ods_all.loc[:,'TradedVolume']*ods_all.loc[:,'TradedPrice']
ods_all.loc[:,'代码']=ods_all.loc[:,'Instrument']

tp=pd.DataFrame.from_dict(策略映射,orient='index').reset_index()
tp.columns=['StrategyName','策略']
ods_all=ods_all.merge(tp,how='left')

tp=pd.DataFrame.from_dict(账户,orient='index').reset_index()
tp.columns=['AccountName','账户']
ods_all=ods_all.merge(tp,how='inner')   #####只统计这些账户的表现


# In[18]:


#t=差异[(差异.策略=='E1LG90TNI')&(差异.账户!='量化PAPER')].sample()


# In[23]:


# 策略_=t.T.loc['策略'].item()
# 标的_=t.T.loc['Instrument'].item()
# 账户_=t.T.loc['账户'].item()
# ods_all[(ods_all.策略==策略_)&(ods_all.Instrument==标的_)&(ods_all.账户==账户_)]


# In[24]:


import os
root='/home/jovyan/work/workspaces/daily report/每日报表/持仓数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('盘后持仓')])))
现有持仓1=df[df.日期==pd.to_datetime(lst_trd_day)]
print(lst_trd_day,'持仓已获取',现有持仓1.shape)

import os
root='/home/jovyan/work/workspaces/daily report/每日报表/持仓数据'
mls=os.listdir(root)
df=pd.read_parquet(os.path.join(root,max([i for i in mls if i.startswith('系统_盘后持仓')])))
现有持仓2=df[df.日期==pd.to_datetime(lst_trd_day)]
print(lst_trd_day,'持仓已获取',现有持仓2.shape)


# In[25]:


tp2=现有持仓2[['策略', '账户', 'Instrument', '累计量']]
tp2.columns=['策略', '账户', 'Instrument', '系统量']
差异=现有持仓1.merge(tp2,how='left')


# In[42]:


差异[(差异.累计量!=差异.系统量)&(差异.策略=='E1AL90T')&(差异.账户=='狮子量化 华创 TRADEX SM')]


# In[43]:


策略_='E1AL90T'
标的_='SZSE.S+002015'
账户_='狮子量化 华创 TRADEX SM'
ods_all[(ods_all.策略==策略_)&(ods_all.Instrument==标的_)&(ods_all.账户==账户_)].sort_values(by='DealDateTime')


# In[28]:


差异[差异.累计量!=差异.系统量].groupby(['账户','策略']).持有市值.count()


# In[29]:


tp=差异[(差异.累计量!=差异.系统量)&(差异.策略.str.startswith('E'))&(差异.账户=='狮子量化 华创 TRADEX SM')]
tp.loc[:,'差值']=tp.累计量-tp.系统量.fillna(0)
tp.to_excel('order和系统差异.xlsx',index=False)


# In[30]:


tp


# In[ ]:





# In[ ]:





# In[ ]:




