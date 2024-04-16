#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
import os
import pandas as pd


# In[2]:


tp0=pd.read_csv('/home/jovyan/data/store/rsync/data_daily//申万行业股票列表2022_renew.csv',encoding='gbk')
tp0=tp0[['code','申万一级','申万二级']]
pct_base=tp0.groupby('申万一级').code.count()/tp0.code.count()


# In[ ]:





# In[3]:


base_index='000985.SH'
import pandas as pd
tp=pd.read_parquet('/home/jovyan/data/store/rsync/data_daily/指数成分/%s.parquet'%base_index)
base_pool=list(tp[tp.date==tp.date.max()].code.str.lower())


# In[13]:


import os

def find_matching_directories(root_dir, file_extension, keyword, starts_with):
    matching_directories = []

    for dirpath, dirnames, filenames in os.walk(root_dir):
        for dirname in dirnames:
            for filename in filenames:
                if filename.endswith(file_extension) and keyword in filename and filename.startswith(starts_with):
                    matching_directories.append(os.path.join(dirpath,filename))
    return matching_directories

root_folder = '/home/jovyan/work/workspaces/daily report/次日列表/PAT列表'  # 替换为您的根文件夹路径
#root_folder = '/home/jovyan/work/workspaces/daily report/次日列表/EIF候选池'  # 替换为您的根文件夹路径
file_extension = ".csv"
keyword =lst_day#nxt_trd_day
starts_with = ("EA", "LP")  # 以EA或LP开头的文件夹

result = find_matching_directories(root_folder, file_extension, keyword, starts_with)

# for directory in result:
#     print(directory)


# In[14]:


import numpy as np
for ml in result:
    df=pd.read_csv(ml,header=None)
    df.columns=['code','score']
    df.code=df.code.apply(lambda x:x[-6:]+'.'+x[:2].lower())
    if len(df)>=10 and 'APool' in ml:
        df=df.merge(tp0,how='left')
        pct=df.groupby('申万一级').sum()
        hy_pct=pd.concat([pct,pct_base],axis=1)
        hy_pct=hy_pct.fillna(0)*100
        hy_pct.columns=['权重','原始权重']
        hy_pct[['权重','原始权重']]=hy_pct[['权重','原始权重']].astype(int)
        hy_pct.loc[:,'gap']=hy_pct.权重.fillna(0)-hy_pct.原始权重
        hy_pct.sort_values(by='gap',inplace=True)
        print('---------------------------------------------')
        print(ml)
        print(hy_pct)
        


# In[15]:


# import os

# def find_matching_directories(root_dir, file_extension, keyword, starts_with):
#     matching_directories = []

#     for dirpath, dirnames, filenames in os.walk(root_dir):
#         for dirname in dirnames:
#             for filename in filenames:
#                 if filename.endswith(file_extension) and keyword in filename and filename.startswith(starts_with):
#                     matching_directories.append(os.path.join(dirpath,filename))
#     return matching_directories

# root_folder = '/home/jovyan/work/workspaces/daily report/次日列表/EIF候选池'  # 替换为您的根文件夹路径
# file_extension = ".csv"
# keyword = "20231013"
# starts_with = ("EA")  # 以EA或LP开头的文件夹

# result = find_matching_directories(root_folder, file_extension, keyword, starts_with)

# for directory in result:
#     print(directory)


# In[16]:


# directory='/home/jovyan/work/workspaces/daily report/次日列表/EIF候选池/EADEAPool_20231016.csv'


# In[17]:


# df=pd.read_csv(directory,header=None)
# df.columns=['code','score']
# df.code=df.code.apply(lambda x:x[-6:]+'.'+x[:2].lower())
# dfbase=pd.read_csv('/home/jovyan/data/store/rsync/data_daily//申万行业股票列表2022_renew.csv',encoding='gbk')
# dfbase=dfbase[['code','申万一级','申万二级']]


# In[18]:


# def modify_list(df0,dfbase,up_limit=0.02,md=True):
#     df=df0.merge(dfbase,how='left').sort_values(by='score',ascending=False).reset_index(drop=True)
    
#     df.loc[:,'score']=df.loc[:,'score'].rank(pct=True)
#     df.loc[:,'score']=df.loc[:,'score'].values-df.groupby(['申万一级'])['score'].transform('mean').values
    
#     df.dropna(subset=['申万一级'],inplace=True)
#     df.sort_values(by='score',ascending=False,inplace=True)
#     pct_base=dfbase.groupby('申万一级').code.count()/dfbase.code.count()
#     pct_base=pct_base.to_dict()

#     ret=pd.Series(0,index=pct_base.keys())
#     ret_list=[]
#     tp=df[['code','申万一级']].values.tolist()

#     n=1
#     ret_dict=ret.to_dict()
#     total_n=0
#     flag=1
#     while len(tp) and n<=5000**2 and flag :
#         flag=0
#         for i in range(len(tp)):
#             n+=1
#             if (ret_dict[tp[i][1]]+1)/(total_n+1)<=pct_base[tp[i][1]]+up_limit or ret_dict[tp[i][1]]<=1:
#                 ret_list.append(tp[i][0])
#                 ret_dict[tp[i][1]]+=1
#                 total_n+=1
#                 tp.pop(i)
#                 flag=1
#                 break
#     t=pd.DataFrame(ret_list,columns=['code'])
#     t.loc[:,'score']=np.array(range(len(t)))[::-1]/(len(t)-1)
#     return t


# In[19]:


# N=100
# t=modify_list(df,dfbase,up_limit=0.01).head(N)
# t.loc[:,'score']=1/N
# df=t.merge(tp0,how='left')
# pct=df.groupby('申万一级').sum()
# hy_pct=pd.concat([pct,pct_base],axis=1)
# hy_pct=hy_pct.fillna(0)*100
# hy_pct.columns=['权重','原始权重']
# hy_pct[['权重','原始权重']]=hy_pct[['权重','原始权重']].astype(int)
# hy_pct.loc[:,'gap']=hy_pct.权重.fillna(0)-hy_pct.原始权重
# hy_pct.sort_values(by='gap',inplace=True)
# print('---------------------------------------------')
# print(directory)
# print(hy_pct)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




