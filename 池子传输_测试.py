#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests

def import_model_stockpool(model_name, model_version, apply_date, file_path):
    url = "https://10.10.0.86:26829/facts-stockpool/backend/model_stockpool/import_one"
    headers = {
        'Authorization': 'Bearer c738bdc8-c819-435e-b277-cf84c4ce4353',
    }
    #apply_date='2023-12-29'
    data = {
        'model_name': model_name,
        'model_version': model_version,
        'apply_date': apply_date,
    }
    files = {
        'file': (file_path.split('/')[-1], open(file_path, 'rb')),
    }
    response = requests.post(url, headers=headers, data=data, files=files,verify=False)
    files['file'][1].close()
    return response


# In[2]:


root='/home/jovyan/work/workspaces/daily report/次日列表/EIF候选池'
import os
EIF_mls=os.listdir(root)


# In[3]:


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


# In[4]:


测试策略列表=(('E1ALA0','E3HSA0','E5HSA0'))

import numpy as np
apply_date = pd.to_datetime(nxt_trd_day).strftime('%Y-%m-%d')
for ml in EIF_mls:
    if apply_date.replace('-','') in ml and 'pred' not in ml and ml.startswith(测试策略列表):
        print(ml)
        tp=pd.read_csv(os.path.join(root,ml),header=None)

        #if tp[1].sum()==0:
        if tp[1].dropna().count()==0:
            print(ml,'-------------------------------->')
            error


# In[5]:


for ml in EIF_mls:
    if apply_date.replace('-','') in ml and 'pred' not in ml and ml.startswith(测试策略列表):
        model_name=ml.split('Pool')[0]
        file_path = os.path.join(root,ml)
        if model_name in 'E1ALT,E1TFT,E2HST,EAALT,EAGAT,EAHST,EATFT,EDLVT,EATFK,EDHST,ETLMS,EVHST'.split(','):
            model_version='20231231'
            print(model_name,model_version)
            response = import_model_stockpool(model_name, model_version, apply_date, file_path)
        elif model_name in 'SISIG,SISUBA'.split(','):
            model_version='20230706'
            print(model_name,model_version)
            response = import_model_stockpool(model_name, model_version, apply_date, file_path)
        else:
            try:
                model_version=max([i for i in os.listdir(os.path.join('/home/jovyan/work/workspaces/daily report/实盘模型/models',model_name)) if i.startswith('20')])
                print(model_name,model_version)
                response = import_model_stockpool(model_name, model_version, apply_date, file_path)
            except:
                model_version='20230911'
                print(model_name,model_version)
                response = import_model_stockpool(model_name, model_version, apply_date, file_path)
                pass
        


# In[6]:


import os
import pandas as pd
funds_allocation_directory='/home/jovyan/work/workspaces/daily report/次日列表/指增资金分配_测试/'
mls=os.listdir(funds_allocation_directory)
ml=max([i for i in mls if 'raw' not in i])
策略列表=pd.read_excel(os.path.join(funds_allocation_directory,ml)).sort_values(by='资金',ascending=False)
if '策略类型' in 策略列表.columns:
    del 策略列表['策略类型']


# In[7]:


策略列表=策略列表.drop_duplicates(['账户.1','策略名'])


# In[8]:


tp=[]
临时添加=False
for i in 策略列表.index:
    t={}
    product=策略列表.loc[i,'账户.1']
    strategy=策略列表.loc[i,'策略名']
    pool_type='geod'
    model_name=策略列表.loc[i,'策略池']
    
    if 临时添加:
        calculation_batch=8
    elif '量化中性' in 策略列表.loc[i,'账户'] and 'E1FAA' in 策略列表.loc[i,'策略池']:
        calculation_batch=4
    elif 'PAPER' in 策略列表.loc[i,'账户']:
        calculation_batch=5
    elif 策略列表.loc[i,'策略名'].startswith('EM'):
        calculation_batch=8
    else:
        calculation_batch=7
    try:
        model_version=max([i for i in os.listdir(os.path.join('/home/jovyan/work/workspaces/daily report/实盘模型/models',model_name)) if i.startswith('20')])
        #print(model_name,model_version)
    except:
        model_version='20230911'
    if model_name in ['TPLT','TPLT2','E5HSA0','ETF1000','E1ALH0','E1ALA0','E3HSA0','E50','E300','EDLVT','E1FAA','E2FAA','E3HSA','E1IGA','E5IGA','E3IGA','EMIGA', 'EAIGAHG', 'EMFAA', 'LPPM', 'LPP','E2ALA','ECALA','EMHSA','EADEA','EMIGAHG','E1ALH','E1ALA','hgalgo','hgalgo2311','ETVLS','EMHST','EMALT']:
        is_manual_import=True
    else:
        is_manual_import=False
    if model_name in 'E1ALT,E1TFT,E2HST,EAALT,EAGAT,EAHST,EATFT,EDLVT,EDHST,ETLMS'.split(','):
        model_version='20231231'
    elif model_name in 'EATFK'.split(','):
        model_version='20230706'
        #print(model_name,model_version)
    for v in 'product,strategy,pool_type,model_name,calculation_batch,model_version,is_manual_import'.split(','):
        t[v]=eval(v)
    #t['params']=str(策略列表.loc[i,'其他参数']).replace('nan','')
    if (((datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y%m%d')==lst_trd_day) or datetime.datetime.now().hour>15) and model_name in ['LPPM', 'LPP']:
        continue
    tp.append(t)


# In[9]:


for v in tp:
    if v['model_name'] in ['t0_model','open_928']:
        v['is_manual_import']=False
    else:
        v['is_manual_import']=True


# In[10]:


tp


# In[11]:


import requests
import json

url = "https://10.10.0.86:26829/facts-stockpool/backend/strategy_model_map/create_multiple"
headers = {
    "accept": "application/json",
    'Authorization': 'Bearer c738bdc8-c819-435e-b277-cf84c4ce4353',
    "Content-Type": "application/json",
}
data = {
    "map_list": tp
}

response = requests.post(url, headers=headers, data=json.dumps(data),verify=False)

# print the response
print(response.json())


# In[12]:


tp=策略列表[['账户.1','策略名','股票数','资金','换仓比例','绝对阈值','调仓频率','其他参数']].reset_index(drop=True)
tp.columns='产品,策略,期望标的,资金量,调仓比例,调仓阈值,调仓频率,params'.split(',')


tp.to_csv('策略参数设置模板_测试.csv',index=False)


# In[13]:


url_upload = "https://10.10.0.86:26829/facts-stockpool/backend/strategy_settings/upload_excel"
headers = {
    "accept": "application/json",
    "Authorization": 'Bearer c738bdc8-c819-435e-b277-cf84c4ce4353',
}
files = {"file": open("策略参数设置模板_测试.csv", "rb")}
response_upload = requests.post(url_upload, headers=headers, files=files,verify=False)
output = response_upload.json()

# 提取结果中的 'result' 字段
output_result = output['result']

# 将结果作为 JSON 对象传递给 API
url_create = "https://10.10.0.86:26829/facts-stockpool/backend/strategy_settings/create_multiple"
headers = {
    "accept": "application/json",
    "Authorization": 'Bearer c738bdc8-c819-435e-b277-cf84c4ce4353',
    "Content-Type": "application/json",
}
data = {"strategy_list": output_result}
response_create = requests.post(url_create, headers=headers, data=json.dumps(data),verify=False)


# In[14]:


import os
command = 'jupyter nbconvert --to script 池子传输_测试.ipynb'
os.system(command)


# In[ ]:





# In[ ]:





# In[ ]:




