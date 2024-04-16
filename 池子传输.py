#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests

def import_model_stockpool(model_name, model_version, apply_date, file_path):
    url = "https://61.172.245.225:26829/facts-stockpool/backend/model_stockpool/import_one"
    headers = {
        'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
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


# import numpy as np
# root='/home/jovyan/work/workspaces/daily report/次日列表/EIF候选池'
# import os
# EIF_mls=os.listdir(root)
# for dt in dts_open[-300:]:
#     apply_date = pd.to_datetime(dt).strftime('%Y-%m-%d')
#     for ml in EIF_mls:
#         if apply_date.replace('-','') in ml and 'pred' not in ml:
#             tp=pd.read_csv(os.path.join(root,ml),header=None)

#             if 1 in tp.columns and tp[1].sum()==0:
#                 print(ml,'-------------------------------->')


# In[5]:


import numpy as np
apply_date = pd.to_datetime(nxt_trd_day).strftime('%Y-%m-%d')
for ml in EIF_mls:
    if apply_date.replace('-','') in ml and 'pred' not in ml:
        print(ml)
        tp=pd.read_csv(os.path.join(root,ml),header=None)

        #if tp[1].sum()==0:
        if tp[1].dropna().count()==0:
            print(ml,'-------------------------------->')
            error


# In[6]:


for ml in EIF_mls:
    if apply_date.replace('-','') in ml and 'pred' not in ml:
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
                print(model_name,model_version,ml)
                response = import_model_stockpool(model_name, model_version, apply_date, file_path)
                pass
        


# In[7]:


root1='/home/jovyan/work/workspaces/daily report/次日列表/其他策略池'
mls=os.listdir(root1)
for ml in mls:
    if apply_date.replace('-','') in ml and ml.startswith(('MaAl1315Pool','MaAl15Pool','MaAl13Pool','MaMlMixPool','TkMl','HG1','HG2','EMSR')) and 'pred' not in ml :
        model_name=ml.split('Pool')[0]
        file_path = os.path.join(root1,ml)
        try:
            model_version=[i for i in os.listdir(os.path.join('/home/jovyan/work/workspaces/daily report/实盘模型/models',model_name)) if i.startswith('20')][0]
            print(model_name,model_version)
            response = import_model_stockpool(model_name, model_version, apply_date, file_path)
        except:
            model_version='20230911'
            print(model_name,model_version)
            response = import_model_stockpool(model_name, model_version, apply_date, file_path)
            pass
        


# In[8]:


import os
import pandas as pd
funds_allocation_directory='/home/jovyan/work/workspaces/daily report/次日列表/指增资金分配/'
mls=os.listdir(funds_allocation_directory)
ml=max([i for i in mls if 'raw' not in i])
策略列表=pd.read_excel(os.path.join(funds_allocation_directory,ml)).sort_values(by='资金',ascending=False)
if '策略类型' in 策略列表.columns:
    del 策略列表['策略类型']


# In[9]:


tp=[
  #{"product": "KY_CY_XY","strategy": "TpltLong_Test","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
    
#   {"product": "HC_SZLH","strategy": "E1AL90A","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#       {"product": "HC_SZLH","strategy": "EAIG96A","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#       {"product": "HC_SZLH","strategy": "EAAL98T","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#       {"product": "HC_SZLH","strategy": "EAHS92A","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#       {"product": "HC_SZLH","strategy": "EAHS98T","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#       {"product": "HC_SZLH","strategy": "E1AL90T","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#       {"product": "HC_SZLH","strategy": "E1TF90T","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#       {"product": "HC_SZLH","strategy": "E1IG90A","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#   {"product": "HX_XYSS","strategy": "E1AL90A","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#       {"product": "HX_XYSS","strategy": "E3HS90A","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#       {"product": "HX_XYSS","strategy": "E1IG90A","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#       {"product": "HX_XYSS","strategy": "EAAL98T","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},   
# {"product": "GL_SZ_ALPHA","strategy": "EAIG98A","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},
#     {"product": "GL_SZ_ALPHA","strategy": "EAHS98T","pool_type": "e925","model_name": "open_928", "calculation_batch": 1},

    
#{"product": "GT_XYSS","strategy": "MaAl13","pool_type": "geod","model_name": "MaAl13", "calculation_batch": 3,'model_version':'20231009'},
#{"product": "GT_XYSS","strategy": "MaAl1315","pool_type": "geod","model_name": "MaAl1315", "calculation_batch": 3,'model_version':'20231009'},

{"product": "DW_RW","strategy": "MaAl13","pool_type": "geod","model_name": "MaAl13", "calculation_batch": 3,'model_version':'20231009'},
{"product": "DW_RW","strategy": "MaAl1315","pool_type": "geod","model_name": "MaAl1315", "calculation_batch": 0,'model_version':'20231009'},
{"product": "DW_RW","strategy": "MaAl15","pool_type": "geod","model_name": "MaAl15", "calculation_batch": 3,'model_version':'20231203'},
    
{"product": "HX_XYSS","strategy": "MaAl13","pool_type": "geod","model_name": "MaAl13", "calculation_batch": 3,'model_version':'20231009'},
{"product": "HX_XYSS","strategy": "MaAl1315","pool_type": "geod","model_name": "MaAl1315", "calculation_batch": 3,'model_version':'20231009'},
{"product": "CJ_SY","strategy": "MaAl13","pool_type": "geod","model_name": "MaAl13", "calculation_batch": 3,'model_version':'20231009'},
{"product": "CJ_SY","strategy": "MaAl1315","pool_type": "geod","model_name": "MaAl1315", "calculation_batch": 3,'model_version':'20231009'},
{"product": "DW_SY","strategy": "MaAl1315","pool_type": "geod","model_name": "MaAl1315", "calculation_batch": 3,'model_version':'20231009'},
{"product": "DW_SY","strategy": "MaAl13","pool_type": "geod","model_name": "MaAl13", "calculation_batch": 3,'model_version':'20231009'},
# {'product': 'QUANT_PAPER', 'strategy': 'LPPM', 'pool_type': 'geod', 'model_name': 'LPPM', 'calculation_batch': 5, 'model_version': '20230911'}, 
#{"product": "HC_SZLH","strategy": "MaAl13","pool_type": "geod","model_name": "MaAl13", "calculation_batch": 3,'model_version':'20231009'},
#{"product": "HC_SZLH","strategy": "MaMlMix","pool_type": "geod","model_name": "MaMlMix", "calculation_batch": 3,'model_version':'20230620'},
#{"product": "HT_SS","strategy": "HG1","pool_type": "geod","model_name": "HG1", "calculation_batch": 0,'model_version':'20230911'},
#{"product": "HT_SS","strategy": "HG2","pool_type": "geod","model_name": "HG2", "calculation_batch": 0,'model_version':'20230911'},
{"product": "HC_SZLH","strategy": "EMSR","pool_type": "geod","model_name": "EMSR", "calculation_batch": 0,'model_version':'20230911'},
    
#{"product": "DW_RW","strategy": "hgalgo","pool_type": "geod","model_name": "hgalgo", "calculation_batch": 0,'model_version':'20230911'},
  ]


# In[10]:


for v in ['QUANT_PAPER', 'KY_CY', 'KY_LHZX', 'GT_RW_DMA', 'GT_SY_DMA', 'HT_MWS', 'HC_SZLH', 'HXN_LHZX', 'HT_SZLH']:
    vv='SISIG'
    tt={'product': v, 'strategy': vv, 'pool_type': 'geod', 'model_name': vv, 'calculation_batch': 0, 'model_version': '20230706'}
    tp.append(tt)
for v in ['KY_LHZX', 'GT_RW_DMA', 'GT_SY_DMA', 'GT_XYSS', 'QUANT_PAPER', 'HT_MWS', 'HC_SZLH', 'GL_SZ_ALPHA']:
    vv='SISUBA'
    tt={'product': v, 'strategy': vv, 'pool_type': 'geod', 'model_name': vv, 'calculation_batch': 0, 'model_version': '20230706'}
    tp.append(tt)
for v in ['KY_CY']:
    for vv in ['SISIG','SISUBA']:
        tt={'product': v, 'strategy': vv+'XX2', 'pool_type': 'geod', 'model_name': vv, 'calculation_batch': 0, 'model_version': '20230706'}
        tp.append(tt)

tt={'product':'GT_RW_DMA', 'strategy': 'SISIGB', 'pool_type': 'geod', 'model_name': 'SISIG', 'calculation_batch': 0, 'model_version': '20230706'}
tp.append(tt)
tt={'product':'GT_RW_DMA', 'strategy': 'E50', 'pool_type': 'geod', 'model_name': 'E50', 'calculation_batch': 0, 'model_version': '20230911'}
tp.append(tt)
tt={'product':'GT_SY_DMA', 'strategy': 'E50', 'pool_type': 'geod', 'model_name': 'E50', 'calculation_batch': 0, 'model_version': '20230911'}
tp.append(tt)

# for product in ['KY_LHZX','HXN_LHZX','HC_SZLH']:
#     tt={'product':product, 'strategy': 'ETF1000', 'pool_type': 'geod', 'model_name': 'ETF1000', 'calculation_batch': 0, 'model_version': '20230911'}
#     tp.append(tt)
#     tt={'product':product, 'strategy': 'ETF500', 'pool_type': 'geod', 'model_name': 'ETF500', 'calculation_batch': 0, 'model_version': '20230911'}
#     tp.append(tt)


# for product in ['HT_MWS','HT_SS','GT_XYSS','KY_CY','HC_SZLH']:
#     tt={'product':product, 'strategy': 'TPLT', 'pool_type': 'geod', 'model_name': 'TPLT', 'calculation_batch': 0, 'model_version': '20230911'}
#     tp.append(tt)
# for product in ['HT_MWS','HT_SS','GT_XYSS','KY_CY','HC_SZLH']:
#     tt={'product':product, 'strategy': 'TPLT2', 'pool_type': 'geod', 'model_name': 'TPLT2', 'calculation_batch': 0, 'model_version': '20230911'}
#     tp.append(tt)


# In[11]:


# tt={"product": "GT_XYSS","strategy": "SISUBA","pool_type": "t0m35","model_name": "t0m35_model", "calculation_batch": 2}
# tp.append(tt)


# In[ ]:





# In[12]:


pd.DataFrame([i for i in tp if i['model_name']=='open_928']).to_parquet('/home/jovyan/work/commons/records/open_928_list.parquet')


# In[13]:


for v in tp:
    if v['model_name'] in ['t0_model','open_928']:
        v['is_manual_import']=False
    else:
        v['is_manual_import']=True


# In[14]:


策略列表=策略列表.drop_duplicates(['账户.1','策略名'])


# In[15]:


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
        print(策略列表.loc[i,'策略名'])
    elif 策略列表.loc[i,'策略名'].startswith('TPLT'):
        calculation_batch=9
    else:
        calculation_batch=7
    
    try:
        model_version=max([i for i in os.listdir(os.path.join('/home/jovyan/work/workspaces/daily report/实盘模型/models',model_name)) if i.startswith('20')])
        #print(model_name,model_version)
    except:
        model_version='20230911'
    if model_name in ['E5TRS0','E1TRSA0','E1ALPA01','E1ALPA05','E1ALPA0','E5ALPA0','EAALA0','ETF500',                      'ETF1000','E1TRS0','TPLT','TPLT2','E5HSA0','E1ALH0','E1ALH02','EMALA','E1ALA0',                      'E1ALA03','E1ALA02','E3HSA00','E1ALA00','E3HSA0','E3HSA02','E50','E300','EDLVT',                      'E1FAA','E2FAA','E3HSA','E1IGA','E5IGA','E3IGA','EMIGA', 'EAIGAHG', 'EMFAA', 'LPPM',                      'LPP','E2ALA','ECALA','EMHSA','EADEA','EMIGAHG','E1ALH','E1ALA','hgalgo','hgalgo2311','ETVLS','EMHST','EMALT',                     'E5ALPA0_S','E1ALPA0_S','E1TRN0','E1TRD0','E3ALA0','E3ALA','E3TRS0','E5TRN0','E1TRN0','E5FAA']:
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


# In[16]:


策略列表.策略池.unique()


# In[17]:


t=pd.DataFrame(tp)
t[t.calculation_batch==7].shape


# In[18]:


import requests
import json

url = "https://61.172.245.225:26829/facts-stockpool/backend/strategy_model_map/create_multiple"
headers = {
    "accept": "application/json",
    "Authorization": "Bearer 8c38d671-e3da-4bad-9445-a3ca2f45940f",
    "Content-Type": "application/json",
}
data = {
    "map_list": tp
}

response = requests.post(url, headers=headers, data=json.dumps(data),verify=False)

# print the response
print(response.json())


# In[19]:


tp=策略列表[['账户.1','策略名','股票数','资金','换仓比例','绝对阈值','调仓频率','其他参数']].reset_index(drop=True)
tp.columns='产品,策略,期望标的,资金量,调仓比例,调仓阈值,调仓频率,params'.split(',')

tp_add=pd.read_excel('..//素材//产品策略配置检查调整.xlsx')
tp_add=tp_add[tp_add.资金量>0]#.iloc[:,:3]
for v in '产品,策略,期望标的,资金量,调仓比例,调仓阈值,调仓频率,params'.split(','):
    if v not in tp_add.columns:
        tp_add.loc[:,v]=''
tp_add=tp_add['产品,策略,期望标的,资金量,调仓比例,调仓阈值,调仓频率,params'.split(',')]
tp=tp.append(tp_add)

tp.to_csv('策略参数设置模板.csv',index=False)


# In[20]:


url_upload = "https://61.172.245.225:26829/facts-stockpool/backend/strategy_settings/upload_excel"
headers = {
    "accept": "application/json",
    "Authorization": "Bearer 8c38d671-e3da-4bad-9445-a3ca2f45940f",
}
files = {"file": open("策略参数设置模板.csv", "rb")}
response_upload = requests.post(url_upload, headers=headers, files=files,verify=False)
output = response_upload.json()

# 提取结果中的 'result' 字段
output_result = output['result']

# 将结果作为 JSON 对象传递给 API
url_create = "https://61.172.245.225:26829/facts-stockpool/backend/strategy_settings/create_multiple"
headers = {
    "accept": "application/json",
    "Authorization": "Bearer 8c38d671-e3da-4bad-9445-a3ca2f45940f",
    "Content-Type": "application/json",
}
data = {"strategy_list": output_result}
response_create = requests.post(url_create, headers=headers, data=json.dumps(data),verify=False)


# In[21]:


import os
command = 'jupyter nbconvert --to script 池子传输.ipynb'
os.system(command)


# In[22]:


tp_add=pd.read_excel('..//素材//产品策略配置检查调整.xlsx')
tp_add=tp_add[tp_add.white_list>0].iloc[:,:2]
tp_add.to_csv('..//素材//whitelist.csv',index=False)


# In[23]:


import requests

def import_whitelist(file_path):
    url = "https://61.172.245.225:26829/facts-stockpool/api/autoconfig/import_whitelist"
    headers = {
        'Authorization': 'Bearer 8c38d671-e3da-4bad-9445-a3ca2f45940f',
        
    }
    files = {
        'file': (file_path.split('/')[-1], open(file_path, 'rb')),
    }
    response = requests.post(url, headers=headers, files=files,verify=False)
    files['file'][1].close()
    return response

file_path = '..//素材//whitelist.csv'
response = import_whitelist(file_path)
print(response.json())


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[24]:


import datetime
import pytz  # 需要安装 pytz 模块

# 获取当前时间
now = datetime.datetime.now()

# 获取当前时区
current_timezone = now.astimezone().tzinfo

print("当前时区:", current_timezone)


# In[ ]:





# In[ ]:





# In[ ]:




