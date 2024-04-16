#!/usr/bin/env python
# coding: utf-8

# In[1]:


import warnings
warnings.filterwarnings("ignore")
import pickle,os
import pandas as pd
import numpy as np
path='/home/jovyan/data/store/rsync/data_daily/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
for v in ['closew','openw','highw','loww','amtw','WA_names_cn']:
    exec(v+'=data_daily[\''+v+'\']')
import pandas as pd
tp=pd.read_parquet('/home/jovyan/data/store/rsync/FinancialData/RebuyData.parquet')
tp=tp.drop_duplicates(['ts_code','ann_date']).rename(columns={'ts_code':'code'})
tp.code=tp.code.str.lower()

# tp.trade_date=tp.trade_date.astype(int)
# dfbase=pd.read_parquet('/home/jovyan/data/store/rsync/data_daily/日线常规指标.parquet')
# dfbase.trade_date=dfbase.trade_date.astype(int)
# tp=tp.merge(dfbase,how='left')


# tp.loc[:,'code']=tp.ts_code.str.lower()
# tp.loc[:,'date']=pd.to_datetime(tp.trade_date.astype(str))


# In[2]:


#tp=pd.read_parquet('/home/jovyan/data/store/rsync/FinancialData/RebuyData.parquet')
#tp.to_excel('..//素材//tushare回购数据.xlsx')


# In[3]:


dts=[i.strftime('%Y%m%d') for i in closew.index[-20:]]


# In[4]:


tp1=pd.read_csv('/home/jovyan/data/store/rsync/data_daily//申万行业股票列表2022_renew.csv',encoding='gbk')
#tp=tp[['code','申万一级','申万二级']].rename(columns={'code':'股票代码'})
tp=tp.merge(tp1,how='left')
dfraw=tp.copy()
tp=tp[tp.ann_date>=min(dts)].reset_index(drop=True)
import datetime
dt=closew.index[-1].strftime('%Y%m%d')
if datetime.datetime.now().weekday() in [5,6]:
     dt=datetime.datetime.now().strftime('%Y%m%d')
path='..//产出//股票回购//近20日股票回购_'+dt+'.xlsx'
flag=os.path.exists(path)
tp=tp.drop_duplicates(['code'],keep='last').reset_index(drop=True)

tp.loc[:,'现价']=closew.tail(1).T.loc[tp.code].values


# In[5]:


DATA_PATH = '/home/jovyan/data/store/rsync/'
t=pd.read_parquet(f'{DATA_PATH}/data_daily/日线常规指标.parquet')
t=t[t.trade_date==t.trade_date.max()].set_index('ts_code')['total_mv']/10000
t.index=t.index.str.lower()
tp=tp.merge(t.reset_index().rename(columns={'ts_code':'code'}),how='left')
tp=tp.to_excel(path,index=False)


# In[6]:


import os
import requests
from requests_toolbelt import MultipartEncoder

def get_token():
    url_token = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
    headers = {
        'Content-Type': 'application/json; charset=utf-8'
    }
    data = {
        "app_id": "cli_a4e6c1f375fb5013",
        "app_secret": "oWl7XNlZsFnVzZX0phikNgiHCcfBuU6i"
    }
    response = requests.post(url_token, headers=headers, json=data)
    result = response.json()
    print(result['tenant_access_token'])
    return result['tenant_access_token']


def upload_file(token,file_path):
    file_size = os.path.getsize(file_path)
    url = "https://open.feishu.cn/open-apis/im/v1/files"
    
    if file_path.endswith('.txt'):
        form = {'file_name': file_path,
                'file_type': 'stream',
                'file': (file_path.split('/')[-1], open(file_path, 'rb'), 'text/plain')}
    elif file_path.endswith('.xlsx'):
        form = {'file_name': file_path,
                'file_type': 'xls',
                'file': (file_path, open(file_path, 'rb'), 'application/json')}
    multi_form = MultipartEncoder(form)
    
    headers = {
        'Authorization': 'Bearer '+token, 
    }
    headers['Content-Type'] = multi_form.content_type
    response = requests.request("POST", url, headers=headers, data=multi_form)
    result = response.json()
    print(result)
    return result['data']['file_key'] 

def send_file(file_key,token,group_id='oc_a6f7e857014839f1b3900bff9072ee97'):
    headers = {
        'Authorization': 'Bearer '+token,
        'Content-Type': 'application/json' 
    }
    msg_data = {
        "receive_id": group_id,
        "content": "{\"file_key\":\""+file_key+"\"}",
        "msg_type": "file"
    }
    url_message = "https://open.feishu.cn/open-apis/im/v1/messages"
    url_request = url_message + '?receive_id_type=chat_id'
    response = requests.post(url_request, headers=headers, json=msg_data)

    status_code = response.status_code
    result = response.json()
    print(status_code, result)

def 飞书发送(path,group_id):
    token = get_token()
    file_key = upload_file(token,path)
    send_file(file_key,token,group_id=group_id)
    #oc_a6f7e857014839f1b3900bff9072ee97 金牛策略讨论
    #oc_644c5d028de0e617ecfbad37dc1e7abe 弥远量化执行播报
if not flag:
    飞书发送(path,'oc_a6f7e857014839f1b3900bff9072ee97')


# In[7]:


command = 'jupyter nbconvert --to script 回购数据.ipynb'
import os
os.system(command)


# In[8]:


# s2zfx96u


# In[9]:


#path='/home/jovyan/work/workspaces/daily report/每日报表/盘后分析/产出/股票回购/近20日股票回购_%s.xlsx'%lst_trd_day


# In[10]:


import requests

# 定义关键字和目标群组的Webhook URL
keyword = "关键字"
target_group_webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/d862df5a-5393-4552-bbb2-556205a81687"

def check_and_forward_message(message):
    if keyword in message:
        forward_message(message)

def forward_message(message):
    payload = {
        "msg_type": "text",
        "content": {
            "text": message
        }
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(target_group_webhook_url, json=payload, headers=headers)

# 监听原始群组的消息
# 请根据飞书的API或SDK来实现消息监听

# 在消息监听中，当收到消息时，调用check_and_forward_message函数来检查关键字并转发消息


# In[11]:


DATA_PATH = '/home/jovyan/data/store/rsync/'
dfraw0=pd.read_parquet(f'{DATA_PATH}/data_daily/stock_day_n.parquet')
#dfraw0=dfraw0[dfraw0.date>=20230101]
dfraw0.date=pd.to_datetime(dfraw0.date.astype(str))
dfraw0=dfraw0[['code','date','close']]
dfraw.loc[:,'date']=pd.to_datetime(dfraw.ann_date.astype(str))
tp=dfraw.merge(dfraw0,how='left')

tp.loc[:,'h_limit']=tp.high_limit/tp.close-1

ret=[]
df=tp[['code','date','proc','h_limit']]
for v in ['预案']:
    df_=df[df.proc==v]
    df_=df_.set_index(['code','date']).iloc[:,1:]
    df_.columns=[v+'_'+i for i in df_.columns]
    ret.append(df_)
tp=pd.concat(ret,axis=1).reset_index()
tp=tp[(~tp.code.str.endswith('.bj'))&(tp.预案_h_limit>0.1)]


# In[12]:


tp1=pd.read_csv('/home/jovyan/data/store/rsync/data_daily//申万行业股票列表2022_renew.csv',encoding='gbk')
#tp=tp[['code','申万一级','申万二级']].rename(columns={'code':'股票代码'})
dfraw1=tp.merge(tp1,how='left').rename(columns={'name':'标的名称','申万一级':'申万一级行业','申万二级':'申万二级行业'})


# In[13]:


def get_base(tkpath,index_code):
    import tushare as ts
    tk=pd.read_csv(tkpath).columns[0]
    ts.set_token(tk)
    pro = ts.pro_api()
    dfbase = pro.index_daily(ts_code=index_code).sort_values(by='trade_date')
    dfbase.trade_date=pd.to_datetime(dfbase.trade_date.astype(str))
    dfbase=dfbase.sort_values(by='trade_date').set_index('trade_date')
    ybase=(dfbase['open'].shift(-6)/dfbase['open'].shift(-1)-1).reset_index()
    ybase.columns=['日期','收益base']
    dfbase=dfbase.reset_index()
    return dfbase
tkpath='/home/jovyan/work/commons/data/daily_data/tk.txt'
base_index='000985.SH'
dfbase=get_base(tkpath,base_index).set_index('trade_date')[['open']]


# In[14]:


closew_ffill=closew.ffill()
openw[pd.isna(openw)]=closew_ffill.shift(1)[pd.isna(openw)]


# In[15]:


for i in [1,3,5,20]:
    tp=openw.shift(-i-1)/openw.shift(-1).ffill()-1
    tp=tp.stack().rename('%s日收益'%str(i)).reset_index()
    tp0=(dfbase.shift(-i-1)/dfbase.shift(-1)-1).reset_index()
    tp0.columns=['date','bench']
    tp=tp.merge(tp0,how='left')
    tp.loc[:,'%s日超额'%str(i)]=tp.loc[:,'%s日收益'%str(i)]-tp.bench
    del tp['bench']
    dfraw1=dfraw1.merge(tp,how='left')


# In[16]:


clms='信号名称,标的代码,信号日期,信号类型,标的名称,申万一级行业,申万二级行业,1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')


# In[17]:


dfraw1.loc[:,'信号名称']='回购预案'
dfraw1.loc[:,'信号类型']='事件驱动'
dfraw1=dfraw1.rename(columns={'date':'信号日期','code':'标的代码'})[clms]
#dfraw1=dfraw1[dfraw1.信号日期<closew.index[-1]]
dfraw1.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')]=np.round(dfraw1.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')]*100,1)


# In[18]:


#dfraw1.columns='标的代码,信号名称,信号日期,信号类型,标的名称,申万一级行业,申万二级行业,1天收益,3天收益,5天收益,20天收益,1天超额收益,3天超额收益,5天超额收益,20天超额收益'.split(',')

path='..//产出//信号看板//回购预案.csv'
dfraw1.to_csv(path,index=False,encoding='utf-8')


# In[19]:


dfraw1.loc[:,'1日收益,3日收益,5日收益,20日收益,1日超额,3日超额,5日超额,20日超额'.split(',')].mean()


# In[20]:


(dfraw1.groupby('信号日期')['5日超额'].count()).cumsum().plot()


# In[21]:


((dfraw1.groupby('信号日期')['5日超额'].mean())/500).cumsum().plot()


# In[22]:


import requests

url = "https://61.172.245.225:26829/signal-board/api/v1/public/import/raw"
headers = {
    'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
}
data = {
    'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
}
files = { 'uploadFile': (path.split('/')[-1], open(path, 'r')) }
response = requests.post(url, headers=headers, data=data, files=files,verify=False)
files['uploadFile'][1].close()
response


# In[23]:


# import requests  ##测试环境

# url = "https://192.168.31.66:26829/signal-board/api/v1/public/import/raw"
# headers = {
#     'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
# }
# data = {
#     'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
# }
# files = { 'uploadFile': (path.split('/')[-1], open(path, 'r')) }
# response = requests.post(url, headers=headers, data=data, files=files,verify=False)
# files['uploadFile'][1].close()
# response


# In[24]:


response


# In[ ]:





# In[ ]:




