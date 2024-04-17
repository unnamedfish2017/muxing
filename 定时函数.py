import json
import requests
import pandas as pd
import os
from requests_toolbelt import MultipartEncoder
# 你复制的webhook地址
def feishu_message(内容):
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/78d4cfc5-0dd3-46f7-8a6a-5abb443c86cb"
    payload_message = {
     "msg_type": "text",
     "content": {
     "text": 内容
        }
    }
    headers = {
     'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload_message))

def feishu_message_muxing(内容):
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/fd58b3a6-5e4b-4fd8-bcb0-804eb0644b21"
    payload_message = {
     "msg_type": "text",
     "content": {
     "text": 内容
        }
    }
    headers = {
     'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload_message))
    
def feishu_message_strategy(内容):
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/1b18c319-3457-4e36-9f5b-cd718c3f4fd4"
    payload_message = {
     "msg_type": "text",
     "content": {
     "text": 内容
        }
    }
    headers = {
     'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload_message))
    
    
def 最新日期():
    import pickle,os
    import pandas as pd
    import numpy as np
    path='/home/jovyan/work/commons/data/daily_data/data_daily.pickle'
    with open(path, 'rb') as f:
        data_daily = pickle.load(f)
    closew=data_daily['closew']
    dts=list(pd.to_datetime(closew.index))
    return dts[-1]
def 检验行业最新():
    path_to='/home/jovyan/work/commons/data/industry_data/行业因子.parquet'
    return pd.read_parquet(path_to).date.values[-1]==最新日期()


def 数据更新提醒():
    tp=[]
    import pickle,os
    import pandas as pd
    import numpy as np
    path='/home/jovyan/work/commons/data/daily_data/data_daily.pickle'
    with open(path, 'rb') as f:
        data_daily = pickle.load(f)
    closew=data_daily['closew']
    dts=list(pd.to_datetime(closew.index))

    for dt_ in dts[-5:]:
        dt=dt_.strftime('%Y%m%d')
        y=dt[:4]
        m=dt[4:6]
        d=dt[6:]
        path='/home/jovyan/data/store/stock/data_/second_3'+'/year='+str(y)+'/month='+str(m)+'/date='+str(d)
        if not os.path.exists(path):
            tp.append(dt)
    if len(tp):
        return 'QT '+','.join(tp)+' tick数据未更新！@刘融'
    else:
        return ''
    
def feishu_message_tongbu(内容):
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/c9aa3dcb-eebe-4ca8-952d-6b969a9a2166"
    payload_message = {
     "msg_type": "text",
     "content": {
     "text": 内容
        }
    }
    headers = {
     'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload_message))

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
    elif file_path.endswith('.png'):
        form = {'file_name': file_path,
            'file_type': 'image',
            'file': (file_path.split('/')[-1], open(file_path, 'rb'), 'image/png')}

    multi_form = MultipartEncoder(form)
    
    headers = {
        'Authorization': 'Bearer '+token, 
    }
    headers['Content-Type'] = multi_form.content_type
    response = requests.request("POST", url, headers=headers, data=multi_form)
    result = response.json()
    print(result)
    return result['data']['file_key'] 

def upload_img(token,img_path):
    resp = requests.post(
        url='https://open.feishu.cn/open-apis/image/v4/put/',
        headers={'Authorization': "Bearer " + token},
        files={
            "image": open(img_path, "rb")
        },
        data={
            "image_type": "message"
        },
        stream=True)
    resp.raise_for_status()
    content = resp.json()
    if content.get("code") == 0:
        return content['data']['image_key']
    else:
        return Exception("Call Api Error, errorCode is %s" % content["code"])

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

def share_image_to_group(file_key,token,group_id='oc_a6f7e857014839f1b3900bff9072ee97'):
    headers = {
        'Authorization': 'Bearer '+token,
        'Content-Type': 'application/json' 
    }
    msg_data = {
        "receive_id": group_id,
        "content": "{\"image_key\":\""+file_key+"\"}",
        "msg_type": "image"
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
    #oc_124e9ca53dc5ec7a344da5866d09cba4 木星策略群
    #oc_26a50158b2753dd2de73d08347beb369 交易记录运维

def 飞书发送_img(path,group_id):
    token = get_token()
    file_key = upload_img(token,path)
    share_image_to_group(file_key,token,group_id=group_id)
    #oc_a6f7e857014839f1b3900bff9072ee97 金牛策略讨论
    #oc_644c5d028de0e617ecfbad37dc1e7abe 弥远量化执行播报
    #oc_124e9ca53dc5ec7a344da5866d09cba4 木星策略群
    #oc_26a50158b2753dd2de73d08347beb369 交易记录运维
