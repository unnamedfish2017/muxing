#!/usr/bin/env python
# coding: utf-8

# In[31]:


ret=[]
分析名称='指增参数归因测试'
root='../产出/%s'%(分析名称)
import os
mls=os.listdir(root)
for ml in mls[-5:]:
    tp=pd.read_excel(os.path.join(root,ml))
    ret.append(tp)
tp=pd.concat(ret)
tp=tp[tp.A属性.str.contains('7')].reset_index(drop=True)
tp.loc[:,'B属性']=tp.A属性.str.cat(tp.B属性,'_').apply(lambda x:x.replace('7收益_','7_'))
tp.loc[:,'A属性']=''
分析名称='指增归因趋势'
tp.loc[:,'分析名称'] = 分析名称
path = '../产出/%s/%s.xlsx'%(分析名称,choose_date)
tp[['日期','A属性','B属性','分析名称','取值']].to_excel(path,index=False)
# 上传
import requests
url = "https://61.172.245.225:26829/profit-mgt/api/v1/posthours-analysis/import"
payload = {}
files = [('file', (path.split('/')[-1], open(path, 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))]
headers = {
    'Authorization': 'Bearer f28d8c4a-5965-4f11-a0c4-09ca35eed126',
    "User-Agent": "curl/7.78.0"
           }
response = requests.request("POST", url, headers=headers, data=payload, files=files, verify=False)
print(response.text)


# In[ ]:




