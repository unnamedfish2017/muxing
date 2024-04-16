#!/usr/bin/env python
# coding: utf-8

# In[55]:


# 导入产品黑名单
# curl -X POST -H 'Authorization: Bearer 0028ea6e-0345-40ff-98be-c2a2a33ac78f' -F "file=@whitelist.csv" http://localhost:8010/facts-stockpool/api/import_product_blacklist


# In[56]:


ret=[]
for v in ['GT_RW_DMA','GT_SY_DMA']:
    for c in 'SHSE.S+601211,SHSE.S+688677,SHSE.S+688479'.split(','):
        ret.append([v,c])
pd.DataFrame(ret).to_csv('..//素材//黑名单.csv',index=False,header=None)


# In[57]:


# -*- coding: utf-8 -*-
import requests
import urllib3

"""
批量导入标签
使用说明
修改url中的ip地址
修改token
"""

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
file_path = '..//素材//黑名单.csv'
headers = {'accept': '*/*', 'Authorization': 'Bearer 8c38d671-e3da-4bad-9445-a3ca2f45940f',
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}

url = "https://61.172.245.225:26829/facts-stockpool/api/import_product_blacklist"
files = {'file': (file_path.split('/')[-1], open(file_path, 'r'))}
response = requests.post(url, headers=headers,  files=files, verify=False)

print(response.text)


# In[58]:


##############################临时池


# In[59]:


funds_allocation_directory='/home/jovyan/work/workspaces/daily report/次日列表/指增资金分配/'
mls=os.listdir(funds_allocation_directory)
ml=max([i for i in mls if 'raw' not in i])
策略列表=pd.read_excel(os.path.join(funds_allocation_directory,ml)).sort_values(by='资金',ascending=False)
tp_add=pd.read_excel('..//素材//产品策略配置检查调整.xlsx')


# In[60]:


prod_nm=list(set(list(策略列表['账户.1'])+list(tp_add['产品'])))


# In[61]:


# black_list=[]
# tplist='002371,000568,600683,002271,002594,300274,300059,603986,002466,588200,600048,515120,513330,159915,159841,512200,159781,002142,002024'.split(',')
# for i in tplist:
#     if i.startswith('6'):
#         black_list.append('SHSE.S+'+i)
#     elif i.startswith(('0','3')):
#         black_list.append('SZSE.S+'+i)
#     elif i.startswith('5'):
#         black_list.append('SHSE.ETF+'+i)
#     elif i.startswith('1'):
#         black_list.append('SZSE.ETF+'+i)
# for v in prod_nm:
#     for c in black_list:
#         ret.append([v,c])
# pd.DataFrame(ret).to_csv('..//素材//黑名单.csv',index=False,header=None)


# In[62]:


# # -*- coding: utf-8 -*-
# import requests
# import urllib3

# """
# 批量导入标签
# 使用说明
# 修改url中的ip地址
# 修改token
# """

# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# file_path = '..//素材//黑名单.csv'
# headers = {'accept': '*/*', 'Authorization': 'Bearer 8c38d671-e3da-4bad-9445-a3ca2f45940f',
#            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}

# url = "https://61.172.245.225:26829/facts-stockpool/api/import_product_blacklist"
# files = {'file': (file_path.split('/')[-1], open(file_path, 'r'))}
# response = requests.post(url, headers=headers,  files=files, verify=False)

# print(response.text)


# In[ ]:





# In[ ]:




