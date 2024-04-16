#!/usr/bin/env python
# coding: utf-8

# In[60]:


import pandas as pd
df=pd.read_parquet('/home/jovyan/work/workspaces/daily report/实盘模型/pred_func/ret_nv_qa.parquet')


# In[71]:


y=(df['EALGA'].fillna(0)/100+1).cumprod()
st=0
h=y[0]
l=y[0]
hc=[]
d=0
for i in range(1,len(y)):
    if y[i]<= h:
        l=min(l,y[i])
        if l==y[i]:
            c=i-st
        d+=1
    else:
        if d>0:
            hc.append((y.index[st],y.index[i],d,c,(l/h-1)*100))
        h=y[i]
        l=y[i]
        d=0
        st=i
if d>0:
    hc.append((y.index[st],y.index[i],d,c,(l/h-1)*100))


# In[72]:


hc


# In[69]:


y.plot()


# In[70]:


y


# In[ ]:





# In[ ]:





# In[ ]:




