#!/usr/bin/env python
# coding: utf-8

# In[6]:

root_sc='/home/vscode/workspace/work/生产/素材'
import tushare as ts
import pandas as pd
tkpath=f'{root_sc}/tk.txt'
tk=pd.read_csv(tkpath).columns[0]
ts.set_token(tk)
pro = ts.pro_api()
for v in ['399905.SZ','399300.SZ','000852.SH','000985.SH','883957.TI']:
    dfbase = pro.index_daily(ts_code=v).sort_values(by='trade_date')
    dfbase.trade_date=pd.to_datetime(dfbase.trade_date.astype(str))
    dfbase=dfbase.sort_values(by='trade_date').set_index('trade_date')
    if len(dfbase)==0:
        import os
        DATA_PATH = '/home/vscode/workspace/data/store/rsync'
        tp=pd.read_parquet(os.path.join(DATA_PATH,'data_daily//close_ths.parquet'))[v].reset_index()
        dfbase.loc[:,['trade_date','close']]=tp.values
        dfbase.loc[:,'pct_chg']=dfbase.close.pct_change()*100
        dfbase.loc[:,'ts_code']=v
        dfbase.index=dfbase.trade_date
        del dfbase['trade_date']
    dfbase.to_parquet(f'{root_sc}//指数基准//%s.parquet'%v)


# In[ ]:




