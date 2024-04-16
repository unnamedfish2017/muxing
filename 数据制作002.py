import sys
sys.path.append('/home/jovyan/work/workspaces/daily report/实盘模型/模型文件/周频策略')
import pickle
from EIFBT import *
import pandas as pd
import os
import numpy as np
import warnings 
import datetime
warnings.filterwarnings("ignore")
path='/home/jovyan/data/store/rsync/data_daily/data_daily.pickle'
with open(path, 'rb') as f:
    data_daily = pickle.load(f)
for v in ['closew','openw','highw','loww','amtw']:
    exec(v+'=data_daily[\''+v+'\']')
dts=list(closew.index)

import datetime
import tushare as ts
import time
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
df_ = pro.query('trade_cal', start_date='20220101', end_date='20301201').sort_values(by='cal_date').reset_index(drop=True)
dts_open=list(df_[df_.is_open>0].cal_date)
df_=df_[(df_.is_open>0)&(df_.cal_date<=lst_day)]
lst_trd_day=str(df_.cal_date.values[-1])
nxt_trd_day=dts_open[dts_open.index(lst_trd_day)+1]

tp=closew
clms=[i for i in tp.columns if not i.endswith('.bj')]
tp=tp[clms]
tp=tp.tail(60).stack().reset_index().iloc[:,:2]
tp.columns=['日期','股票代码']
n_prd=5
y=closew.shift(-n_prd-1)/closew.shift(-1)-1
y.iloc[-1,:]=0
y=y.stack().rename('收益').reset_index().rename(columns={'date':'日期','code':'股票代码'})
y=y[(~y.股票代码.str.endswith('.bj'))&(y.日期==closew.index[-1])].reset_index(drop=True)
data_set=tp.merge(y,how='left')

DATA_PATH = '/home/jovyan/data/store/rsync/'
params={
'path':'/home/jovyan/data/store/rsync/data_daily/data_daily.pickle',
'path_':'/home/jovyan/work/commons/data/daily_data/stock_day_n.parquet',

'通联因子路径':f'{DATA_PATH}/factor_data/A股精品因子数据.parquet',
'日线常规因子路径':f'{DATA_PATH}/data_daily/日线常规指标.parquet',
'个股因子路径':f'{DATA_PATH}/factor_data/日线因子_p1/',
'大盘因子路径':f'{DATA_PATH}/factor_data/大盘因子/',
'风格因子路径':f'{DATA_PATH}/jq_factor/风格因子.parquet',
'通联风格因子路径':f'{DATA_PATH}/factor_data/通联申万21行业风险因子暴露表/tl_barra_factor.parquet',
'行业个股因子路径':f'{DATA_PATH}/factor_data/个股行业因子/',
'原始行情因子路径':f'{DATA_PATH}/data_daily/stock_day_n.parquet',
'高频因子路径':f'{DATA_PATH}/factor_data/高频因子_p2/',
'高频衍生因子路径':f'{DATA_PATH}/factor_data/高频衍生因子',
'小盘成长因子路径':f'{DATA_PATH}/factor_data/小盘成长因子/LG_factor.parquet',
'n_prd':5 
        }

import time
t1=time.time()
data_set_raw,因子列表=载入因子数据(data_set,params,closew=closew)
t2=time.time()
print('耗时:',t2-t1)

t=closew.tail(1).T.iloc[:,0]
stoplist=list(t[pd.isna(t)].index)
data_set_raw=data_set_raw[~data_set_raw.股票代码.isin(stoplist)]

data_set_raw=data_set_raw[data_set_raw.close>0]

data_set_002=data_set_raw.copy()
del data_set_raw

import pickle

# 从文件中读取字典
with open('all_factor.pickle', 'rb') as f:
    all_dict = pickle.load(f)
    
flipped_dict = {value: key for key, value in all_dict.items()}

lst1=[' ',
         '交通运输',
         '传媒',
         '公用事业',
         '农林牧渔',
         '医药生物',
         '商贸零售',
         '国防军工',
         '基础化工',
         '家用电器',
         '建筑材料',
         '建筑装饰',
         '房地产',
         '有色金属',
         '机械设备',
         '汽车',
         '煤炭',
         '环保',
         '电力设备',
         '电子',
         '石油石化',
         '社会服务',
         '纺织服饰',
         '综合',
         '美容护理',
         '计算机',
         '轻工制造',
         '通信',
         '钢铁',
         '银行',
         '非银金融',
         '食品饮料',
             '']
lst2=[' ',
 'IT服务Ⅱ',
 '一般零售',
 '专业工程',
 '专业服务',
 '专业连锁Ⅱ',
 '专用设备',
 '个护用品',
 '中药Ⅱ',
 '乘用车',
 '互联网电商',
 '休闲食品',
 '体育Ⅱ',
 '保险Ⅱ',
 '元件',
 '光伏设备',
 '光学光电子',
 '其他家电Ⅱ',
 '其他电子Ⅱ',
 '其他电源设备Ⅱ',
 '养殖业',
 '军工电子Ⅱ',
 '农业综合Ⅱ',
 '农产品加工',
 '农化制品',
 '农商行Ⅱ',
 '冶钢原料',
 '出版',
 '动物保健Ⅱ',
 '包装印刷',
 '化妆品',
 '化学制品',
 '化学制药',
 '化学原料',
 '化学纤维',
 '医疗器械',
 '医疗服务',
 '医疗美容',
 '医药商业',
 '半导体',
 '厨卫电器',
 '商用车',
 '国有大型银行Ⅱ',
 '地面兵装Ⅱ',
 '城商行Ⅱ',
 '基础建设',
 '塑料',
 '多元金融',
 '家居用品',
 '家电零部件Ⅱ',
 '小家电',
 '小金属',
 '工业金属',
 '工程咨询服务Ⅱ',
 '工程机械',
 '广告营销',
 '影视院线',
 '房地产开发',
 '房地产服务',
 '房屋建设Ⅱ',
 '摩托车及其他',
 '教育',
 '数字媒体',
 '文娱用品',
 '旅游及景区',
 '旅游零售Ⅱ',
 '普钢',
 '服装家纺',
 '林业Ⅱ',
 '橡胶',
 '水泥',
 '汽车服务',
 '汽车零部件',
 '油服工程',
 '油气开采Ⅱ',
 '消费电子',
 '渔业',
 '游戏Ⅱ',
 '炼化及贸易',
 '焦炭Ⅱ',
 '煤炭开采',
 '照明设备Ⅱ',
 '燃气Ⅱ',
 '物流',
 '特钢Ⅱ',
 '环保设备Ⅱ',
 '环境治理',
 '玻璃玻纤',
 '生物制品',
 '电力',
 '电子化学品Ⅱ',
 '电机Ⅱ',
 '电池',
 '电网设备',
 '电视广播Ⅱ',
 '白色家电',
 '白酒Ⅱ',
 '种植业',
 '纺织制造',
 '综合Ⅱ',
 '股份制银行Ⅱ',
 '能源金属',
 '自动化设备',
 '航天装备Ⅱ',
 '航海装备Ⅱ',
 '航空机场',
 '航空装备Ⅱ',
 '航运港口',
 '装修建材',
 '装修装饰Ⅱ',
 '计算机设备',
 '证券Ⅱ',
 '调味发酵品Ⅱ',
 '贵金属',
 '贸易Ⅱ',
 '轨交设备Ⅱ',
 '软件开发',
 '通信服务',
 '通信设备',
 '通用设备',
 '造纸',
 '酒店餐饮',
 '金属新材料',
 '铁路公路',
 '非白酒',
 '非金属材料Ⅱ',
 '风电设备',
 '食品加工',
 '饮料乳品',
 '饰品',
 '饲料',
 '黑色家电',
     '']

data_set_002.sort_values(by=['日期','股票代码'],inplace=True)
data_set_002.loc[:,因子列表['风格因子']]=data_set_002.groupby('股票代码')[因子列表['风格因子']].ffill(limit=1)

加中性因子=True
if 加中性因子:
    from sklearn.preprocessing import StandardScaler
    import statsmodels.api as sm
    def 中性化(tp,v,clms=['mvw']):
        scaler = StandardScaler()
        x_values = tp.loc[:,clms]
        y_values = pd.DataFrame(tp.loc[:,v])
        y = scaler.fit_transform(y_values)
        y[np.isnan(y)]=0
        x = scaler.fit_transform(x_values)
        x[np.isnan(x)]=0
        result = sm.OLS(y, x, hasconst=True).fit()
        return pd.Series(result.resid)
    one_hot_encoded = pd.get_dummies(data_set_002['hy_name'])  
    one_hot_encoded.columns=['hy_%s'%str(int(i)) for i in one_hot_encoded.columns]
    one_hot_clms=list(one_hot_encoded.columns)
    data_set_002=pd.concat([data_set_002,one_hot_encoded],axis=1)
    data_set_002.sort_values(by=['日期','股票代码'],inplace=True)
    中性因子=one_hot_clms+因子列表['风格因子']
    factor=[]
    for v in 因子列表:
        if v!='原始行情因子':
            factor+=因子列表[v]
    #todo_list=[i for i in factor if i not in 因子列表['大盘因子'] and i not in 因子列表['风格因子'] and i not in ['hy_name','lv2_hy_name']]
    todo_list=[i for i in factor if i not in 因子列表['大盘因子'] and i not in 因子列表['风格因子']]
    for v in todo_list:
        data_set_002[v]=data_set_002[v].replace(np.inf,np.nan)
        data_set_002.loc[:,v+'_neu']=data_set_002[['日期','股票代码',v]+中性因子].groupby('日期').apply(lambda x:中性化(x,v,中性因子)).values.flatten()    
        factor.append(v+'_neu')
        print(v,'finished!')

data_set_002['hy_name']=data_set_002['hy_name'].fillna(0).apply(lambda x:lst1[int(x)] if x<len(lst1) else lst1[-1])
data_set_002['lv2_hy_name']=data_set_002['lv2_hy_name'].fillna(0).apply(lambda x:lst2[int(x)] if x<len(lst2) else lst2[-1])
    
    
df_prevday_facts_raw=data_set_002[data_set_002.日期==data_set_002.日期.max()].rename(columns=flipped_dict)
df_prevday_facts_raw.loc[:,'datetime']=df_prevday_facts_raw.日期
df_prevday_facts_raw.loc[:,'instrument']=df_prevday_facts_raw.股票代码.apply(lambda x:x[-2:].upper()+'SE.S+'+x[:6])
df_prevday_facts_raw=df_prevday_facts_raw[['instrument','datetime']+[i for i in df_prevday_facts_raw.columns if '.' in i]]

df_60d_facts_raw=data_set_002.rename(columns=flipped_dict)
df_60d_facts_raw.loc[:,'datetime']=df_60d_facts_raw.日期
df_60d_facts_raw.loc[:,'instrument']=df_60d_facts_raw.股票代码.apply(lambda x:x[-2:].upper()+'SE.S+'+x[:6])
df_60d_facts_raw=df_60d_facts_raw[['instrument','datetime']+[i for i in df_60d_facts_raw.columns if '.' in i]]

stock_price_reinstatement_path_raw = '/home/jovyan/work/commons/data/daily_data/data_daily.pickle'

import datetime
import tushare as ts
import time
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
df_ = pro.query('trade_cal', start_date='20220101', end_date='20301201').sort_values(by='cal_date').reset_index(drop=True)
dts_open=list(df_[df_.is_open>0].cal_date)
df_=df_[(df_.is_open>0)&(df_.cal_date<=lst_day)]
lst_trd_day=str(df_.cal_date.values[-1])
nxt_trd_day=dts_open[dts_open.index(lst_trd_day)+1]
nxt_trd_day_=nxt_trd_day[:4]+'-'+nxt_trd_day[4:6]+'-'+nxt_trd_day[-2:]

basic_data_dict_raw={}
try:
    basic_data_dict_raw['INDEX_COMPONENT_AND_WEIGHT'] = pd.read_parquet('/home/jovyan/data/store/rsync/facts-stockpool/basic/%s/index_component_and_weight.parquet'%nxt_trd_day)
except:
    basic_data_dict_raw['INDEX_COMPONENT_AND_WEIGHT'] = pd.read_parquet('/home/jovyan/data/store/rsync/facts-stockpool/basic/%s/index_component_and_weight.parquet'%lst_trd_day)
import tushare as ts
tkpath='/home/jovyan/work/commons/data/daily_data/tk.txt'
DATA_PATH = '/home/jovyan/data/store/rsync/'
tk=pd.read_csv(tkpath).columns[0]
ts.set_token(tk)###最新20220607
pro = ts.pro_api()
stlist = pro.bak_basic(trade_date=closew.index[-1].strftime('%Y%m%d'), fields='trade_date,ts_code,name')
stlist['l1'] = stlist['name'].apply(lambda x:x[:2])
stlist['l2'] = stlist['name'].apply(lambda x:x[:3])
data = stlist[(stlist['l1']=='ST' )| (stlist['l2']=='*ST')|(stlist['l2']=='S*S')]
stlist=list(data.ts_code.str.lower())
stlist2=list(pd.read_csv(f'{DATA_PATH}/data_daily/STlist.csv').代码)
tp=pd.DataFrame(list(set(stlist+stlist2)))
tp.loc[:,'instrument']=tp[0].apply(lambda x:x[-2:].upper()+'SE.S+'+x[:6])
tp.loc[:,'tonghuashun.ST']=1
tp.loc[:,'tonghuashun.delisting']=1
df_prevday_facts_raw=df_prevday_facts_raw.merge(tp.iloc[:,1:],how='left')
df_prevday_facts_raw.loc[:,'tonghuashun.ST']=df_prevday_facts_raw.loc[:,'tonghuashun.ST'].fillna(0)
df_prevday_facts_raw.loc[:,'tonghuashun.delisting']=df_prevday_facts_raw.loc[:,'tonghuashun.delisting'].fillna(0)


root='/home/jovyan/work/workspaces/daily report/实盘模型/每日数据'
df_prevday_facts_raw.to_parquet(f'{root}/%s_prevday_daily_facts_full_002.parquet'%nxt_trd_day_)
df_60d_facts_raw.to_parquet(f'{root}/%s_60d_daily_facts_small_002.parquet'%nxt_trd_day_)