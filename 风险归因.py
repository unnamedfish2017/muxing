# -*- coding: utf-8 -*-


import os
import pandas as pd
import numpy as np
import datetime, dateutil
from pytz import timezone


def attribution_analysis():

    def style_attribution(merge_df, date_list):
        barra_attr = []
        for trade_date in date_list:
            merge_day = merge_df[merge_df['date']==trade_date]
            pf_ret = np.sum(merge_day['weight_P']*merge_day['ret'])
            bm_ret = np.sum(merge_day['weight_B']*merge_day['ret'])
            # Barra归因
            dy1d_factor_ret_day = dy1d_factor_ret_df[dy1d_factor_ret_df['date']==trade_date]
            pf_ret_indu = []
            for indu_factor in CNE5_IndustryName:
                pf_ret_tmp = (np.sum(merge_day['weight_P']*merge_day[indu_factor])-np.sum(merge_day['weight_B']*merge_day[indu_factor]))*dy1d_factor_ret_day[indu_factor].iloc[0]
                pf_ret_indu.append(pf_ret_tmp)
            pf_ret_style = []
            for style_factor in CNE5_StyleName:
                pf_ret_tmp = (np.sum(merge_day['weight_P']*merge_day[style_factor])-np.sum(merge_day['weight_B']*merge_day[style_factor]))*dy1d_factor_ret_day[style_factor].iloc[0]
                pf_ret_style.append(pf_ret_tmp)
            pf_ret_country = np.sum((merge_day['weight_P']-merge_day['weight_B'])*dy1d_factor_ret_day['COUNTRY'].iloc[0])
            pf_ret_spec = pf_ret - bm_ret - pf_ret_country - np.nansum(pf_ret_indu) - np.nansum(pf_ret_style)
            barra_attr_day = pd.DataFrame([pf_ret, bm_ret, pf_ret-bm_ret, pf_ret_country, np.nansum(pf_ret_indu), np.nansum(pf_ret_style), pf_ret_spec]+pf_ret_style+pf_ret_indu, index=['组合收益', '基准收益', '超额收益', '市场收益', '行业收益', '风格收益', '特质收益']+CNE5_StyleName+CNE5_IndustryName).T
            barra_attr_day['date'] = trade_date
            barra_attr.append(barra_attr_day)

        barra_attr = pd.concat(barra_attr).set_index('date')

        # 计算累计收益
        cum_df = []
        for i in ['组合收益', '基准收益', '超额收益', '市场收益', '行业收益', '风格收益', '特质收益']+CNE5_StyleName+CNE5_IndustryName:
            cum_df.append((1+barra_attr[i]).cumprod()[-1]-1)
        cum_df = pd.DataFrame(cum_df, index=['组合收益', '基准收益', '超额收益', '市场收益', '行业收益', '风格收益', '特质收益']+CNE5_StyleName+CNE5_IndustryName).T
        cum_df['超额收益'] = cum_df['组合收益'] - cum_df['基准收益']
        cum_df['行业收益'] = np.sum(cum_df[CNE5_IndustryName].values)
        cum_df['风格收益'] = np.sum(cum_df[CNE5_StyleName].values)
        cum_df['特质收益'] = cum_df['组合收益'] - cum_df['基准收益'] - cum_df['市场收益'] - cum_df['行业收益'] - cum_df['风格收益']

        return barra_attr, cum_df
    
    # 加载风险模型数据
    def get_parquet(root):
        mls=os.listdir(root)
        ret=[]
        for ml in mls:
            if ml.endswith('.parquet'):
                tp=pd.read_parquet(os.path.join(root,ml))
                ret.append(tp)
        ret=pd.concat(ret)
        return ret

    CNE5_StyleName = ['BETA', 'MOMENTUM', 'SIZE', 'EARNYILD', 'RESVOL', 'GROWTH', 'BTOP', 'LEVERAGE', 'LIQUIDTY', 'SIZENL']
    CNE5_IndustryName_SW14 = ['Agriculture','Automobiles','Banks','BuildMater','Chemicals','Commerce','Computers','Conglomerates','ConstrDecor','Defense','ElectricalEquip','Electronics','FoodBeverages','HealthCare','HomeAppliances','Leisure','LightIndustry','MachineEquip','Media','Mining','NonbankFinan','NonferrousMetals','RealEstate','Steel','Telecoms','TextileGarment','Transportation','Utilities']
    CNE5_IndustryName_SW21 = ['Agriculture','Automobiles','Banks','BuildMater','Computers','Conglomerates','ConstrDecor','Defense','Electronics','FoodBeverages','HealthCare','HomeAppliances','LightIndustry','MachineEquip','Media','NonbankFinan','NonferrousMetals','RealEstate','Steel','Telecoms','Transportation','Utilities','BasicChemicals','BeautyCare','Coal','EnvironProtect','Petroleum','PowerEquip','RetailTrade','SocialServices','TextileApparel']
    CNE5_IndustryName = CNE5_IndustryName_SW21
    root="/home/jovyan/data/store/rsync/factor_data/通联试用数据"

    exposure_all_df=get_parquet(os.path.join(root,'RMExposureDaySW21'))
    exposure_all_df=exposure_all_df[exposure_all_df.exchangeCD.isin(['XSHE', 'XSHG'])]
    exposure_all_df['code']=exposure_all_df['secID'].apply(lambda x:x[:6]+'.sz' if x.endswith('XSHE') else x[:6]+'.sh')
    exposure_all_df['date']=pd.to_datetime(exposure_all_df['tradeDate'].astype(str))
    # exposure_all_df=exposure_all_df[['date','code']+因子列表].fillna(0)

    dy1d_specific_ret_all_df=get_parquet(os.path.join(root,'RMSpecificRetDaySW21'))
    dy1d_specific_ret_all_df=dy1d_specific_ret_all_df[dy1d_specific_ret_all_df.exchangeCD.isin(['XSHE', 'XSHG'])]
    dy1d_specific_ret_all_df['code']=dy1d_specific_ret_all_df['secID'].apply(lambda x:x[:6]+'.sz' if x.endswith('XSHE') else x[:6]+'.sh')
    dy1d_specific_ret_all_df['date']=pd.to_datetime(dy1d_specific_ret_all_df['tradeDate'].astype(str))
    # dy1d_specific_ret_all_df=dy1d_specific_ret_all_df[['date','code','spret']].fillna(0)

    dy1d_factor_ret_all_df=get_parquet(os.path.join(root,'RMFactorRetDaySW21'))
    dy1d_factor_ret_all_df['date']=pd.to_datetime(dy1d_factor_ret_all_df['tradeDate'].astype(str))
    # dy1d_factor_ret_all_df=dy1d_factor_ret_all_df[['date','factorName']+因子列表].fillna(0)

    
    # 选择日期
    def get_trade_calendar():
        DATA_PATH = "/home/jovyan/data/store/rsync"
        dts = pd.read_csv(f'{DATA_PATH}/data_daily/dts.csv')
        dts['date']=pd.to_datetime(dts['cal_date'].astype(str))
        return dts

    def get_tradeday_nbefore(date, Ndays):
        date = pd.to_datetime(date)
        dts = get_trade_calendar()
        trade_df = dts[dts['is_open']==1].sort_values('date')
        if Ndays == 0:
            tmp = trade_df.query('date<=@date')
            if len(tmp) == 0:
                return None
            else:
                return tmp['date'].values[(-1)]
        else:
            if Ndays > 0:
                tmp = trade_df.query('date<@date')
                if len(tmp) < Ndays:
                    return None
                else:
                    return tmp.tail(Ndays)['date'].values[0]
            else:
                tmp = trade_df.query('date>@date')
                if len(tmp) < Ndays:
                    return None
                else:
                    return tmp.head(-1 * Ndays)['date'].values[(-1)]
                
    start_date = '20240101'
    #end_date = get_tradeday_nbefore((datetime.datetime.now(timezone('Asia/Shanghai'))).strftime("%Y%m%d"), Ndays=1)
    #end_date = pd.to_datetime(end_date).strftime("%Y%m%d")
    
    data_daily = pd.read_pickle('/home/jovyan/data/store/rsync/data_daily/data_daily.pickle')
    #closew = data_daily["closew"].query("index>=@start_date").query("index<=@end_date")
    closew = data_daily["closew"].query("index>=@start_date")
    end_date=closew.index[-1].strftime("%Y%m%d")
    
    benchmark_dict = {'SZZZ':'000001.SH','QA':'000002.SH','SH50':'000016.SH','HS300':'000300.SH','KC50':'000688.SH','ZZ1000':'000852.SH','ZZ500':'000905.SH','ZZ800':'000906.SH','ZZQA':'000985.SH','CY':'399006.SZ','CYZ':'399102.SZ','SZZZ':'399106.SZ','GZ2000':'399303.SZ'}
    DATA_PATH = "/home/jovyan/data/store/rsync"
    benchmark_all_dict = {}
    for benchmark in ['HS300','ZZ500','ZZ1000']:
        base_index = benchmark_dict[benchmark]
        benchmark_all_df=pd.read_parquet(f'{DATA_PATH}/data_daily/指数成分/%s.parquet'%base_index).drop_duplicates()
        benchmark_all_df['code']=benchmark_all_df['code'].apply(lambda x:x.lower())
        benchmark_all_df['date']=pd.to_datetime(benchmark_all_df['date'].astype(str))
        benchmark_all_df['weight'] = benchmark_all_df['weight']/100
        
        # 填充所有交易日的投资域成分股
        dts = pd.read_csv(f'{DATA_PATH}/data_daily/dts.csv')
        dts = dts[dts['is_open']==1]
        dts['date']=pd.to_datetime(dts['cal_date'].astype(str))
        df_list = []
        for date in dts['date']:
            df_date = benchmark_all_df[benchmark_all_df['date'] == date]
            if len(df_date) == 0 and len(df_list) > 0:
                df_date = df_list[-1].copy()
            df_date['date'] = date
            df_list.append(df_date)
        benchmark_all_df = pd.concat(df_list).reset_index(drop=True)

        benchmark_all_dict[benchmark] = benchmark_all_df.query("date>=@start_date").query("date<=@end_date")

    data_daily = closew.pct_change().reset_index().melt(id_vars=['date'])
    data_daily.columns = ['date','code','ret']
    equd_df = data_daily.query("date>=@start_date").query("date<=@end_date")

    exposure_df = exposure_all_df.query("date>=@start_date").query("date<=@end_date")
    dy1d_specific_ret_df = dy1d_specific_ret_all_df.query("date>=@start_date").query("date<=@end_date")
    dy1d_factor_ret_df = dy1d_factor_ret_all_df.query("date>=@start_date").query("date<=@end_date")


    # 组合归因
    choose_date = end_date
#     root = '/home/vscode/workspace/work/ljgao/weight_data/'
    root = '/home/jovyan/data/store/rsync/opt_pools'
    # 指增收益归因分析
    tp = []
    for ml in os.listdir(root):
        if ml.startswith('E') & ml.endswith('0Pool_%s.csv'%choose_date):
            factor_name = ml[:-17]
            benchmark_df = benchmark_all_dict[{'1':'ZZ1000','3':'HS300','5':'ZZ500'}[ml[1]]]
            weight_df = pd.read_csv(os.path.join(root,ml),header=None)
            weight_df.columns = ['code','weight']
            weight_df['date']=pd.to_datetime(choose_date)
            weight_df['code'] = weight_df['code'].apply(lambda x: x[-6:]+'.'+x[:2].lower())

            # 优化后权重处理
            # weight_df = pd.read_csv('/home/vscode/workspace/work/ljgao/weight_df_merge.csv',index_col=0)
            # weight_df = weight_df[weight_df['date']==choose_date]
            # weight_df = weight_df.rename(columns={'abosulte_weight':'weight'})[['code','weight']]
            # weight_df['date']=pd.to_datetime(date)
            # weight_df = weight_df[weight_df['weight']>0.00001]


            date_list = sorted(list(set(weight_df['date'].unique())&set(benchmark_df['date'].unique())&set(equd_df['date'].unique())))
            # 合并数据
            merge_df = pd.merge(weight_df[weight_df['date'].isin(date_list)],benchmark_df[benchmark_df['date'].isin(date_list)],how='outer',on=['code','date'],suffixes=('_P','_B'))
            merge_df = merge_df.merge(equd_df,how='left',on=['code','date'])
            merge_df = merge_df.merge(exposure_df,how='left',on=['code','date']).merge(dy1d_specific_ret_df,how='left',on=['code','date'])
            merge_df['weight_P'] = merge_df['weight_P'].fillna(0)
            merge_df['weight_B'] = merge_df['weight_B'].fillna(0)

            barra_attr, cum_df = style_attribution(merge_df, date_list)

            # 风格归因

            print('当期归因：%s,%s'%(factor_name,choose_date))
            print(barra_attr.T[:7].round(4))

            factor_exposure = []
            factor_return = []
            for style_factor in CNE5_StyleName:
            # for style_factor in CNE5_IndustryName:
                exposure_style_A = merge_df.groupby('date').apply(lambda df: np.sum((df['weight_P']-df['weight_B'])*df[style_factor]))
                exposure_style_P = merge_df.groupby('date').apply(lambda df: np.sum(df['weight_P']*df[style_factor]))
                exposure_style_B = merge_df.groupby('date').apply(lambda df: np.sum(df['weight_B']*df[style_factor]))
            #     print(style_factor+':'+str([exposure_style_A,exposure_style_P,exposure_style_B]))
                factor_exposure.append(exposure_style_A[0])
                factor_return.append(barra_attr[style_factor][0])
            factor_exposure = pd.Series(factor_exposure, index=CNE5_StyleName) 
            factor_return = pd.Series(factor_return, index=CNE5_StyleName)
            print(pd.concat([factor_exposure.rename('factor_exposure'), dy1d_factor_ret_df[dy1d_factor_ret_df['date']==choose_date][CNE5_StyleName].T[0].rename('style_return'), factor_return.rename('factor_return')], axis=1).T.reset_index().round(4))

            # 输出格式
            factor_ret=(barra_attr * 100).T[:17].round(4).reset_index()
            factor_ret.columns = ['B属性','取值']
            factor_ret['A属性'] = factor_name+'收益'
            tp.append(factor_ret)
            factor_exp=(factor_exposure).round(4).reset_index()
            factor_exp.columns = ['B属性','取值']
            factor_exp['A属性'] = factor_name+'暴露'
            tp.append(factor_exp)

    tp = pd.concat(tp,axis=0)
    分析名称='指增收益归因分析'
    tp.loc[:,'分析名称'] = 分析名称
    tp.loc[:,'日期'] = pd.to_datetime(choose_date)
    tp.loc[:,'日期'] = tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
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
    
    # 指增参数归因测试
    tp = []
    for ml in os.listdir(root):
#         if ml.startswith('E') & (ml.endswith('3Pool_%s.csv'%choose_date)|ml.endswith('4Pool_%s.csv'%choose_date)|ml.endswith('5Pool_%s.csv'%choose_date)|ml.endswith('6Pool_%s.csv'%choose_date)|ml.endswith('7Pool_%s.csv'%choose_date)):
        if ml.endswith('Pool_%s.csv'%choose_date):
            factor_name = ml[:-17]
            benchmark_df = benchmark_all_dict[{'1':'ZZ1000','3':'HS300','5':'ZZ500'}[ml[1]]]
            weight_df = pd.read_csv(os.path.join(root,ml),header=None)
            weight_df.columns = ['code','weight']
            weight_df['date']=pd.to_datetime(choose_date)
            weight_df['code'] = weight_df['code'].apply(lambda x: x[-6:]+'.'+x[:2].lower())

            # 优化后权重处理
            # weight_df = pd.read_csv('/home/vscode/workspace/work/ljgao/weight_df_merge.csv',index_col=0)
            # weight_df = weight_df[weight_df['date']==choose_date]
            # weight_df = weight_df.rename(columns={'abosulte_weight':'weight'})[['code','weight']]
            # weight_df['date']=pd.to_datetime(date)
            # weight_df = weight_df[weight_df['weight']>0.00001]


            date_list = sorted(list(set(weight_df['date'].unique())&set(benchmark_df['date'].unique())&set(equd_df['date'].unique())))
            # 合并数据
            merge_df = pd.merge(weight_df[weight_df['date'].isin(date_list)],benchmark_df[benchmark_df['date'].isin(date_list)],how='outer',on=['code','date'],suffixes=('_P','_B'))
            merge_df = merge_df.merge(equd_df,how='left',on=['code','date'])
            merge_df = merge_df.merge(exposure_df,how='left',on=['code','date']).merge(dy1d_specific_ret_df,how='left',on=['code','date'])
            merge_df['weight_P'] = merge_df['weight_P'].fillna(0)
            merge_df['weight_B'] = merge_df['weight_B'].fillna(0)

            barra_attr, cum_df = style_attribution(merge_df, date_list)

            # 风格归因

            print('当期归因：%s,%s'%(factor_name,choose_date))
            print(barra_attr.T[:7].round(4))

            factor_exposure = []
            factor_return = []
            for style_factor in CNE5_StyleName:
            # for style_factor in CNE5_IndustryName:
                exposure_style_A = merge_df.groupby('date').apply(lambda df: np.sum((df['weight_P']-df['weight_B'])*df[style_factor]))
                exposure_style_P = merge_df.groupby('date').apply(lambda df: np.sum(df['weight_P']*df[style_factor]))
                exposure_style_B = merge_df.groupby('date').apply(lambda df: np.sum(df['weight_B']*df[style_factor]))
            #     print(style_factor+':'+str([exposure_style_A,exposure_style_P,exposure_style_B]))
                factor_exposure.append(exposure_style_A[0])
                factor_return.append(barra_attr[style_factor][0])
            factor_exposure = pd.Series(factor_exposure, index=CNE5_StyleName) 
            factor_return = pd.Series(factor_return, index=CNE5_StyleName)
            print(pd.concat([factor_exposure.rename('factor_exposure'), dy1d_factor_ret_df[dy1d_factor_ret_df['date']==choose_date][CNE5_StyleName].T[0].rename('style_return'), factor_return.rename('factor_return')], axis=1).T.reset_index().round(4))

            # 输出格式
            factor_ret=(barra_attr * 100).T[:17].round(4).reset_index()
            factor_ret.columns = ['B属性','取值']
            factor_ret['A属性'] = factor_name+'收益'
            tp.append(factor_ret)
            factor_exp=(factor_exposure).round(4).reset_index()
            factor_exp.columns = ['B属性','取值']
            factor_exp['A属性'] = factor_name+'暴露'
            tp.append(factor_exp)
            (barra_attr * 100).T.round(4).to_excel('../产出/%s/%s.xlsx'%('票池每日归因',ml.split('Pool')[0]+'_'+choose_date))

    tp = pd.concat(tp,axis=0)
    分析名称='指增参数归因测试'
    tp.loc[:,'分析名称'] = 分析名称
    tp.loc[:,'日期'] = pd.to_datetime(choose_date)
    tp.loc[:,'日期'] = tp.日期.apply(lambda x:x.strftime('%Y/%m/%d'))
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
    
    ret=[]
    分析名称='指增参数归因测试'
    root='../产出/%s'%(分析名称)
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
    

if __name__ == '__main__':
    attribution_analysis()
