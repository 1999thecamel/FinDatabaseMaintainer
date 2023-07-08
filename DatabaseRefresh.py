import numpy as np
from datetime import datetime, timedelta
import time
current_time = time.strftime("%H:%M:%S", time.localtime())
current_date = datetime.today().strftime('%Y%m%d')
current_month = datetime.today().strftime('%Y%m')
current_year = datetime.today().strftime('%Y')

# -*- coding: utf-8 -*-
import time
import pandas as pd
from WCFAdox import PCAX

#設定連線主機IP並產生物件
PX=PCAX("")

from sqlalchemy import create_engine, text
engine = create_engine('mssql+pyodbc://?driver=SQL+Server+Native+Client+11.0')
connection = engine.connect()



if (current_time > '21:00:00') and (current_time < '24:00:00'):
    
    daily_price=pd.concat([PX.Mul_Data("日收盤表排行", "D", current_date, ps="<CM代號,1>"), PX.Mul_Data("日收盤表排行", "D", current_date, ps="<CM代號,2>")])
    daily_price = daily_price.sort_values(['日期', '股票代號'], ascending=[False, True])
    index_column = daily_price['日期'] + '-' + daily_price['股票代號']
    daily_price.insert(loc=0, column='index', value=index_column)
    daily_price.loc[0]=np.arange(len(daily_price.columns))
    daily_price.loc[0] = 'C'+(daily_price.loc[0]+1).astype(str)
    daily_price.columns = daily_price.iloc[0]
    daily_price = daily_price.iloc[1:, :]
    # daily_price['data_time'] = datetime.today().strftime('%Y-%m-%d')
    daily_price.to_sql('QUANTDATA_DAILY_PRICE', con=engine, index=False, if_exists='append')
    
    dividend_policy=pd.concat([PX.Mul_Data("股利政策表","Y",current_year, ps="<CM代號,1>"), PX.Mul_Data("股利政策表","Y",current_year, ps="<CM代號,2>")])
    dividend_policy = dividend_policy.sort_values(['年度', '股票代號'], ascending=[False, True])
    index_column = dividend_policy['年度'] + '-' + dividend_policy['股票代號']
    dividend_policy.insert(loc=0, column='index', value=index_column)
    dividend_policy.loc[0]=np.arange(len(dividend_policy.columns))
    dividend_policy.loc[0] = 'C'+(dividend_policy.loc[0]+1).astype(str)
    dividend_policy.columns = dividend_policy.iloc[0]
    dividend_policy = dividend_policy.iloc[1:, :]
    # dividend_policy['data_time'] = datetime.today().strftime('%Y-%m-%d')
    dividend_policy.to_sql('QUANTDATA_DIVIDEND_POLICY', con=engine, index=False, if_exists='append')
    
    indicators=pd.concat([PX.Mul_Data("日常用技術指標表", "D", current_date, ps="<CM代號,1>"), PX.Mul_Data("日常用技術指標表", "D", current_date, ps="<CM代號,2>")])
    indicators = indicators.sort_values(['日期', '股票代號'], ascending=[False, True])
    indicators['日期+股票代號'] = indicators['日期'] + indicators['股票代號']
    index_column = indicators['日期'] + '-' + indicators['股票代號']
    indicators.insert(loc=0, column='index', value=index_column)
    indicators.loc[0]=np.arange(len(indicators.columns))
    indicators.loc[0] = 'C'+(indicators.loc[0]+1).astype(str)
    indicators.columns = indicators.iloc[0]
    indicators = indicators.iloc[1:, :]
    # indicators['data_time'] = datetime.today().strftime('%Y-%m-%d')
    indicators.to_sql('QUANTDATA_INDICATORS', con=engine, index=False, if_exists='append')

    indicators_2=pd.concat([PX.Mul_Data("日常用技術指標表Ⅱ", "D", current_date, ps="<CM代號,1>"), PX.Mul_Data("日常用技術指標表Ⅱ", "D", current_date, ps="<CM代號,2>")])
    indicators_2 = indicators_2.sort_values(['日期', '股票代號'], ascending=[False, True])
    indicators_2['日期+股票代號'] = indicators_2['日期'] + indicators_2['股票代號']
    index_column = indicators_2['日期'] + '-' + indicators_2['股票代號']
    indicators_2.insert(loc=0, column='index', value=index_column)
    indicators_2.loc[0]=np.arange(len(indicators_2.columns))
    indicators_2.loc[0] = 'C'+(indicators_2.loc[0]+1).astype(str)
    indicators_2.columns = indicators_2.iloc[0]
    indicators_2 = indicators_2.iloc[1:, :]
    # indicators_2['data_time'] = datetime.today().strftime('%Y-%m-%d')
    indicators_2.to_sql('QUANTDATA_INDICATORS_2', con=engine, index=False, if_exists='append')

    quater_income_fin=PX.Mul_Data("季IFRS財報總表(金融)–損益單季","Q", current_month)
    quater_income_fin = quater_income_fin.sort_values(['年季', '代號'], ascending=[False, True])
    index_column = quater_income_fin['年季'] + '-' + quater_income_fin['代號']
    quater_income_fin.insert(loc=0, column='index', value=index_column)
    quater_income_fin.loc[0]=np.arange(len(quater_income_fin.columns))
    quater_income_fin.loc[0] = 'C'+(quater_income_fin.loc[0]+1).astype(str)
    quater_income_fin.columns = quater_income_fin.iloc[0]
    quater_income_fin = quater_income_fin.iloc[1:, :]
    # quater_income_fin['data_time'] = datetime.today().strftime('%Y-%m-%d')
    quater_income_fin.to_sql('QUANTDATA_QUATER_INCOME_FIN', con=engine, index=False, if_exists='append')

    quater_finratios_fin=PX.Mul_Data("季IFRS財報總表(金融)–財務比率","Q", current_month)
    quater_finratios_fin = quater_finratios_fin.sort_values(['年季', '代號'], ascending=[False, True])
    index_column = quater_finratios_fin['年季'] + '-' + quater_finratios_fin['代號']
    quater_finratios_fin.insert(loc=0, column='index', value=index_column)
    quater_finratios_fin.loc[0]=np.arange(len(quater_finratios_fin.columns))
    quater_finratios_fin.loc[0] = 'C'+(quater_finratios_fin.loc[0]+1).astype(str)
    quater_finratios_fin.columns = quater_finratios_fin.iloc[0]
    quater_finratios_fin = quater_finratios_fin.iloc[1:, :]
    # quater_finratios_fin['data_time'] = datetime.today().strftime('%Y-%m-%d')
    quater_finratios_fin.to_sql('QUANTDATA_QUATER_FINRATIOS_FIN', con=engine, index=False, if_exists='append')

    quater_finratios=PX.Mul_Data("季IFRS財報(財務比率)","Q", current_month)
    quater_finratios = quater_finratios.sort_values(['年季', '股票代號'], ascending=[False, True])
    index_column = quater_finratios['年季'] + '-' + quater_finratios['股票代號']
    quater_finratios.insert(loc=0, column='index', value=index_column)
    quater_finratios.loc[0]=np.arange(len(quater_finratios.columns))
    quater_finratios.loc[0] = 'C'+(quater_finratios.loc[0]+1).astype(str)
    quater_finratios.columns = quater_finratios.iloc[0]
    quater_finratios = quater_finratios.iloc[1:, :]
    # quater_finratios['data_time'] = datetime.today().strftime('%Y-%m-%d')
    quater_finratios.to_sql('QUANTDATA_QUATER_FINRATIOS', con=engine, index=False, if_exists='append')

    quater_income=PX.Mul_Data("季IFRS財報(損益單季)","Q", current_month)
    quater_income = quater_income.sort_values(['年季', '股票代號'], ascending=[False, True])
    index_column = quater_income['年季'] + '-' + quater_income['股票代號']
    quater_income.insert(loc=0, column='index', value=index_column)
    quater_income.loc[0]=np.arange(len(quater_income.columns))
    quater_income.loc[0] = 'C'+(quater_income.loc[0]+1).astype(str)
    quater_income.columns = quater_income.iloc[0]
    quater_income = quater_income.iloc[1:, :]
    # quater_income['data_time'] = datetime.today().strftime('%Y-%m-%d')
    quater_income.to_sql('QUANTDATA_QUATER_INCOME', con=engine, index=False, if_exists='append')

    three_parties_inandout=PX.Mul_Data("日三大法人速選", "D", current_date)
    three_parties_inandout = three_parties_inandout.sort_values(['日期', '股票代號'], ascending=[False, True])
    index_column = three_parties_inandout['日期'] + '-' + three_parties_inandout['股票代號']
    three_parties_inandout.insert(loc=0, column='index', value=index_column)
    three_parties_inandout.loc[0]=np.arange(len(three_parties_inandout.columns))
    three_parties_inandout.loc[0] = 'C'+(three_parties_inandout.loc[0]+1).astype(str)
    three_parties_inandout.columns = three_parties_inandout.iloc[0]
    three_parties_inandout = three_parties_inandout.iloc[1:, :]
    # three_parties_inandout['data_time'] = datetime.today().strftime('%Y-%m-%d')
    three_parties_inandout.to_sql('QUANTDATA_THREE_PARTIES_INANDOUT', con=engine, index=False, if_exists='append')

    weekly_shares_distribution=PX.Mul_Data("週集保戶股權分散表", "D", current_date)
    weekly_shares_distribution = weekly_shares_distribution.sort_values(['日期', '股票代號'], ascending=[False, True])
    index_column = weekly_shares_distribution['日期'] + '-' + weekly_shares_distribution['股票代號']
    weekly_shares_distribution.insert(loc=0, column='index', value=index_column)
    weekly_shares_distribution.loc[0]=np.arange(len(weekly_shares_distribution.columns))
    weekly_shares_distribution.loc[0] = 'C'+(weekly_shares_distribution.loc[0]+1).astype(str)
    weekly_shares_distribution.columns = weekly_shares_distribution.iloc[0]
    weekly_shares_distribution = weekly_shares_distribution.iloc[1:, :]
    # weekly_shares_distribution['data_time'] = datetime.today().strftime('%Y-%m-%d')
    weekly_shares_distribution.to_sql('QUANTDATA_WEEKLY_SHARES_DISTRIBUTION', con=engine, index=False, if_exists='append')

    weekly_shares_distributionstats=PX.Mul_Data("週集保戶股權分散統計表", "D", current_date)
    weekly_shares_distributionstats = weekly_shares_distributionstats.sort_values(['日期', '股票代號'], ascending=[False, True])
    index_column = weekly_shares_distributionstats['日期'] + '-' + weekly_shares_distributionstats['股票代號']
    weekly_shares_distributionstats.insert(loc=0, column='index', value=index_column)
    weekly_shares_distributionstats.loc[0]=np.arange(len(weekly_shares_distributionstats.columns))
    weekly_shares_distributionstats.loc[0] = 'C'+(weekly_shares_distributionstats.loc[0]+1).astype(str)
    weekly_shares_distributionstats.columns = weekly_shares_distributionstats.iloc[0]
    weekly_shares_distributionstats = weekly_shares_distributionstats.iloc[1:, :]
    # weekly_shares_distributionstats['data_time'] = datetime.today().strftime('%Y-%m-%d')
    weekly_shares_distributionstats.to_sql('QUANTDATA_WEEKLY_SHARES_DISTRIBUTIONSTATS', con=engine, index=False, if_exists='append')
    
    ETF_fundamentals=PX.Mul_Data("ETF基本資料表","Y", current_year)
    ETF_fundamentals = ETF_fundamentals.sort_values(['年度', '股票代號'], ascending=[False, True])
    index_column = ETF_fundamentals['年度'] + '-' + ETF_fundamentals['股票代號']
    ETF_fundamentals.insert(loc=0, column='index', value=index_column)
    ETF_fundamentals.loc[0]=np.arange(len(ETF_fundamentals.columns))
    ETF_fundamentals.loc[0] = 'C'+(ETF_fundamentals.loc[0]+1).astype(str)
    ETF_fundamentals.columns = ETF_fundamentals.iloc[0]
    ETF_fundamentals = ETF_fundamentals.iloc[1:, :]
    # ETF_fundamentals['data_time'] = datetime.today().strftime('%Y-%m-%d')
    ETF_fundamentals.to_sql('QUANTDATA_ETF_FUNDAMENTALS', con=engine, index=False, if_exists='append')

    ETF_payout=PX.Mul_Data("ETF收益分配明細表","Y", current_year)
    index_column = ETF_payout['年度'] + '-' + ETF_payout['股票代號']
    ETF_payout.insert(loc=0, column='index', value=index_column)
    ETF_payout.loc[0]=np.arange(len(ETF_payout.columns))
    ETF_payout.loc[0] = 'C'+(ETF_payout.loc[0]+1).astype(str)
    ETF_payout.columns = ETF_payout.iloc[0]
    ETF_payout = ETF_payout.iloc[1:, :]
    # ETF_payout['data_time'] = datetime.today().strftime('%Y-%m-%d')
    ETF_payout.to_sql('QUANTDATA_ETF_PAYOUT', con=engine, index=False, if_exists='append')
    
    
elif (current_time > '13:00:00') and (current_time < '16:00:00'):

    US_stock_fundamentals=PX.Mul_Data("美股上市公司基本資料","Y", current_year)
    US_stock_fundamentals = US_stock_fundamentals.sort_values(['年度', '代號'], ascending=[False, True])
    index_column = US_stock_fundamentals['年度'] + '-' + US_stock_fundamentals['代號']
    US_stock_fundamentals.insert(loc=0, column='index', value=index_column)
    US_stock_fundamentals.loc[0]=np.arange(len(US_stock_fundamentals.columns))
    US_stock_fundamentals.loc[0] = 'C'+(US_stock_fundamentals.loc[0]+1).astype(str)
    US_stock_fundamentals.columns = US_stock_fundamentals.iloc[0]
    US_stock_fundamentals = US_stock_fundamentals.iloc[1:, :]
    # US_stock_fundamentals['data_time'] = datetime.today().strftime('%Y-%m-%d')
    US_stock_fundamentals.to_sql('QUANTDATA_US_STOCK_FUNDAMENTALS', con=engine, index=False, if_exists='append')

    US_day_prices=PX.Mul_Data("美股日收盤還原表排行","D", datetime.strftime(datetime.strptime(current_date, '%Y%m%d') - timedelta(days=1), '%Y%m%d'))
    US_day_prices = US_day_prices.sort_values(['日期', '代號'], ascending=[False, True])
    index_column = US_day_prices['日期'] + '-' + US_day_prices['代號']
    US_day_prices.insert(loc=0, column='index', value=index_column)
    US_day_prices.loc[0]=np.arange(len(US_day_prices.columns))
    US_day_prices.loc[0] = 'C'+(US_day_prices.loc[0]+1).astype(str)
    US_day_prices.columns = US_day_prices.iloc[0]
    US_day_prices = US_day_prices.iloc[1:, :]
    # US_day_prices['data_time'] = datetime.today().strftime('%Y-%m-%d')
    US_day_prices.to_sql('QUANTDATA_US_DAY_PRICES', con=engine, index=False, if_exists='append')
    
    # US_day_prices=PX.Mul_Data("美股日收盤還原表排行","D", '20230630')
    # US_day_prices = US_day_prices.sort_values(['日期', '代號'], ascending=[False, True])
    # index_column = US_day_prices['日期'] + '-' + US_day_prices['代號']
    # US_day_prices.insert(loc=0, column='index', value=index_column)
    # US_day_prices.loc[0]=np.arange(len(US_day_prices.columns))
    # US_day_prices.loc[0] = 'C'+(US_day_prices.loc[0]+1).astype(str)
    # US_day_prices.columns = US_day_prices.iloc[0]
    # US_day_prices = US_day_prices.iloc[1:, :]
    # # US_day_prices['data_time'] = datetime.today().strftime('%Y-%m-%d')
    # US_day_prices.to_sql('QUANTDATA_US_DAY_PRICES', con=engine, index=False, if_exists='append')

    US_indicators=PX.Mul_Data("美股日常用技術指標表","D", datetime.strftime(datetime.strptime(current_date, '%Y%m%d') - timedelta(days=1), '%Y%m%d'))
    US_indicators = US_indicators.sort_values(['日期', '代號'], ascending=[False, True])
    index_column = US_indicators['日期'] + '-' + US_indicators['代號']
    US_indicators.insert(loc=0, column='index', value=index_column)
    US_indicators.loc[0]=np.arange(len(US_indicators.columns))
    US_indicators.loc[0] = 'C'+(US_indicators.loc[0]+1).astype(str)
    US_indicators.columns = US_indicators.iloc[0]
    US_indicators = US_indicators.iloc[1:, :]
    # US_indicators['data_time'] = datetime.today().strftime('%Y-%m-%d')
    US_indicators.to_sql('QUANTDATA_US_INDICATORS', con=engine, index=False, if_exists='append')

    US_indicators2=PX.Mul_Data("美股日常用技術指標表II","D", datetime.strftime(datetime.strptime(current_date, '%Y%m%d') - timedelta(days=1), '%Y%m%d'))
    US_indicators2 = US_indicators2.sort_values(['日期', '代號'], ascending=[False, True])
    index_column = US_indicators2['日期'] + '-' + US_indicators2['代號']
    US_indicators2.insert(loc=0, column='index', value=index_column)
    US_indicators2.loc[0]=np.arange(len(US_indicators2.columns))
    US_indicators2.loc[0] = 'C'+(US_indicators2.loc[0]+1).astype(str)
    US_indicators2.columns = US_indicators2.iloc[0]
    US_indicators2 = US_indicators2.iloc[1:, :]
    # US_indicators2['data_time'] = datetime.today().strftime('%Y-%m-%d')
    US_indicators2.to_sql('QUANTDATA_US_INDICATORS2', con=engine, index=False, if_exists='append')

    US_dividend_policy=PX.Mul_Data("美股股利政策表","Y", current_year)
    index_column = US_dividend_policy['年度'] + '-' + US_dividend_policy['代號']
    US_dividend_policy.insert(loc=0, column='index', value=index_column)
    US_dividend_policy.loc[0]=np.arange(len(US_dividend_policy.columns))
    US_dividend_policy.loc[0] = 'C'+(US_dividend_policy.loc[0]+1).astype(str)
    US_dividend_policy.columns = US_dividend_policy.iloc[0]
    US_dividend_policy = US_dividend_policy.iloc[1:, :]
    # US_dividend_policy['data_time'] = datetime.today().strftime('%Y-%m-%d')
    US_dividend_policy.to_sql('QUANTDATA_US_DIVIDEND_POLICY', con=engine, index=False, if_exists='append')

    US_quater_income=PX.Mul_Data("美股季財報(損益單季)","Q", current_month)
    US_quater_income = US_quater_income.sort_values(['年季', '代號'], ascending=[False, True])
    index_column = US_quater_income['年季'] + '-' + US_quater_income['代號']
    US_quater_income.insert(loc=0, column='index', value=index_column)
    US_quater_income.loc[0]=np.arange(len(US_quater_income.columns))
    US_quater_income.loc[0] = 'C'+(US_quater_income.loc[0]+1).astype(str)
    US_quater_income.columns = US_quater_income.iloc[0]
    US_quater_income = US_quater_income.iloc[1:, :]
    # US_quater_income['data_time'] = datetime.today().strftime('%Y-%m-%d')
    US_quater_income.to_sql('QUANTDATA_US_QUATER_INCOME', con=engine, index=False, if_exists='append')

    US_quater_finratios=PX.Mul_Data("美股季財報(財務比率)","Q", current_month)
    US_quater_finratios = US_quater_finratios.sort_values(['年季', '代號'], ascending=[False, True])
    index_column = US_quater_finratios['年季'] + '-' + US_quater_finratios['代號']
    US_quater_finratios.insert(loc=0, column='index', value=index_column)
    US_quater_finratios.loc[0]=np.arange(len(US_quater_finratios.columns))
    US_quater_finratios.loc[0] = 'C'+(US_quater_finratios.loc[0]+1).astype(str)
    US_quater_finratios.columns = US_quater_finratios.iloc[0]
    US_quater_finratios = US_quater_finratios.iloc[1:, :]
    # US_quater_finratios['data_time'] = datetime.today().strftime('%Y-%m-%d')
    US_quater_finratios.to_sql('QUANTDATA_US_QUATER_FINRATIOS', con=engine, index=False, if_exists='append')

elif (current_time > '02:00:00') and (current_time < '04:00:00'):

    HK_stock_fundamentals=PX.Mul_Data("港股上市公司基本資料","Y", current_year)
    HK_stock_fundamentals = HK_stock_fundamentals.sort_values(['年度', '代號'], ascending=[False, True])
    index_column = HK_stock_fundamentals['年度'] + '-' + HK_stock_fundamentals['代號']
    HK_stock_fundamentals.insert(loc=0, column='index', value=index_column)
    HK_stock_fundamentals.loc[0]=np.arange(len(HK_stock_fundamentals.columns))
    HK_stock_fundamentals.loc[0] = 'C'+(HK_stock_fundamentals.loc[0]+1).astype(str)
    HK_stock_fundamentals.columns = HK_stock_fundamentals.iloc[0]
    HK_stock_fundamentals = HK_stock_fundamentals.iloc[1:, :]
    # HK_stock_fundamentals['data_time'] = datetime.today().strftime('%Y-%m-%d')
    HK_stock_fundamentals.to_sql('QUANTDATA_HK_STOCK_FUNDAMENTALS', con=engine, index=False, if_exists='append')


    HK_day_prices=PX.Mul_Data("港股日收盤表排行","D", datetime.strftime(datetime.strptime(current_date, '%Y%m%d') - timedelta(days=1), '%Y%m%d'))
    HK_day_prices = HK_day_prices.sort_values(['日期', '代號'], ascending=[False, True])
    index_column = HK_day_prices['日期'] + '-' + HK_day_prices['代號']
    HK_day_prices.insert(loc=0, column='index', value=index_column)
    HK_day_prices.loc[0]=np.arange(len(HK_day_prices.columns))
    HK_day_prices.loc[0] = 'C'+(HK_day_prices.loc[0]+1).astype(str)
    HK_day_prices.columns = HK_day_prices.iloc[0]
    HK_day_prices = HK_day_prices.iloc[1:, :]
    # HK_day_prices['data_time'] = datetime.today().strftime('%Y-%m-%d')
    HK_day_prices.to_sql('QUANTDATA_HK_DAY_PRICES', con=engine, index=False, if_exists='append')

    HK_cashdividend_policy=PX.Mul_Data("港股股利政策表(派現)","Y", current_year)
    index_column = HK_cashdividend_policy['年度'] + '-' + HK_cashdividend_policy['代號']
    HK_cashdividend_policy.insert(loc=0, column='index', value=index_column)
    HK_cashdividend_policy.loc[0]=np.arange(len(HK_cashdividend_policy.columns))
    HK_cashdividend_policy.loc[0] = 'C'+(HK_cashdividend_policy.loc[0]+1).astype(str)
    HK_cashdividend_policy.columns = HK_cashdividend_policy.iloc[0]
    HK_cashdividend_policy = HK_cashdividend_policy.iloc[1:, :]
    # HK_cashdividend_policy['data_time'] = datetime.today().strftime('%Y-%m-%d')
    HK_cashdividend_policy.to_sql('QUANTDATA_HK_CASHDIVIDEND_POLICY', con=engine, index=False, if_exists='append')


    HK_indicators=PX.Mul_Data("港股常用技術指標","D", datetime.strftime(datetime.strptime(current_date, '%Y%m%d') - timedelta(days=1), '%Y%m%d'))
    HK_indicators = HK_indicators.sort_values(['日期', '代號'], ascending=[False, True])
    index_column = HK_indicators['日期'] + '-' + HK_indicators['代號']
    HK_indicators.insert(loc=0, column='index', value=index_column)
    HK_indicators.loc[0]=np.arange(len(HK_indicators.columns))
    HK_indicators.loc[0] = 'C'+(HK_indicators.loc[0]+1).astype(str)
    HK_indicators.columns = HK_indicators.iloc[0]
    HK_indicators = HK_indicators.iloc[1:, :]
    # HK_indicators['data_time'] = datetime.today().strftime('%Y-%m-%d')
    HK_indicators.to_sql('QUANTDATA_HK_INDICATORS', con=engine, index=False, if_exists='append')

    HK_indicators2=PX.Mul_Data("港股常用技術指標Ⅱ","D", datetime.strftime(datetime.strptime(current_date, '%Y%m%d') - timedelta(days=1), '%Y%m%d'))
    HK_indicators2 = HK_indicators2.sort_values(['日期', '代號'], ascending=[False, True])
    index_column = HK_indicators2['日期'] + '-' + HK_indicators2['代號']
    HK_indicators2.insert(loc=0, column='index', value=index_column)
    HK_indicators2.loc[0]=np.arange(len(HK_indicators2.columns))
    HK_indicators2.loc[0] = 'C'+(HK_indicators2.loc[0]+1).astype(str)
    HK_indicators2.columns = HK_indicators2.iloc[0]
    HK_indicators2 = HK_indicators2.iloc[1:, :]
    # HK_indicators2['data_time'] = datetime.today().strftime('%Y-%m-%d')
    HK_indicators2.to_sql('QUANTDATA_HK_INDICATORS2', con=engine, index=False, if_exists='append')


    HK_quater_finreport=PX.Mul_Data("港股IFRS年財報(總表)","Y", current_year)
    HK_quater_finreport = HK_quater_finreport.sort_values(['年度', '代號'], ascending=[False, True])
    index_column = HK_quater_finreport['年度'] + '-' + HK_quater_finreport['代號']
    HK_quater_finreport.insert(loc=0, column='index', value=index_column)
    HK_quater_finreport.loc[0]=np.arange(len(HK_quater_finreport.columns))
    HK_quater_finreport.loc[0] = 'C'+(HK_quater_finreport.loc[0]+1).astype(str)
    HK_quater_finreport.columns = HK_quater_finreport.iloc[0]
    HK_quater_finreport = HK_quater_finreport.iloc[1:, :]
    # HK_quater_finreport['data_time'] = datetime.today().strftime('%Y-%m-%d')
    HK_quater_finreport.to_sql('QUANTDATA_HK_QUATER_FINREPORT', con=engine, index=False, if_exists='append')
else:
    pass

table_list = ['QUANTDATA_DAILY_PRICE', 'QUANTDATA_DIVIDEND_POLICY', 'QUANTDATA_INDICATORS', 'QUANTDATA_INDICATORS_2', 'QUANTDATA_QUATER_INCOME_FIN', 'QUANTDATA_QUATER_FINRATIOS_FIN', 'QUANTDATA_QUATER_FINRATIOS', 'QUANTDATA_QUATER_INCOME', 'QUANTDATA_THREE_PARTIES_INANDOUT', 'QUANTDATA_WEEKLY_SHARES_DISTRIBUTION', 'QUANTDATA_WEEKLY_SHARES_DISTRIBUTIONSTATS', 'QUANTDATA_ETF_FUNDAMENTALS', 'QUANTDATA_ETF_PAYOUT', 'QUANTDATA_US_STOCK_FUNDAMENTALS', 'QUANTDATA_US_DAY_PRICES', 'QUANTDATA_US_INDICATORS', 'QUANTDATA_US_INDICATORS2', 'QUANTDATA_US_DIVIDEND_POLICY', 'QUANTDATA_US_QUATER_INCOME', 'QUANTDATA_US_QUATER_FINRATIOS', 'QUANTDATA_HK_STOCK_FUNDAMENTALS', 'QUANTDATA_HK_DAY_PRICES', 'QUANTDATA_HK_CASHDIVIDEND_POLICY','QUANTDATA_HK_INDICATORS', 'QUANTDATA_HK_INDICATORS2', 'QUANTDATA_HK_QUATER_FINREPORT']

import pymssql
conn = pymssql.connect(server='', user='', password='', database='')
cursor = conn.cursor()
for table in table_list: 
    # cursor.execute("SELECT DISTINCT ON C1 * FROM QUANTDATA_US_STOCK_FUNDAMENTALS")
    query = f"WITH cte AS (SELECT *, row_number() OVER (PARTITION BY C1 ORDER BY C1) AS rn FROM {table})SELECT * FROM  cte WHERE  rn = 1;"
    cursor.execute(query)
    distinct_data = cursor.fetchall()
    distinct_data = pd.DataFrame(distinct_data)
    if not distinct_data.empty:
        distinct_data.loc[0]=np.arange(len(distinct_data.columns))
        distinct_data.loc[0] = 'C'+(distinct_data.loc[0]+1).astype(str)
        distinct_data.columns = distinct_data.iloc[0]
        distinct_data = distinct_data.iloc[1:, :]
        distinct_data = distinct_data.iloc[:, :-1]
        distinct_data.to_sql(table, con=engine, index=False, if_exists='replace')
    
    
