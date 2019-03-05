# -*- coding:utf-8 -*-
import numpy as np
import pandas as pd

from replay.writer import Writer


class SubAccountWriter(Writer):
    """docstring for AccountWriter"""

    def __init__(self, config, conn):
        super(SubAccountWriter, self).__init__(config, conn)

    def get_subaccount_dfs(self, subacccode):
        instrument_df = self.get_instruments()

        settlement_sql_column = ','.join(self.config['table']['settlement']['raw'].keys())
        position_sql_column = ','.join(self.config['table']['position']['raw'].keys())

        settlement_sql = "select {} from SubAccountSettlement where SubAccountCode = '{}'".format(settlement_sql_column,subacccode)
        position_sql = "select {} from SubAccountPosition where SubAccountCode = '{}'".format(position_sql_column, subacccode)

        settlement_df = pd.read_sql(settlement_sql, self.conn, index_col='TradingDay')
        position_df = pd.read_sql(position_sql, self.conn)

        settlement_df['Deposit&Withdraw'] = settlement_df['Deposit'] - settlement_df['Withdraw']
        settlement_df['NetProfit'] = settlement_df['PositionProfit'] + settlement_df['CloseProfit'] - settlement_df['Commission']
        settlement_df['CumNetProfit'] = settlement_df['NetProfit'].cumsum()
        settlement_df['CumCommission'] = settlement_df['Commission'].cumsum()
        settlement_df['CumNetProfit/CumCommission'] = settlement_df['CumNetProfit'] / settlement_df['CumCommission']
        settlement_df['RiskLevel'] = 1 - settlement_df['Available'] / settlement_df['Balance']

        networth_list = []
        share_list = []

        i = 0
        for index, row in settlement_df.iterrows():
            """DataFrame.iterrows()返回的是(index, series) 其中series就是columns和values"""
            if i == 0:
                share_list.append(row['Deposit&Withdraw'])
                networth_list.append(row['Balance'] / row['Deposit&Withdraw'])
            else:
                share_list.append(share_list[i-1] + row['Deposit&Withdraw'] / networth_list[i-1])
                networth_list.append(row['Balance'] / share_list[i])
            i += 1

        settlement_df['Share'] = share_list
        settlement_df['NetWorth'] = networth_list

        """list(DataFrame)得到的是columns"""
        settlement_cols = list(settlement_df)
        settlement_cols.insert(5, settlement_cols.pop(settlement_cols.index('Deposit&Withdraw')))
        settlement_cols.insert(3, settlement_cols.pop(settlement_cols.index('NetProfit')))
        settlement_df = settlement_df.ix[:, settlement_cols]

        position_df['LongPosition'] = position_df['Position'] * position_df['PosiDirection'].apply(lambda x: 1 if int(x) == 2 else 0)
        position_df['ShortPosition'] = position_df['Position'] * position_df['PosiDirection'].apply(lambda x: 1 if int(x) == 3 else 0)
        position_df['NetPosition'] = position_df['LongPosition'] - position_df['ShortPosition']

        del position_df['PosiDirection']
        del position_df['Position']
        position_df.replace(0, np.nan)
        position_df = pd.merge(position_df, instrument_df, on='InstrumentCode')

        """drop_duplicates(subset,keep,inplace) subset为选定的列做distinct,默认为所有列 keep 值选项{'first','last',False} 保留
        重复元素的第一个，最后一个，或者全部删除"""
        days_df = pd.to_datetime(position_df['TradingDay']).drop_duplicates().nlargest(2)
        if not days_df.empty:
            #df.apply返回一个DataFrame
            current_trading_day, last_trading_day = days_df.apply(lambda x: x.strftime('%Y%m%d'))
            position_gb = position_df.groupby(['TradingDay'])
            current_position_df = position_gb.get_group(current_trading_day)
            last_position_df = position_gb.get_group(last_trading_day)
            current_position_df.set_index('TradingDay', inplace=True)
            last_position_df.set_index('TradingDay', inplace=True)

            """
            表明这行来自于哪个DataFrame
            indicator=True 增加一个column ：_merge 
            indicator=str 增加一个column: str 
            value:left right both
            """
            increase_position_df = pd.merge(current_position_df, last_position_df, how='outer', indicator=True).query('_merge == "left_only"').drop(['_merge'], axis=1)
            decrease_position_df = pd.merge(current_position_df, last_position_df, how='outer', indicator=True).query('_merge == "right_only"').drop(['_merge'], axis=1)

            del current_position_df['NetPosition']
            del last_position_df['NetPosition']
            del increase_position_df['NetPosition']
            del decrease_position_df['NetPosition']

            current_position_df = current_position_df.groupby(['ProductCode', 'InstrumentCode']).sum()
            last_position_df = last_position_df.groupby(['ProductCode', 'InstrumentCode']).sum()
            increase_position_df = increase_position_df.groupby(['ProductCode', 'InstrumentCode']).sum()
            decrease_position_df = decrease_position_df.groupby(['ProductCode', 'InstrumentCode']).sum()

        his_position_df = position_df.groupby(['TradingDay']).sum()
        his_position_df = his_position_df.reset_index()
        del his_position_df['LongPosition']
        del his_position_df['ShortPosition']
        del position_df['NetPosition']
        his_position_df = pd.merge(position_df, his_position_df, on='TradingDay')
        his_position_df = his_position_df.groupby(['TradingDay', 'NetPosition', 'ProductCode', 'InstrumentCode']).sum()
        t = (position_df, settlement_df, his_position_df, current_position_df, last_position_df, increase_position_df, decrease_position_df, days_df, subacccode)
        self.df_list.append({
            'type': 'subaccount',
            'code': subacccode,
            'dfs': t
        })

