# -*- coding:utf-8 -*-
import pandas as pd
from writer import Writer

from replay.subacc import SubAccountWriter


class UserWriter(Writer):

    def __init__(self, config, conn):
        super(UserWriter, self).__init__(config, conn)
        self.subaccount_writer = SubAccountWriter(config, conn)

    def get_user_dfs(self, user_code):
        subaccount_df = self.get_subaccounts_by_user_code(user_code)

        i = 0
        for subaccount_code in subaccount_df['SubAccCode']:
            self.subaccount_writer.get_subaccount_dfs(subaccount_code)
            if i == 0:
                position_df = self.subaccount_writer.df_list[0]['dfs'][0]
                settlement_df = self.subaccount_writer.df_list[0]['dfs'][1]
            else:
                df1 = self.subaccount_writer.df_list[0]['dfs'][0]
                df2 = self.subaccount_writer.df_list[0]['dfs'][1]
                position_df = position_df.append(df1)
                settlement_df = settlement_df.add(df2, fill_value=0)

            i += 1
        self.df_list = self.subaccount_writer.df_list

        settlement_df = settlement_df.iloc[:, :11]
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
            if i == 0:
                share_list.append(row['Deposit&Withdraw'])
                networth_list.append(row['Balance'] / row['Deposit&Withdraw'])
            else:
                share_list.append(share_list[i-1] + row['Deposit&Withdraw'] / networth_list[i-1])
                networth_list.append(row['Balance'] / share_list[i])
            i += 1
        settlement_df['Share'] = share_list
        settlement_df['NetWorth'] = networth_list

        #改变columns的顺序
        settlement_cols = list(settlement_df)
        settlement_cols.insert(5, settlement_cols.pop(settlement_cols.index('Deposit&Withdraw')))
        settlement_cols.insert(3, settlement_cols.pop(settlement_cols.index('NetProfit')))
        settlement_df = settlement_df.ix[:, settlement_cols]

        position_df['NetPosition'] = position_df['LongPosition'] - position_df['ShortPosition']

        days_df = pd.to_datetime(position_df['TradingDay']).drop_duplicates().nlargest(2)
        if not days_df.empty:
            current_trading_day, last_trading_day = days_df.apply(lambda x: x.strftime('%Y%m%d'))
            position_gb = position_df.groupby(['TradingDay'])

            current_position_df = position_gb.get_group(current_trading_day)
            last_position_df = position_gb.get_group(last_trading_day)
            current_position_df.set_index('TradingDay', inplace=True)
            last_position_df.set_index('TradingDay', inplace=True)

            increase_position_df = pd.merge(current_position_df, last_position_df, how='outer', indicator=True).query('_merge == "left only"').drop(['_merge'], axis=1)
            decrease_position_df = pd.merge(current_position_df, last_position_df, how='outer', indicator=True).query('_merge =="right only"').drop(['_merge'], axis=1)

            del current_position_df['NetPosition']
            del last_position_df['NetPosition']
            del increase_position_df['NetPosition']
            del decrease_position_df['NetPosition']

            current_position_df = current_position_df.groupby(['ProductCode', 'InstrumentCode']).sum()
            last_position_df = last_position_df.groupby(['ProductCode', 'InstrumentCode']).sum()

            increase_position_df = increase_position_df.groupby(['ProductCode', 'InstrumentCode']).sum()
            decrease_position_df = decrease_position_df.groupby(['ProductCode', 'InstrumentCode']).sum()

        his_position_df1 = position_df.groupby(['TradingDay']).sum()
        his_position_df1 = his_position_df1.reset_index()
        del his_position_df1['LongPosition']
        del his_position_df1['ShortPosition']
        del position_df['NetPosition']

        his_position_df = pd.merge(position_df, his_position_df1, on='TradingDay')
        his_position_df = his_position_df.groupby(['TradingDay', 'NetPosition', 'ProductCode', 'InstrumentCode']).sum()
        t = (position_df, settlement_df, his_position_df, current_position_df, last_position_df, increase_position_df, decrease_position_df, days_df, user_code)
        self.df_list.append({
            'type': 'user',
            'code': user_code,
            'dfs': t
        })
