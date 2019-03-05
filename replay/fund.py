# -*- coding:utf-8 -*-
import pandas as pd
from acc_subacc import AccountWriter
from writer import Writer

from replay.user_subacc import UserWriter


class FundWriter(Writer):
    def __init__(self, config, conn):
        super(FundWriter, self).__init__(config, conn)
        self.user_writer = UserWriter(config, conn)
        self.account_writer = AccountWriter(config, conn)

    def get_fund_dfs(self, fund_code, type):
        # fund->user
        if type == 1:
            id_df = self.get_users_by_fund_code(fund_code)
            for user_code in id_df['UserCode']:
                self.user_writer.get_user_dfs(user_code)
                self.df_list.extend(self.user_writer.df_list)

        # fund->account type == 2
        else:
            id_df = self.get_accounts_by_fund_code(fund_code)
            for account_code in id_df['AccountCode']:
                self.account_writer.get_account_dfs(account_code)
                self.df_list.extend(self.account_writer.df_list)

        i = 0
        for dict in self.df_list:
            if dict['type'] == 'user' or dict['type'] == 'account':
                position_df = dict['dfs'][0].ix[:, ['TradingDay', 'InstrumentCode', 'LongPosition', 'ShortPosition', 'ProductCode']]
                settlement_df = dict['dfs'][1].iloc[:, :11]
                if i > 0:
                    position_df = position_df.append(position_df)
                    settlement_df = settlement_df.add(settlement_df, fill_value=0)
            i += 1

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
                share_list.append(share_list[i - 1] + row['Deposit&Withdraw'] / networth_list[i - 1])
                networth_list.append(row['Balance'] / share_list[i])
            i += 1
        settlement_df['Share'] = share_list
        settlement_df['NetWorth'] = networth_list

        position_df['NetPosition'] = position_df['LongPosition'] - position_df['ShortPosition']

        days_df = pd.to_datetime(position_df['TradingDay']).drop_duplicates().nlargest(2)
        if not days_df.empty:
            current_trading_day, last_trading_day = days_df.apply(lambda x: x.strftime('%Y%m%d'))
            position_gb = position_df.groupby(['TradingDay'])
            current_position_df = position_gb.get_group(current_trading_day)
            last_position_df = position_gb.get_group(last_trading_day)
            current_position_df.set_index('TradingDay', inplace=True)
            last_position_df.set_index('TradingDay', inplace=True)

            increase_position_df = pd.merge(current_position_df, last_position_df, how='outer', indicator=True).query(
                '_merge=="left_only"').drop(['_merge'], axis=1)
            decrease_position_df = pd.merge(current_position_df, last_position_df, how='outer', indicator=True).query(
                '_merge=="right_only"').drop(['_merge'], axis=1)

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

        t = (position_df, settlement_df, his_position_df, current_position_df, last_position_df, increase_position_df, decrease_position_df, days_df, fund_code)
        self.df_list.append({'type': 'fund', 'dfs': t})
