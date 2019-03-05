# coding=utf8
import pandas as pd


class Position(object):

    def __init__(self, today_position_df):
        self.today_position_df = today_position_df

    def get_position_df_on_instrument(self):
        if not self.today_position_df.empty:
            transaction_date = self.today_position_df['trade_time'].max().date()
            df = self.today_position_df[['strategy_id', 'instrument', 'direction', 'amount', 'price']].copy(deep=True)
            df['balance'] = df['amount'] * df['price']
            del df['price']
            df = df.groupby(['strategy_id', 'instrument', 'direction']).sum()
            df.reset_index(inplace=True)
            df['avg_price'] = df['balance'] / df['amount']
            del df['balance']
            df.rename(columns={'position': 'amount'}, inplace=True)
            df['transaction_date'] = transaction_date
            return df
        return pd.DataFrame()
