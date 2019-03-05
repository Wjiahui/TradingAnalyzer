# coding=utf8
import pandas as pd


class Writer(object):

    def __init__(self, _engine):
        self.engine = _engine

    def output(self, _df, _name):
        """
            持久化
        """
        _df.to_sql(_name, self.engine, if_exists='append', index=True)


class HiveQuantWriter(Writer):

    def __init__(self, _engine):
        super(HiveQuantWriter, self).__init__(_engine)

    def print_daily_summary(self, daily_summary_df):

        cols = ['strategy_id', 'round', 'profit_count', 'loss_count', 'winning_rate', 'profit', 'loss',
                'net_profit', 'transaction_date', 'balance']
        daily_summary_df = daily_summary_df.loc[:, cols]
        daily_summary_df.to_sql("daily_summary", self.engine, if_exists='append', index=False)

    def print_position(self, position_df):
        cols = ['strategy_id', 'instrument', 'direction', 'amount', 'avg_price', 'transaction_date']
        position_df = position_df.ix[:, cols]
        position_df.to_sql("position", self.engine, if_exists='append', index=False)

    def print_trade_pair(self, pair_df):
        cols = ['strategy_id', 'long_short', 'instrument', 'amount', 'open_time', 'close_time', 'open_price', 'close_price', 'net_profit']
        pair_df = pair_df.ix[:, cols]
        pair_df.to_sql("trade_pair", self.engine, if_exists='append', index=False)
