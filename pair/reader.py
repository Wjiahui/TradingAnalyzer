# coding=utf8

import pandas as pd


class Reader(object):

    def __init__(self):
        pass


class SQLReader(Reader):

    def __init__(self, _engine):

        super(SQLReader, self).__init__()
        self.engine = _engine

        self.daily_summary_df = pd.read_sql_table('daily_summary', self.engine)
        if not self.daily_summary_df.empty:
            latest_date = self.daily_summary_df['transaction_date'].max() + pd.DateOffset(days=1)
            sql = "select * from trade where trade_time > '{}'".format(latest_date)
            self.latest_daily_summary_df = self.daily_summary_df.groupby('strategy_id').last()
        else:
            sql = "select * from trade"
            self.latest_daily_summary_df = pd.DataFrame()

        self.trade_df = pd.read_sql(sql, self.engine)

        if self.trade_df.empty:
            raise Exception("trade_df empty!")

        self.position_df = pd.read_sql_table('position', self.engine)
        self.strategy_init_balance_df = pd.read_sql_table('strategy', self.engine).loc[:, ['id', 'init_principal']]
        self.strategy_init_balance_df.rename(columns={'id': 'strategy_id', 'init_principal': 'balance'}, inplace=True)

    def get_trade_df(self):
        """
        获取trade的当日交易流水
        :return:  DataFrame
        """
        return self.trade_df

    def get_daily_summary_df(self):
        """
        获取daily summary的所有数据
        :return:  DataFrame
        """
        return self.daily_summary_df

    def get_position_df(self):
        """
        获取最新一个交易日的持仓数据position
        :return: DataFrame
        """
        if not self.position_df.empty:
            return self.position_df.groupby(['strategy_id', 'instrument']).last().reset_index()
        return pd.DataFrame()

    def get_latest_balance(self):
        """
        获取最新一个交易日的可用资金
        :return: DataFrame
        """
        if self.latest_daily_summary_df.empty:
            return self.strategy_init_balance_df
        else:
            self.latest_daily_summary_df = self.latest_daily_summary_df.loc[:, ['strategy_id', 'balance']].combine_first(self.strategy_init_balance_df)
            return self.latest_daily_summary_df


class CSVReader(Reader):

    def __init__(self, _engine):
        super(CSVReader, self).__init__()
        self.engine = _engine
