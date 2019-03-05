# coding=utf8
from mock import Mock
from config import *
from db import DataBase
from reader import SQLReader
from dailyStatics import DailyStatics
from pair import Pair
from position import Position
from writer import HiveQuantWriter


class Builder(object):
    def __init__(self):
        self.db = DataBase(CONFIG)
        self.engine = self.db.get_engine()

    def run(self):
        reader = SQLReader(self.engine)

        trade_df = reader.get_trade_df()
        position_df = reader.get_position_df()
        balance_df = reader.get_latest_balance()

        pair = Pair(trade_df, position_df)
        pair_df = pair.get_pair_df()
        today_position_df = pair.get_today_position()

        d = DailyStatics(pair_df, balance_df)
        d1 = d.get_df_on_instrument()
        d2 = d.get_df_on_instrument_type()
        d3 = d.get_df_on_strategy_id()

        p = Position(today_position_df)
        d4 = p.get_position_df_on_instrument()
        w = HiveQuantWriter(self.engine)
        w.print_daily_summary(d3)
        w.print_position(d4)
        w.print_trade_pair(pair_df)


class MockBuilder(Builder):

    def __init__(self):
        super(MockBuilder, self).__init__()
        self.mock = Mock(self.engine)

    def run(self):
        self.mock.clean()
        self.mock.generate_trade_data('1/1/2017', 0)
        super(MockBuilder, self).run()
        self.mock.generate_trade_data('1/2/2017', 1000)
        super(MockBuilder, self).run()


class ProdBuilder(Builder):

    def __init__(self):
        super(ProdBuilder, self).__init__()
