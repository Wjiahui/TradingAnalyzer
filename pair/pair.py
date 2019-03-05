# coding=utf8
from collections import deque
import pandas as pd

from config import *


def transfer(position_df):
    """
    position_df转换为trade_df
    :param position_df: 
    :return: 
    """
    position_df = position_df.rename(columns={'avg_price': 'price', 'transaction_date': 'trade_time'})
    position_df['offset'] = 0
    return position_df


class Pair(object):

    def __init__(self, trade_df, position_df):
        self.pair_df = pd.DataFrame()
        self.trade_df = trade_df
        self.position_df = position_df
        self.today_position_df = pd.DataFrame()

    def get_pair_df(self):
        """
        平仓时 配对开仓
        :return: pair_df
        """

        yesterday_df = transfer(self.position_df)
        new_df = pd.concat([yesterday_df, self.trade_df])

        grouped = new_df.groupby(['strategy_id', 'instrument'])
        for index, group in grouped:
            d = self.get_instrument_deque(group)
            long_open = d['long_open']
            long_close = d['long_close']
            short_open = d['short_open']
            short_close = d['short_close']
            self.get_operation_deque(long_close, long_open)
            self.get_operation_deque(short_close, short_open)
            # 计算今天持仓

            for open_dict in long_open:
                self.today_position_df = self.today_position_df.append(open_dict, ignore_index=True)
            for open_dict in short_open:
                self.today_position_df = self.today_position_df.append(open_dict, ignore_index=True)

        return self.pair_df

    def get_today_position(self):
        """
        
        :return: 今日仓位 on strategy_id and instrument
        """

        return self.today_position_df

    def get_instrument_deque(self, _group):
        """
        分类：long_open, long_close, short_open, short_close
        :param _group: 
        :return: 四个deque
        """

        long_open = deque()
        long_close = deque()
        short_open = deque()
        short_close = deque()

        for index, series in _group.iterrows():
            if series['offset'] == LONG_OPEN['offset'] and series['direction'] == LONG_OPEN['direction']:
                long_open.append(series)
            elif series['offset'] == LONG_CLOSE['offset'] and series['direction'] == LONG_CLOSE['direction']:
                long_close.append(series)
            elif series['offset'] == SHORT_OPEN['offset'] and series['direction'] == SHORT_OPEN['direction']:
                short_open.append(series)
            elif series['offset'] == SHORT_CLOSE['offset'] and series['direction'] == SHORT_CLOSE['direction']:
                short_close.append(series)

        return {'long_open': long_open,
                'long_close': long_close,
                'short_open': short_open,
                'short_close': short_close}

    def get_operation_deque(self, close_deque, open_deque):
        """
        遍历close_deque 
        :param close_deque: 
        :param open_deque: 
        :return: 
        """

        for close_series in close_deque:
            if self.check_amount(close_series, open_deque):
                self.get_each_operation(close_series, open_deque)
            else:
                raise ValueError("amount invalid")

    def check_amount(self, close_series, open_deque):
        """
        判断 平仓前 是否有足够开仓
        :param close_series: 
        :param open_deque: 
        :return: boolean is_valid
        """
        sum_amount = 0
        for open_series in open_deque:
            if pd.to_datetime(open_series['trade_time']) < pd.to_datetime(close_series['trade_time']):
                sum_amount += open_series['amount']

        if close_series['amount'] > sum_amount:
            return False
        else:
            return True

    def get_each_operation(self, close_series, open_deque):
        """
        一次平仓 读开仓队列  配对
        :param close_series: 
        :param open_deque: 
        :return: 
        """
        open_series = open_deque[0]

        # 平仓 = 第一次开仓部分
        if close_series['amount'] <= open_series['amount']:
            net_profit = (close_series['price'] - open_series['price']) * close_series['amount']

            if close_series['amount'] == open_series['amount']:
                open_deque.popleft()
            else:
                open_series['amount'] -= close_series['amount']

            d = {'strategy_id': close_series['strategy_id'],
                 'long_short': close_series['direction'],
                 'instrument': close_series['instrument'],
                 'amount': close_series['amount'],
                 'open_time': open_series['trade_time'],
                 'close_time': close_series['trade_time'],
                 'open_price': open_series['price'],
                 'close_price': close_series['price'],
                 'net_profit': net_profit}

            self.pair_df = self.pair_df.append(d, ignore_index=True)

        # 平仓 = 第一次开仓 + 第二次 + ...
        else:
            sum_amount = 0
            used_deque = deque()

            for open_series in open_deque:
                if sum_amount < close_series['amount']:
                    sum_amount += open_series['amount']
                    used_deque.append(open_series)

            sum_price = 0
            amount = close_series['amount']
            open_time_deque = deque()

            for open_series in used_deque:

                open_time_deque.append(open_series['trade_time'])

                # 取第n次开仓部分
                if open_series is used_deque[-1] and not amount == sum_amount:
                    remain = sum_amount - amount
                    used_amount = open_series['amount'] - remain
                    sum_price += open_series['price'] * used_amount
                    open_deque[0]['amount'] = remain

                #  取前n-1次开仓 || 第n次开仓全部
                else:
                    sum_price += open_series['price'] * open_series['amount']
                    open_deque.popleft()

            net_profit = close_series['price'] * amount - sum_price

            open_time_str = ""
            for item in open_time_deque:
                open_time_str = open_time_str + str(item) + ","
            open_time_str = open_time_str[:-1]

            self.pair_df = self.pair_df.append({'strategy_id': close_series['strategy_id'],
                                                'long_short': close_series['direction'],
                                                'instrument': close_series['instrument'],
                                                'amount': amount,
                                                'open_time': open_time_str,
                                                'close_time': close_series['trade_time'],
                                                'open_price': sum_price/amount,
                                                'close_price': close_series['price'],
                                                'net_profit': net_profit}, ignore_index=True)
