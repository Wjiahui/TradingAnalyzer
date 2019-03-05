import pandas as pd
import re


class DailyStatics(object):

    def __init__(self, trade_pair, balance_df):
        self.trade_pair = trade_pair
        self.balance_df = balance_df
        self.row_df = pd.DataFrame()
        self.preprocess()

    def preprocess(self):
        grouped = self.trade_pair.groupby(['strategy_id', 'instrument'])

        for (strategy_id, instrument), group in grouped:
            profit_position = 0
            loss_position = 0

            profit = 0
            loss = 0

            profit_count = 0
            count = 0

            for index, series in group.iterrows():

                if type(series['amount']) == str:
                    sum_amount = 0
                    for item in series['amount'].split(','):
                        sum_amount += int(item)
                else:
                    sum_amount = series['amount']

                if series['net_profit'] >= 0:
                    profit_position += sum_amount
                    profit += series['net_profit']
                    profit_count += 1

                else:
                    loss_position += sum_amount
                    loss += abs(series['net_profit'])

                count += 1

            p = re.compile(r'[a-z]{2}')
            instrument_type = "".join(p.findall(instrument))

            d = {"strategy_id": strategy_id,
                 "instrument_type": instrument_type,
                 "instrument": instrument,
                 "profit_position": profit_position,
                 "loss_position": loss_position,
                 "profit": profit,
                 "loss": loss,
                 "profit_count": profit_count,
                 "loss_count": (count - profit_count),
                 "round": count}
            self.row_df = self.row_df.append(d, ignore_index=True)
            self.row_df['net_profit'] = self.row_df['profit'] - self.row_df['loss']

    def get_df_on_instrument(self):

        daily_df = self.row_df.copy(deep=True)

        daily_df['winning_rate'] = daily_df['profit_count'] / daily_df['round']

        return daily_df

    def get_df_on_instrument_type(self):

        daily_df = self.row_df.copy(deep=True)

        del daily_df['instrument']

        group_by_instrument_type = daily_df.groupby(['strategy_id', "instrument_type"])
        daily_df = group_by_instrument_type.sum()

        daily_df['winning_rate'] = daily_df['profit_count'] / daily_df['round']

        return daily_df

    def get_df_on_strategy_id(self):

        daily_df = self.row_df.copy(deep=True)

        del daily_df['instrument']
        del daily_df['instrument_type']

        group_by_strategy = daily_df.groupby(['strategy_id'])
        daily_df = group_by_strategy.sum()
        daily_df['winning_rate'] = daily_df['profit_count'] / daily_df['round']
        daily_df = daily_df.reset_index()
        daily_df = pd.merge(daily_df, self.balance_df, on='strategy_id')
        daily_df['balance'] = daily_df['balance'] + daily_df['net_profit']
        daily_df['transaction_date'] = self.trade_pair['close_time'][0]

        return daily_df
