import pandas as pd
import numpy as np
from sqlalchemy import MetaData


class Mock(object):

    def __init__(self, _engine):
        self.engine = _engine

    def generate_my_location(self):
        size = 5
        trade_time = pd.to_datetime('2013/4/3')
        df = pd.DataFrame()
        for i in range(size):
            trade_time += pd.DateOffset(day=1)
            price = np.random.random()
            s = {"trade_time": trade_time,
                 "price": price}
            df = df.append(s)
        return df

    def generate_trade_data(self, date_str, start_id):
        strategy_size = 3
        size = 200
        d = {}
        instruments = ['rb1801', 'rb1802', 'cu1801', 'cu1709']
        for i in range(strategy_size):
            d[i] = {}
            for instrument in instruments:
                d[i][instrument] = {}
                for direction in range(2, 4):
                    d[i][instrument][direction] = 0
        l = []
        init_trade_time = trade_time = pd.Timestamp(date_str)
        for i in range(size):
            strategy_id = np.random.randint(strategy_size)
            instrument = instruments[np.random.randint(4)]
            direction = np.random.randint(2, 4)
            trade_time += pd.DateOffset(hours=np.random.binomial(1, 0.1), minutes=np.random.randint(0, 60), seconds=np.random.randint(0, 60))
            if trade_time.to_datetime().date() != init_trade_time.to_datetime().date():
                break
            if i > 900:
                break
            cum_sum = d[strategy_id][instrument][direction]
            if cum_sum > 1:
                position = np.random.randint(-cum_sum, -1) if np.random.binomial(1, 0.1) == 0 else np.random.randint(1, 10)
            else:
                position = np.random.randint(1, 10)
                # position = np.random.randint(1, 10)
            s = pd.Series({
                'id': i + start_id,
                'instrument': instrument,
                # 'offset': np.random.randint(0, 2, size=size),
                'direction': direction,
                'price': np.random.random_sample() * 10000.0,
                'amount': position,
                'trade_time': trade_time,
                'strategy_id': strategy_id
            })
            d[strategy_id][instrument][direction] += position
            l.append(s)
        df = pd.DataFrame(l)
        df['offset'] = np.sign(df['amount']).apply(lambda x: 0 if x == 1 else 1)
        df['amount'] = np.abs(df['amount'])
        df.to_sql('trade', self.engine, if_exists='append', index=False)

    def clean(self):

        meta = MetaData(bind=self.engine, reflect=True)
        con = self.engine.connect()
        trans = con.begin()
        for table in reversed(meta.sorted_tables):
            if not table.name in ['strategy']:
                con.execute(table.delete())
        trans.commit()


