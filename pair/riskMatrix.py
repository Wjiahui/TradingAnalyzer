#coding=utf8

import pandas as pd
import empyrical as ep
from db import DataBase
from config import CONFIG


class RiskMatrix(object):
    """
        计算策略的累计风险指标
    """
    def __init__(self, engine):
        self.engine = engine

    def get_his_daily_summary(self):
        """
            获取历史的daily summary
        """
        pass

    def process(self):
        """
            计算指标的核心步骤
        """
        pass

    def output(self):
        """
            持久化
        """
        pass


class HiveQuantRiskMatrix(RiskMatrix):

    def __init__(self, engine, daily_df):
        super(HiveQuantRiskMatrix, self).__init__(engine)
        self.his_daily_df = self.get_his_daily_summary()
        self.daily_df = daily_df
        self.df = pd.concat([self.his_daily_df, self.daily_df])
        self.strategy_df = self.get_strategy_df()

    def get_strategy_df(self):
        return pd.read_sql_table('strategy', self.engine)

    #TODO
    def get_his_daily_summary(self):
        return pd.read_sql_table('daily_summary', self.engine)

    def preprocess(self):
        statics_df = pd.DataFrame()
        grouped = self.df.groupby('strategy_id')
        for strategy_id, group in grouped:
            s = self.get_statics(group, strategy_id)
            statics_df = statics_df.append(s, ignore_index=True)
        print(statics_df)
        return statics_df

    def get_statics(self, df, strategy_id):
        tmp_df = self.strategy_df[self.strategy_df['strategy_code'] == str(strategy_id)]
        if not tmp_df.empty:
            init_balance = float(tmp_df['init_principal'])
        else:
            raise Exception('init_principal for strategy(strategy_code={}) not found'.format(strategy_id))

        df['returns'] = df['balance'].pct_change()
        df['returns'].iloc[0] = df['balance'].iloc[0] / init_balance - 1

        max_draw_down = ep.max_drawdown(df['returns'])
        # df['cum_returns'] = ep.cum_returns(df['returns'])
        cum_returns_final = ep.cum_returns_final(df['returns'])
        annual_return = ep.annual_return(df['returns'])
        annual_volatility = ep.annual_volatility(df['returns'])
        calmar_ratio = ep.calmar_ratio(df['returns'])
        omega_ratio = ep.omega_ratio(df['returns'])
        sharpe_ratio = ep.sharpe_ratio(df['returns'])
        sortino_ratio = ep.sortino_ratio(df['returns'])
        downside_risk = ep.downside_risk(df['returns'])
        stability_of_timeseries = ep.stability_of_timeseries(df['returns'])
        tail_ratio = ep.tail_ratio(df['returns'])
        cagr = ep.cagr(df['returns'])
        # roll_max_drawdown = ep.roll_max_drawdown(df['returns'])
        # roll_sharpe_ratio = ep.roll_sharpe_ratio(df['returns'])
        value_at_risk = ep.value_at_risk(df['returns'])
        conditional_value_at_risk = ep.conditional_value_at_risk(df['returns'])

        s = {'strategy_id': strategy_id,
             'max_draw_down': max_draw_down,
             'cum_returns_final': cum_returns_final,
             'annual_return': annual_return,
             'annual_volatility': annual_volatility,
             'calmar_ratio': calmar_ratio,
             'omega_ratio': omega_ratio,
             'sharpe_ratio': sharpe_ratio,
             'sortino_ratio': sortino_ratio,
             'downside_risk': downside_risk,
             'stability_of_timeseries': stability_of_timeseries,
             'tail_ratio': tail_ratio,
             'cagr': cagr,
             # 'roll_max_drawdown': roll_max_drawdown,
             # 'roll_sharpe_ratio': roll_sharpe_ratio,
             'value_at_risk': value_at_risk,
             'conditional_value_at_risk': conditional_value_at_risk}
        print(s)
        return s


if __name__ == '__main__':
    db = DataBase(CONFIG)
    engine = db.get_engine()
    df = pd.DataFrame()
    hqrm = HiveQuantRiskMatrix(engine, df)
    hqrm.preprocess()
