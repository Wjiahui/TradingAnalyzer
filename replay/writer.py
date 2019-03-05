# -*- coding:utf-8 -*-
import time

import pandas as pd

from replay.config import *


class Writer(object):

    def __init__(self, config, conn):
        self.config = config
        self.conn = conn
        self.writer = None
        self.excel_name = self.config['excel']['name'] + str(time.time()) + '.xlsx'
        self.df_list = []

    def get_excel_name(self):
        return self.excel_name

    def close(self):
        if self.writer:
            self.writer.save()

    def to_excel(self):
        self.writer = pd.ExcelWriter(self.excel_name, engine='xlsxwriter')
        for dict in reversed(self.df_list):
            self.df_to_excel(dict['dfs'])

    def df_to_excel(self, t):

        _, settlement_df, his_position_df, current_position_df, last_position_df, increase_position_df, \
            decrease_position_df, days_df, code = t

        current_trading_day, last_trading_day = days_df.apply(lambda x: x.strftime('%Y%m%d'))
        num_settlement_df, col_settlement_df = settlement_df.shape

        settlement_df.to_excel(self.writer,
                               sheet_name=u'资金_{}'.format(code),
                               index_label=[self.config['table']['settlement']['all']['TradingDay']],
                               header=map(lambda x: self.config['table']['settlement']['all'][x] if x in self.config[
                                   'table']['settlement']['all'].keys() else x, settlement_df.columns),
                               float_format='%.4f')

        if not days_df.empty:
            # shape返回一个tuple (num_index, num_column)
            offset_row = current_position_df.shape[0] + 6
            offset_col = current_position_df.shape[1] + 6
            current_position_df.to_excel(self.writer,
                                        sheet_name=u'最新持仓_{}'.format(code),
                                        index_label=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                                                self.config[
                                                                                                                    'table'][
                                                                                                                    'position'][
                                                                                                                    'all'].keys() else x,
                                                        current_position_df.index.names),
                                        header=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                                           self.config[
                                                                                                               'table'][
                                                                                                               'position'][
                                                                                                               'all'].keys() else x,
                                                   current_position_df.columns),
                                        startrow=2,
                                        startcol=0)
            last_position_df.to_excel(self.writer,
                                     sheet_name=u'最新持仓_{}'.format(code),
                                     index_label=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                                             self.config[
                                                                                                                 'table'][
                                                                                                                 'position'][
                                                                                                                 'all'].keys() else x,
                                                     last_position_df.index.names),
                                     header=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                                        self.config[
                                                                                                            'table'][
                                                                                                            'position'][
                                                                                                            'all'].keys() else x,
                                                last_position_df.columns),
                                     startrow=2,
                                     startcol=offset_col)
        if not days_df.empty and not increase_position_df.empty:
            increase_position_df.to_excel(self.writer,
                            sheet_name=u'最新持仓_{}'.format(code),
                            index_label=map(lambda x: self.config['table']['position']['all'][x] if x in self.config[
                                'table']['position']['all'].keys() else x, increase_position_df.index.names),
                            header=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                               self.config['table'][
                                                                                                   'position'][
                                                                                                   'all'].keys() else x,
                                       increase_position_df.columns),
                            startrow=offset_row,
                            startcol=0)
        if not days_df.empty and not decrease_position_df.empty:
            decrease_position_df.to_excel(self.writer,
                            sheet_name=u'最新持仓_{}'.format(code),
                            index_label=map(lambda x: self.config['table']['position']['all'][x] if x in self.config[
                                'table']['position']['all'].keys() else x, decrease_position_df.index.names),
                            header=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                               self.config['table'][
                                                                                                   'position'][
                                                                                                   'all'].keys() else x,
                                       decrease_position_df.columns),
                            startrow=offset_row + increase_position_df.shape[0] + 2,
                            startcol=0)

        his_position_df.to_excel(self.writer,
                                 sheet_name=u'历史持仓_{}'.format(code),
                                 index_label=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                                         self.config[
                                                                                                             'table'][
                                                                                                             'position'][
                                                                                                             'all'].keys() else x,
                                                 his_position_df.index.names),
                                 header=map(lambda x: self.config['table']['position']['all'][x] if x in self.config[
                                     'table']['position']['all'].keys() else x, his_position_df.columns)
                                 )

        workbook = self.writer.book
        worksheet = self.writer.sheets[u'资金_{}'.format(code)]
        bold = workbook.add_format({'bold': True})
        italic = workbook.add_format({'italic': True})
        money = workbook.add_format({'num_format': u'￥#,###.##', 'align': 'right'})
        integer = workbook.add_format({'num_format': '0', 'align': 'right'})
        decimal = workbook.add_format({'num_format': '0.00', 'align': 'right'})
        percentage = workbook.add_format({'num_format': '0.0%', 'align': 'right'})
        green = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        red = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        yellow = workbook.add_format({'bg_color': '#FFFF00'})
        worksheet.freeze_panes(1, 0)
        # worksheet.conditional_format('K{}:K{}'.format(2, num_settlement_df + 1), {'type': '3_color_scale'})
        worksheet.conditional_format('E{}:E{}'.format(2, num_settlement_df + 1), {'type': 'cell',
                                                                                  'criteria': '>=',
                                                                                  'value': 0,
                                                                                  'format': green})

        worksheet.conditional_format('E{}:E{}'.format(2, num_settlement_df + 1), {'type': 'cell',
                                                                                  'criteria': '<',
                                                                                  'value': 0,
                                                                                  'format': red})

        networth_chart = workbook.add_chart({'type': 'line'})

        networth_chart.add_series({
            'name': u'净值',
            'categories': u'=资金_{}!$A${}:$A${}'.format(code, 2, num_settlement_df + 1),
            'values': u'=资金_{}!$P${}:$P${}'.format(code, 2, num_settlement_df + 1)})
        networth_chart.set_x_axis({
            'name_font': {'size': 14, 'bold': True},
            'num_font': {'italic': True},
        })

        worksheet.insert_chart('R2', networth_chart)
        cum_return_chart = workbook.add_chart({'type': 'line'})

        cum_return_chart.add_series({
            'name': u'累计盈亏',
            'categories': u'=资金_{}!$A${}:$A${}'.format(code, 2, num_settlement_df + 1),
            'values': u'=资金_{}!$K${}:$K${}'.format(code, 2, num_settlement_df + 1)})
        cum_return_chart.set_x_axis({
            'name_font': {'size': 14, 'bold': True},
            'num_font': {'italic': True},
        })

        worksheet.insert_chart('R20', cum_return_chart)

        if not days_df.empty:
            worksheet = self.writer.sheets[u'最新持仓_{}'.format(code)]
            worksheet.write_string(0, 0, current_trading_day, bold)
            worksheet.write_string(1, 0, u'当前持仓', bold)
            worksheet.write_string(offset_row - 1, 0, u'增加的持仓', bold)
            worksheet.write_string(offset_row + increase_position_df.shape[0] + 1, 0, u'减少的持仓', bold)

            worksheet.write_string(0, offset_col, last_trading_day, bold)
            worksheet.write_string(1, offset_col, u'上一交易日持仓', bold)

        worksheet = self.writer.sheets[u'历史持仓_{}'.format(code)]
        worksheet.freeze_panes(1, 0)

    def get_instruments(self):
        sql = "select InstrumentID, ProductCode from Instrument"
        df = pd.read_sql(sql, self.conn)
        df.rename(columns={'InstrumentID': 'InstrumentCode'}, inplace=True)
        return df

    def get_users_by_fund_code(self, fund_code):
        sql = "select UserCode from Mapping where FundCode = {} and type = '1' order by UserCode".format(fund_code)
        df = pd.read_sql(sql, self.conn)
        return df

    def get_accounts_by_fund_code(self, fund_code):
        sql = "select AccountCode from Mapping where FundCode = {} and type = '2' order by AccountCode".format(fund_code)
        df = pd.read_sql(sql, self.conn)
        return df

    def get_subaccounts_by_user_code(self, user_code):
        sql = "select SubAccCode from Mapping where UserCode = {} and type = '13' order by SubAccCode".format(user_code)
        df = pd.read_sql(sql, self.conn)
        return df

    def get_subaccounts_by_account_code(self, account_code):
        sql = "select SubAccountCode from SubAccount where AccountCode = {} order by SubAccountCode".format(account_code)
        df = pd.read_sql(sql, self.conn)
        return df

    def get_subaccount_by_subaccount_code(self, subaccount_code):
        sql = "select * from SubAccount where SubAccountCode = {}".format(subaccount_code)
        df = pd.read_sql(sql, self.conn)
        return df

    def check_valid(self, code, _type):

        # fund
        if _type == FUND_USER_TYPE or _type == FUND_ACC_TYPE:
            fund_code = code
            fund_sql = "select * from Mapping where FundCode = {}".format(fund_code)
            fund_df = pd.read_sql(fund_sql, self.conn)
            if fund_df.empty:
                return ERRORS['error_codes']['fund_not_exist']
            else:
                return {'code': 0, 'description': 'valid'}

        # user
        elif _type == USER_SUB_TYPE:
            user_code = code
            user_sql = "select * from Mapping where UserCode = {}".format(user_code)
            user_df = pd.read_sql(user_sql, self.conn)
            if user_df.empty:
                return ERRORS['error_codes']['user_not_exist']
            else:
                return {'code': 0, 'description': 'valid'}

        # account
        elif _type == ACC_SUB_TYPE:
            account_code = code
            account_sql = "select * from Mapping where AccountCode = {}".format(account_code)
            account_df = pd.read_sql(account_sql, self.conn)
            if account_df.empty:
                return ERRORS['error_codes']['account_not_exist']
            else:
                return {'code': 0, 'description': 'valid'}

        # subaccount
        elif _type == SUB_TYPE:
            subaccount_code = code
            subaccount_df = self.get_subaccount_by_subaccount_code(subaccount_code)
            if subaccount_df.empty:
                return ERRORS['error_codes']['subaccount_not_exist']
            else:
                return {'code': 0, 'description': 'valid'}
