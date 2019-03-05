# -*- coding: utf-8 -*-
import logging

from acc_subacc import AccountWriter
from config import *
from db import Database
from flask import Flask, request, send_from_directory, jsonify, make_response
from fund import FundWriter
from subacc import SubAccountWriter

from replay.user_subacc import UserWriter

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='myapp.log',
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

logging.debug('This is debug message')
logging.info('This is info message')
logging.warning('This is warning message')

app = Flask(__name__)

app.config['UPLOAD_FOLDER'] = app.root_path


@app.route('/', methods=['GET'])
def download():

    code = request.args.get('code')
    _type = request.args.get('type')


    db = Database(CONFIG)
    conn = db.get_conn()
    _type_value = CONFIG['type'].values()

    if code is None:
        return jsonify(ERRORS['error_codes']['code_empty'])

    elif _type is None:
        return jsonify(ERRORS['error_codes']['type_empty'])

    try:
        _type = int(_type)
    except Exception as e:
        return jsonify(ERRORS['error_codes']['type_invalid'])

    if _type not in _type_value:
        return jsonify(ERRORS['error_codes']['type_invalid'])

    # fund->user
    elif _type == CONFIG['type']['fund_user_type']:
        fund = FundWriter(CONFIG, conn)
        error = fund.check_valid(code, _type)
        if error['code'] == 0:
            fund.get_fund_dfs(code, 1)
            fund.to_excel()
            fund.close()
            return send_from_directory(app.config['UPLOAD_FOLDER'], fund.get_excel_name(), as_attachment=True)
        else:
            return jsonify(error)
    # fund->account
    elif _type == FUND_ACC_TYPE:
        fund = FundWriter(CONFIG, conn)
        error = fund.check_valid(code, _type)
        if error['code'] == 0:
            fund.get_fund_dfs(code, 2)
            fund.to_excel()
            fund.close()
            return send_from_directory(app.config['UPLOAD_FOLDER'], fund.get_excel_name(), as_attachment=True)
        else:
            return jsonify(error)
    # user
    elif _type == USER_SUB_TYPE:
        user = UserWriter(CONFIG, conn)
        error = user.check_valid(code, _type)
        if error['code'] == 0:
            user.get_user_dfs(code)
            user.to_excel()
            user.close()
            return send_from_directory(app.config['UPLOAD_FOLDER'], user.get_excel_name(), as_attachment=True)
        else:
            return jsonify(error)
    # account
    elif _type == ACC_SUB_TYPE:
        account = AccountWriter(CONFIG, conn)
        error = account.check_valid(code, _type)
        if error['code'] == 0:
            account.get_account_dfs(code)
            account.to_excel()
            account.close()
            return send_from_directory(app.config['UPLOAD_FOLDER'], account.get_excel_name(), as_attachment=True)
        else:
            return jsonify(error)
    # subaccount
    elif _type == SUB_TYPE:
        subaccount = SubAccountWriter(CONFIG, conn)
        error = subaccount.check_valid(code, _type)
        if error['code'] == 0:
            subaccount.get_subaccount_dfs(code)
            subaccount.to_excel()
            subaccount.close()
            return send_from_directory(app.config['UPLOAD_FOLDER'], subaccount.get_excel_name(), as_attachment=True)
        else:
            return jsonify(error)


@app.errorhandler(404)
def not_found():
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':
    app.run()
