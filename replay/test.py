import nose
import requests
from nose.tools import assert_equal

from replay.config import *

base_url = "http://127.0.0.1:5000/"


class Test():
    def test_invalid(self):
        url = base_url
        r = requests.get(url)
        d = r.json()
        assert_equal(d['description'], CODE_EMPTY)

        url = base_url + "?code=123"
        r = requests.get(url)
        d = r.json()
        assert_equal(d['description'], TYPE_EMPTY)

        url = base_url + "?code=123&type=19"
        r = requests.get(url)
        d = r.json()
        assert_equal(d['description'], TYPE_INVALID)

    def test_fund_not_exist(self):
        url = base_url + "?code=135&type={}".format(FUND_USER_TYPE)
        r = requests.get(url)
        d = r.json()
        assert_equal(d['description'], FUND_NOT_EXIST)

        url = base_url + "?code=111&type={}".format(FUND_ACC_TYPE)
        r = requests.get(url)
        d = r.json()
        assert_equal(d['description'], FUND_NOT_EXIST)

    def test_user_not_exist(self):
        url = base_url + "?code=111&type={}".format(USER_SUB_TYPE)
        r = requests.get(url)
        d = r.json()
        assert_equal(d['description'], USER_NOT_EXIST)

    def test_account_not_exist(self):
        url = base_url + "?code=111&type={}".format(ACC_SUB_TYPE)
        r = requests.get(url)
        d = r.json()
        assert_equal(d['description'], ACCOUNT_NOT_EXIST)

    def test_subaccount_not_exist(self):
        url = base_url + "?code=18929&type={}".format(SUB_TYPE)
        r = requests.get(url)
        d = r.json()
        assert_equal(d['description'], SUBACCOUNT_NOT_EXIST)

    # def test_subaccount(self):
    #
    #     url = base_url + "?code=044226-000&type={}".format(SUB_TYPE)
    #     r = requests.get(url)
    #     d = r.json()
    #     assert_equal(d['description'], 'valid')

        # def test_fund_user(self):
        #
        #     url = base_url + "?code=123&type={}".format(FUND_USER_TYPE)
        #     r = requests.get(url)
        #     d = r.json()
        #     assert_equal(d['description'], 'valid')
        #
        # def test_fund_account(self):
        #
        #     url = base_url + "?code=123&type={}".format(FUND_ACC_TYPE)
        #     r = requests.get(url)
        #     d = r.json()
        #     assert_equal(d['description'], 'valid')


        # def test_user(self):
        #     url = base_url + "?code=20003&type={}".format(USER_SUB_TYPE)
        #     r = requests.get(url)
        #     d = r.json()
        #     assert_equal(d['description'], 'valid')
        #
        # def test_account(self):
        #
        #     url = base_url + "?code=013716&type={}".format(ACC_SUB_TYPE)
        #     r = requests.get(url)
        #     d = r.json()
        #     assert_equal(d['description'], 'valid')




if __name__ == "__main__":
    nose.runmodule()


