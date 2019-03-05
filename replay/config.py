import codecs
import yaml

from replay.orderdict_yaml_loader import OrderedDictYAMLLoader

CONFIG = yaml.load(codecs.open('config.yaml', 'r', 'utf8'), Loader=OrderedDictYAMLLoader)
ERRORS = yaml.load(codecs.open('error.yaml', 'r', 'utf8'), Loader=OrderedDictYAMLLoader)

FUND_USER_TYPE = CONFIG['type']['fund_user_type']
FUND_ACC_TYPE = CONFIG['type']['fund_acc_type']
USER_SUB_TYPE = CONFIG['type']['user_sub_type']
ACC_SUB_TYPE = CONFIG['type']['acc_sub_type']
SUB_TYPE = CONFIG['type']['sub_type']

CODE_EMPTY = ERRORS['error_codes']['code_empty']['description']
TYPE_EMPTY = ERRORS['error_codes']['type_empty']['description']
TYPE_INVALID = ERRORS['error_codes']['type_invalid']['description']
FUND_NOT_EXIST = ERRORS['error_codes']['fund_not_exist']['description']
USER_NOT_EXIST = ERRORS['error_codes']['user_not_exist']['description']
ACCOUNT_NOT_EXIST = ERRORS['error_codes']['account_not_exist']['description']
SUBACCOUNT_NOT_EXIST = ERRORS['error_codes']['subaccount_not_exist']['description']

