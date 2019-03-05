import codecs
import yaml

CONFIG = yaml.load(codecs.open('config.yaml', 'r', 'utf8'))

LONG_OPEN = CONFIG['position_type']['long_open']
LONG_CLOSE = CONFIG['position_type']['long_close']
SHORT_OPEN = CONFIG['position_type']['short_open']
SHORT_CLOSE = CONFIG['position_type']['short_close']