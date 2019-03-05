from sqlalchemy import create_engine
import logging

class DataBase(object):

    def __init__(self, _config):
        super(DataBase, self).__init__()
        self.config = _config

    def get_engine(self):
        try:
            user = self.config['mysql']['user']
            passwd = self.config['mysql']['passwd']
            host = self.config['mysql']['host']
            port = self.config['mysql']['port']
            db = self.config['mysql']['db']
            coon_string = 'mysql://{}:{}@{}:{}/{}'.format(user, passwd, host, port, db)
            engine = create_engine(coon_string, echo=True)
            return engine
        except IOError as e:
            logging.error(e)
