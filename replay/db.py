# -*- coding:utf-8 -*-
import MySQLdb


class Database(object):

    def __init__(self, config):
        super(Database, self).__init__()
        self.config = config

    def get_conn(self):
        try:
            conn = MySQLdb.connect(
                host=self.config['mysql']['host'],
                port=self.config['mysql']['port'],
                user=self.config['mysql']['user'],
                passwd=self.config['mysql']['passwd'],
                db=self.config['mysql']['db'],
            )
            return conn

        except MySQLdb.Error as e:
                print("Myposition_sql Error %d: %s" % (e.args[0], e.args[1]))
