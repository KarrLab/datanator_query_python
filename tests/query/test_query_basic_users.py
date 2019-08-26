import unittest
from datanator_query.query import query_basic_users
from datanator_query.config import config
from bson.objectid import ObjectId
import tempfile
import shutil
import os

class TestQueryBasicUsers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        db = 'registered_users'
        conf = config.UserAccountConfig()
        username = conf.USERDAEMON
        password = conf.USERDAEMON_PASSWORD
        MongoDB = conf.SERVER
        port = conf.PORT
        authDB = conf.USERDAEMON_AUTHDB
        cls.src = query_basic_users.QueryBasicUsers( MongoDB=MongoDB, db=db,
                 verbose=True, max_entries=20, username=username, password=password,
                 authSource=authDB)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)

    def test_get_username_from_id(self):
        results = self.src.get_username_from_id([ObjectId('5d4097bdbb8176b8d91db00b')])
        exp = ['test_username']
        self.assertEqual(results, exp)

    def test_get_userid_from_username(self):
        results = self.src.get_userid_from_username(['test_username'])
        exp = [ObjectId('5d4097bdbb8176b8d91db00b')]
        self.assertEqual(results, exp)