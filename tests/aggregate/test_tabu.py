import unittest
from datanator_query_python.aggregate import tabu
from datanator_query_python.config import config


class TestTabu(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'datanator'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.src = tabu.Tabu(MongoDB=MongoDB, username=username, password=password,
                            db=db, max_entries=20, verbose=True, authSource='admin',
                            readPreference='nearest')

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()


