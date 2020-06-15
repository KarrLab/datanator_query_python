from datanator_query_python.util import motor_util
from datanator_query_python.config import config
import unittest


class TestMUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = motor_util.MotorUtil(MongoDB=config.AtlasConfig.SERVER,
                username=config.DatanatorTest.USERNAME,
                password=config.DatanatorTest.PASSWORD,
                authSource=config.AtlasConfig.AUTHDB,
                replicaSet=config.AtlasConfig.REPLSET,
                readPreference=config.AtlasConfig.READ_PREFERENCE )
        cls.test_collection = 'test_motor'
        cls.test_database = "test"

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()
        cls.src.client.get_database(cls.test_database).drop_collection(cls.test_collection)

    def test_client(self):
        self.src.client.get_database(self.test_database)[self.test_collection].insert_one({"test": 1})