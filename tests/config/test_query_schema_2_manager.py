import unittest
from datanator_query_python.config import query_schema_2_manager
from pymongo import ReadPreference


class TestQ(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = query_schema_2_manager.QM()

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    def test_conn_protein(self):
        result = self.src.conn_protein('datanator-test')
        self.assertEqual(result.read_preference, ReadPreference.NEAREST)