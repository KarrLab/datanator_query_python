import unittest
from datanator_query_python.config import config as query_config
from datanator_query_python.query import query_ecmdb


class TestQueryCorum(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = 'test'
        conf = query_config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.MongoDB = MongoDB
        cls.username = username
        cls.password = password
        cls.src = query_ecmdb.QueryEcmdb(server=cls.MongoDB, database=cls.db,
                 verbose=True, max_entries=20, username = cls.username,
                 password = cls.password, collection_str='test_query_ecmdb')

        doc_0 = {'m2m_id': 'm2m_0', 'concentrations': ['some values 0']}
        doc_1 = {'m2m_id': 'm2m_1', 'not_concentrations': ['some values']}
        doc_2 = {'m2m_id': 'm2m_2', 'concentrations': ['some values 1']}
        doc_3 = {'m2m_id': 'm2m_2', 'field_0': ['some values 1'], 'field_2': 'add'}
        cls.src.collection.insert_many([doc_0,doc_1,doc_2,doc_3])

    @classmethod
    def tearDownClass(cls):
        cls.src.db.drop_collection('test_query_ecmdb')
        cls.src.client.close()

    def test_get_all_concentrations(self):
        result = self.src.get_all_concentrations(projection={'concentrations': 1})
        self.assertEqual(len(result), 2)