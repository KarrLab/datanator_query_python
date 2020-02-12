import unittest
from datanator_query_python.config import config as query_config
from datanator_query_python.query import query_xmdb


class TestQueryEcmdb(unittest.TestCase):

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
        cls.src = query_xmdb.QueryXmdb(server=cls.MongoDB, database=cls.db,
                 verbose=True, max_entries=20, username = cls.username,
                 password = cls.password, collection_str='test_query_ecmdb')

        doc_0 = {'m2m_id': 'm2m_0', 'concentrations': ['some values 0'], 'inchikey': 'key0', 'name': 'name0'}
        doc_1 = {'m2m_id': 'm2m_1', 'not_concentrations': ['some values'], 'inchikey': 'key1', 'name': 'name1'}
        doc_2 = {'m2m_id': 'm2m_2', 'concentrations': ['some values 1'], 'inchikey': 'key2', 'name': 'name2'}
        doc_3 = {'m2m_id': 'm2m_2', 'field_0': ['some values 1'], 'field_2': 'add', 'inchikey': 'key3', 'name': 'name3'}
        cls.src.collection.insert_many([doc_0,doc_1,doc_2,doc_3])

    @classmethod
    def tearDownClass(cls):
        cls.src.db.drop_collection('test_query_ecmdb')
        cls.src.client.close()

    @unittest.skip('immediate query after insertion causes indeterminant behavior')
    def test_get_all_concentrations(self):
        result = self.src.get_all_concentrations(projection={'concentrations': 1})
        self.assertEqual(len(result), 2)

    def test_get_name_by_inchikey(self):
        key_0 = 'key0'
        name_0 = self.src.get_name_by_inchikey(key_0)
        self.assertEqual(name_0, 'name0')