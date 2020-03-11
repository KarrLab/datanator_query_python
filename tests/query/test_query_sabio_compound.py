import unittest
from datanator_query_python.query import query_sabio_compound
from datanator_query_python.config import config


class TestSabioCompound(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = 'test'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.MongoDB = MongoDB
        cls.username = username
        cls.password = password
        cls.src = query_sabio_compound.QuerySabioCompound(server=cls.MongoDB, database=cls.db,
                 verbose=True, max_entries=20, username=cls.username,
                 password=cls.password, collection_str='test_query_sabio_compound',
                 readPreference='nearest')
        compound_0 = {'_id': 0, 'name': 'a', 'synonyms': ['a0', 'a1', 'a2']}
        compound_1 = {'_id': 1, 'name': 'b', 'synonyms': ['b0', 'b1', 'b2']}
        compound_2 = {'_id': 2, 'name': 'c', 'synonyms': ['c0', 'c1', 'c2']}
        cls.src.collection.insert_many([compound_0,compound_1,compound_2])

    @classmethod
    def tearDownClass(cls):
        cls.src.db.drop_collection('test_query_sabio_compound')
        cls.src.client.close()

    def test_query_sabio_compound(self):
        names_0 = ['a', 'c1', 'nonsense','b2']
        result_0 = self.src.get_id_by_name(names_0)
        self.assertEqual(result_0, [])
        names_1 = ['nonsense']
        result_1 = self.src.get_id_by_name(names_1)
        self.assertEqual(result_1, [])

