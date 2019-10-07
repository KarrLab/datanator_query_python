import unittest
from datanator_query_python.config import config as query_config
from datanator_query_python.query import query_intact_complex


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
        cls.src = query_intact_complex.QueryIntactComplex(server=cls.MongoDB, database=cls.db,
                 verbose=True, max_entries=20, username = cls.username,
                 password = cls.password, collection_str='test_query_intact_complex')
        doc_0 = {'name': 'name_0', 'ncbi_id': 9606}
        doc_1 = {'name': 'name_1', 'ncbi_id': 9607}
        doc_2 = {'name': 'name_2', 'ncbi_id': 9606}
        doc_3 = {'name': 'name_3', 'ncbi_id': 9607}
        cls.src.collection.insert_many([doc_0,doc_1,doc_2,doc_3])

    @classmethod
    def tearDownClass(cls):
        cls.src.db.drop_collection('test_query_intact_complex')
        cls.src.client.close()

    def test_get_complex_with_ncbi(self):
        result_0 = self.src.get_complex_with_ncbi(9606)
        result_1 = self.src.get_complex_with_ncbi(9607)
        self.assertEqual(result_0[0], {'name': 'name_0', 'ncbi_id': 9606})
        self.assertEqual(result_1[1], {'name': 'name_3', 'ncbi_id': 9607})