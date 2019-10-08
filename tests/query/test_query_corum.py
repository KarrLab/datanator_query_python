import unittest
from datanator_query_python.config import config as query_config
from datanator_query_python.query import query_corum


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
        cls.src = query_corum.QueryCorum(server=cls.MongoDB, database=cls.db,
                 verbose=True, max_entries=20, username = cls.username,
                 password = cls.password, collection_str='test_query_corum')
        doc_0 = {'subunits_uniprot_id': ['P0', 'P1'], 'SWISSPROT_organism_NCBI_ID': 9606}
        doc_1 = {'subunits_uniprot_id': ['P1', 'P2'], 'SWISSPROT_organism_NCBI_ID': 9606}
        doc_2 = {'subunits_uniprot_id': ['P1', 'P2', 'P3'], 'SWISSPROT_organism_NCBI_ID': 9606}
        doc_3 = {'subunits_uniprot_id': ['P1', 'P2'], 'SWISSPROT_organism_NCBI_ID': 9607}
        doc_4 = {'subunits_uniprot_id': ['P1', 'P3'], 'SWISSPROT_organism_NCBI_ID': 9607}
        cls.src.collection.insert_many([doc_0,doc_1,doc_2,doc_3,doc_4])


    @classmethod
    def tearDownClass(cls):
        cls.src.db.drop_collection('test_query_corum')
        cls.src.client.close()

    def test_get_complexes_with_uniprot(self):
        result_0 = self.src.get_complexes_with_uniprot('P2', ncbi_id=9606)
        result_1 = self.src.get_complexes_with_uniprot('P2', ncbi_id=9607)
        self.assertEqual(result_0[0], {'subunits_uniprot_id': ['P1', 'P2'], 'SWISSPROT_organism_NCBI_ID': 9606})
        self.assertEqual(result_1[0], {'subunits_uniprot_id': ['P1', 'P2'], 'SWISSPROT_organism_NCBI_ID': 9607})

    def test_get_complexes_with_ncbi(self):
        result_0 = self.src.get_complexes_with_ncbi(9606)
        self.assertEqual(len(result_0), 3)