import unittest
from datanator_query_python.query import query_uniprot
from datanator_query_python.config import config


class TestUniprot(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'datanator'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.src = query_uniprot.QueryUniprot(username=username, password=password,
                                            server=MongoDB, database=db, collection_str='uniprot')

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    def test_get_gene_name_by_locus(self):
        docs, count = self.src.get_gene_name_by_locus('BUAP5A_486')
        for doc in docs:
            self.assertEqual(doc['gene_name'], 'aroE')
