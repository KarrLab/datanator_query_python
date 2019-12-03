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
        docs, count = self.src.get_doc_by_locus('BUAP5A_486')
        for doc in docs:
            self.assertEqual(doc['gene_name'], 'aroE')

    def test_get_gene_protein_name_by_oln(self):
        gene_name_0, protein_name_0 = self.src.get_gene_protein_name_by_oln('CENSYa_1839')
        self.assertEqual(gene_name_0, 'cdc6')
        self.assertEqual(protein_name_0, 'ORC1-type DNA replication protein')
        gene_name_1, protein_name_1 = self.src.get_gene_protein_name_by_oln('somenonesense')
        self.assertEqual(gene_name_1, None)
        self.assertEqual(protein_name_1, None)