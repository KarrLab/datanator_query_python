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

    def test_get_id_by_org_gene(self):
        org_gene = 'aly:ARALYDRAFT_486312'
        _, count = self.src.get_id_by_org_gene(org_gene)
        self.assertEqual(count, 0)
        org_gene = 'ath:AT3G58610'
        _, count = self.src.get_id_by_org_gene(org_gene)
        self.assertEqual(count, 1)

    def test_get_info_by_entrez_id(self):
        _id = '374073'
        self.assertEqual('Q75QI0', self.src.get_info_by_entrez_id(_id))
        _id = 'adfasdfaslkf'
        self.assertEqual(None, self.src.get_info_by_entrez_id(_id))