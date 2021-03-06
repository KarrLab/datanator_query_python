from datanator_query_python.query import query_rna_halflife
from datanator_query_python.config import config
import unittest


class TestQueryRna(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'datanator'
        conf = config.TestConfig()
        username = conf.USERNAME
        password = conf.PASSWORD
        MongoDB = conf.SERVER
        cls.col_str = 'rna_halflife_new'
        cls.src = query_rna_halflife.QueryRNA(server=MongoDB, username=username, password=password,
                                            verbose=True, db=db, collection_str=cls.col_str,
                                            readPreference='nearest')

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    @unittest.skip('collection changed.')
    def test_get_doc_by_oln(self):
        oln_0 = 'ma0003'
        docs_0, count_0 = self.src.get_doc_by_oln(oln_0)
        self.assertEqual(count_0, 1)
        for doc in docs_0:
            self.assertEqual(len(doc['halflives']), 2)
        
    def test_get_doc_by_protein_name(self):
        protein_name_0 = 'Cell division control protein 6'
        _, count_0 = self.src.get_doc_by_names(protein_name_0)
        self.assertEqual(count_0, 1)

    def test_get_doc_by_ko(self):
        ko_number = 'K13280'
        docs, count = self.src.get_doc_by_ko(ko_number)
        self.assertEqual(docs[0]['uniprot_id'], 'P15367')