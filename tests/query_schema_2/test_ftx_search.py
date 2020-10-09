from datanator_query_python.query_schema_2 import ftx_search
import unittest


class TestQEn(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = ftx_search.FTX()

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    def test_search_taxon(self):
        docs = self.src.search_taxon("off",
                                     token_order="any")
        print(docs)