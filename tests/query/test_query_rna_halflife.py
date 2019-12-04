from datanator_query_python.query import query_rna_halflife
from datanator_query_python.config import config
import unittest


class TestQueryRna(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'datanator'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.col_str = 'rna_halflife'
        cls.src = query_rna_halflife.QueryRNA(server=MongoDB, username=username, password=password,
                                            verbose=True, db=db, collection_str=cls.col_str,
                                            readPreference='primary')

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    def test_get_doc_by_oln(self):
        oln_0 = 'ma0003'
        docs_0, count_0 = self.src.get_doc_by_oln(oln_0)
        self.assertEqual(count_0, 1)
        for doc in docs_0:
            self.assertEqual(len(doc['halflives']), 2)
        