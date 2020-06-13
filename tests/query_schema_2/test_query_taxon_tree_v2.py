import unittest
from datanator_query_python.config import config as query_config
from datanator_query_python.query_schema_2 import query_taxon_tree_v2


class TestQTaxon(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = query_taxon_tree_v2.QTaxon(username=query_config.TestConfig.MONGO_TEST_USERNAME,
                                             password=query_config.TestConfig.MONGO_TEST_PASSWORD)

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    def test_get_canon_ancestor(self):
        _id = 1915648
        ids, _ = self.src.get_canon_ancestor(_id, _format='tax_id')
        self.assertTrue(2283796 not in ids)
        self.assertTrue(2157 in ids)
        _id = "nonsense"
        ids, _ = self.src.get_canon_ancestor(_id, _format='tax_name')
        self.assertEqual([], ids)