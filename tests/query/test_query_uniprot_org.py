import unittest
from datanator_query_python.query import query_uniprot_org


class TestQueryUniprotOrg(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = query_uniprot_org.QueryUniprotOrg()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_kegg_ortholog(self):
        result = self.src.get_kegg_ortholog('arCOG00119')
        self.assertEqual(result, 'K03341')