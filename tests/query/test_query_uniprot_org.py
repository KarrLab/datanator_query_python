import unittest
from datanator_query_python.query import query_uniprot_org


class TestQueryUniprotOrg(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = query_uniprot_org.QueryUniprotOrg

    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_kegg_ortholog(self):
        result = query_uniprot_org.QueryUniprotOrg('arCOG00119', api='https://www.uniprot.org/uniprot/?', include='yes', compress='no',
                limit=1, offset=0).get_kegg_ortholog()
        self.assertEqual(result, 'K03341')
