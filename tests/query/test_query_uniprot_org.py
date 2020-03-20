import unittest
from datanator_query_python.query import query_uniprot_org


class TestQueryUniprotOrg(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = query_uniprot_org.QueryUniprotOrg('arCOG00119', api='https://www.uniprot.org/uniprot/?', include='yes', compress='no',
                limit=1, offset=0)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_kegg_ortholog(self):
        result = self.src.get_kegg_ortholog()
        self.assertEqual(result, 'K03341')

    def test_get_uniprot(self):
        result = self.src.get_uniprot_id()
        self.assertEqual('Q6LZM9', result)

    def test_get_protein_name(self):
        result = self.src.get_protein_name()
        self.assertEqual('O-phosphoseryl-tRNA(Sec) selenium transferase', result)