from datanator_query_python.query_schema_2 import query_observation
import unittest


class TestQOb(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = query_observation.QueryObs()

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    def test_get_protein_halflives(self):
        identfier = {"namespace": "gene_symbol", "value": "BAG1"}
        results = self.src.get_protein_halflives(identfier)
        self.assertEqual(results[0]["entity"]["name"], 'BCL2-associated athanogene.')
        identfier = {"namespace": "gene_symbol", "value": "something"}
        results = self.src.get_protein_halflives(identfier)
        self.assertEqual(results, [])
