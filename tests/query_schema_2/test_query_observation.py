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
        for i, r in enumerate(results):
            if i == 1:
                break
            self.assertEqual(r["entity"]["name"], 'BCL2-associated athanogene.')
        identfier = {"namespace": "gene_symbol", "value": "something"}
        results = self.src.get_protein_halflives(identfier)
        for r in results:            
            self.assertEqual(r, None)
