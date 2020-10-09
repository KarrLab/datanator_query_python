import os
os.environ["WHERE"] = "API_TEST"
import unittest
from datanator_query_python.config import config as query_config
from datanator_query_python.query_schema_2 import query_taxon_tree_v2
import asyncio
from pprint import pprint



class TestQTaxon(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(os.getenv("WHERE"))
        cls.src = query_taxon_tree_v2.QTaxon()

    @classmethod
    def tearDownClass(cls):
        os.environ["WHERE"] = ""

    def test_get_canon_ancestor(self):
        _id = 1915648
        loop = asyncio.get_event_loop()
        ids, _ = loop.run_until_complete(self.src.get_canon_ancestor(_id, _format='tax_id'))         
        self.assertTrue(2283796 not in ids)
        self.assertTrue(2157 in ids)
        _id = "nonsense"
        loop = asyncio.get_event_loop()
        ids, _ = loop.run_until_complete(self.src.get_canon_ancestor(_id, _format='tax_name'))  
        self.assertEqual([], ids)

    # @unittest.skip("for now")
    def test_aggregate_distance(self):
        measured_0 = [{"canon_anc_ids": [131567, 2, 1224, 1236, 91347, 543, 590, 28901],
                       "tax_name": "Salmonella enterica subsp. enterica serovar Newport str. CFSAN000907"}] #tax_id1299189
        target_0 = 0
        target_1 = 1227178
        loop = asyncio.get_event_loop()
        result_0 = loop.run_until_complete(self.src.aggregate_distance(measured_0, target_0, name_field='tax_name'))
        self.assertEqual(result_0, measured_0)
        result_1 = loop.run_until_complete(self.src.aggregate_distance(measured_0, target_1, name_field='tax_name'))
        self.assertEqual(result_1[0]['taxon_distance']['Salmonella enterica subsp. enterica serovar Newport str. CFSAN001557'], 0)
        # taget is measured's ancestor
        measured_1 = [{"canon_anc_ids": [131567, 2759, 4751, 4890, 4891, 4892, 4893, 4930, 4932],
                       "tax_name": "Saccharomyces cerevisiae CAT-1"}]
        target_2 = 4932  # Saccharomyces cerevisiae
        result_2 = loop.run_until_complete(self.src.aggregate_distance(measured_1, target_2, name_field='tax_name'))
        self.assertEqual(result_2[0]['taxon_distance']['Saccharomyces cerevisiae'], 0)
        self.assertEqual(result_2[0]['taxon_distance']['Saccharomyces cerevisiae CAT-1'], 1)