import unittest
from datanator_query_python.aggregate import lookups


class TestLookups(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = lookups.Lookups()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_simple_lookup(self):
        result = self.src.simple_lookup("kegg_orthology", "local", "foreign", "_as")
        self.assertEqual(result["$lookup"]['as'], "_as")