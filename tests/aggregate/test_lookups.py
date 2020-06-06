import unittest
from datanator_query_python.aggregate import lookups


# just for test branch upload
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

    def test_complex_lookup(self):
        result = self.src.complex_lookup("taxon_tree", {'something': 1}, [{'this': 0}], 'output')
        self.assertEqual(result["$lookup"]["let"], {'something': 1})