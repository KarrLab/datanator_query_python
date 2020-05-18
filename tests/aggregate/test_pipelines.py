import unittest
from datanator_query_python.aggregate import pipelines


class TestPipelines(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = pipelines.Pipeline()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_aggregate_kegg_orthology(self):
        result = self.src.aggregate_kegg_orthology()
        self.assertEqual(result[1], {"$project": {"stock_item": 0, "_id": 0}})