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
        expr = {"$and":
                    [
                        {"$eq": [ "$stock_item", "$$order_item"]},
                        {"$gte": ["$instock", "$$order_qty"]}
                    ]
               }
        result = self.src.aggregate_kegg_orthology(expr)
        self.assertEqual(result[1], {"$project": {'_id': 0, 'gene_ortholog': 0}})