import unittest
from datanator_query_python.aggregate import pipelines


class TestPipelines(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = pipelines.Pipeline()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_aggregate_common_canon_ancestors(self):
        result = self.src.aggregate_common_canon_ancestors({'canon_anc_ids': [0,1,2,3]}, 1, org_format='tax_id')
        self.assertEqual(result[0], {"$match": {'tax_id': 1}})
        self.assertEqual(result[1]["$project"]["anc_match"]["$setIntersection"][0], "$canon_anc_ids")

    def test_aggregate_kegg_orthology(self):
        expr = {"$and":
                    [
                        {"$eq": [ "$stock_item", "$$order_item"]},
                        {"$gte": ["$instock", "$$order_qty"]}
                    ]
               }
        result = self.src.aggregate_kegg_orthology(expr)
        self.assertEqual(result[1], {"$project": {'_id': 0, 'gene_ortholog': 0}})

    def test_aggregate_total_array_length(self):
        field = 'test'
        self.assertEqual(self.src.aggregate_total_array_length(field)[0]["$project"]["_len"]["$size"], {"$ifNull": ["$test", []]})

    def test_aggregate_field_count(self):
        unwind_0 = {"$unwind": "$parameter"}
        self.assertEqual(self.src.aggregate_field_count(field="filler", unwind=unwind_0)[2], unwind_0)
        self.assertEqual(len(self.src.aggregate_field_count(field="filler")), 3)