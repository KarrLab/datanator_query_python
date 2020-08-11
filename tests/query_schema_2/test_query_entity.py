from datanator_query_python.query_schema_2 import query_entity
import unittest


class TestQEn(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = query_entity.QueryEn()

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    def test_query_entity(self):
        _id = {"namespace": "inchikey", "value": "TYEYBOSBBBHJIV-UHFFFAOYSA-N"}
        r = self.src.query_entity(_id)
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["name"], "2-Ketobutyric acid")
        _id = {"namespace": "inchikey", "value": "234wgadgas"}
        r = self.src.query_entity(_id)
        self.assertEqual(r, [])