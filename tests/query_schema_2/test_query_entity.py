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
        print(self.src.query_entity(_id))