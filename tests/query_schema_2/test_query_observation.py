from datanator_query_python.query_schema_2 import query_observation
import unittest


class TestQOb(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        self.src = query_observation.QueryObs()