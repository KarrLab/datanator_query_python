import unittest
from datanator_query_python.query import query_metabolite_concentrations
from datanator_query_python.config import config


class TestQMC(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.src = query_metabolite_concentrations.QueryMetaboliteConcentrations(MongoDB=MongoDB,
        password=password, username=username, db='datanator', collection_str='metabolite_concentrations')

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    def test_get_similar_concentrations(self):
        self.assertEqual(self.src.get_similar_concentrations('something'), [])
        result = self.src.get_similar_concentrations('GRSZFWQUAKGDAV-KQYNXXCUSA-N')
        self.assertTrue(result[0]['similarity_score'] >= result[1]['similarity_score'])

    def test_get_conc_count(self):
        self.assertEqual(self.src.get_conc_count(), 3841)