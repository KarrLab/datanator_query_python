import unittest
from datanator_query_python.query import query_kegg_organism_code
from datanator_query_python.config import config


class TestKOC(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'datanator'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.src = query_kegg_organism_code.QueryKOC(username=username, password=password,
                                            server=MongoDB, database=db, collection_str='kegg_organism_code')

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    def test_get_org_code_by_ncbi(self):
        _id = 9606
        self.assertEqual(self.src.get_org_code_by_ncbi(_id), 'hsa')
        _id = 1234556
        self.assertEqual(self.src.get_org_code_by_ncbi(_id), 'No code found.')