import unittest
from datanator_query_python.query import query_uniprot


class TestUniprot(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'datanator'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.src = query_uniprot.QueryUniprot(username=username, password=password,
                                            server=MongoDB, database=db)

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    def test_get_gene_name_by_locus(self):
        docs, count = self.src.get_gene_name_by_locus('BUAP5A_486')
        print(count)