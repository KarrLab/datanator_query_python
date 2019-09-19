import unittest
from datanator_query_python.query import query_metabolites
from datanator_query_python.config import config
import tempfile
import shutil


class TestQueryMetabolites(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.db = 'datanator'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.src = query_metabolites.QueryMetabolites(
            cache_dirname=cls.cache_dirname,
            MongoDB=MongoDB,
            db=cls.db,
            verbose=True,
            max_entries=20,
            username=username,
            password=password)

    @classmethod
    def tearDownClass(cls):
        cls.src.client_ecmdb.close()
        cls.src.client_ymdb.close()
        cls.src.metabolites_meta_manager.client.close()
        shutil.rmtree(cls.cache_dirname)

    def test_get_conc_from_inchi(self):
        inchi = '''1S/C10H16N5O13P3/c11-8-5-9(13-2-12-8)15(3-14-5)10-7(17)6(16)4(26-10)1-25-30(21,22)28-31(23,24)27-29(18,19)20/h2-4,6-7,10,16-17H,1H2,(H,21,22)(H,23,24)(H2,11,12,13)(H2,18,19,20)/t4-,6-,7-,10-/m1/s1'''
        result_0 = self.src.get_conc_from_inchi(inchi)
        self.assertEqual(len(result_0), 2)
        result_1 = self.src.get_conc_from_inchi(inchi, consensus=True)
        self.assertTrue('consensus_value' in result_1[0])
        inchi_1 = '1S/C8H11NO2/c9-4-3-6-1-2-7(10)8(11)5-6/h1-2,5,10-11H,3-4,9H2'
        result_2 = self.src.get_conc_from_inchi(inchi_1)
        self.assertEqual(len(result_2), 1)