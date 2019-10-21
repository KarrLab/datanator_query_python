import unittest
from datanator_query_python.query import query_sabiork_old
import tempfile
import shutil
from datanator_query_python.config import config


class TestQuerySabioOld(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.db = 'datanator'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.MongoDB = MongoDB
        cls.username = username
        cls.password = password
        cls.src = query_sabiork_old.QuerySabioOld(
            cache_dirname=cls.cache_dirname, MongoDB=cls.MongoDB, db=cls.db,
                 verbose=True, max_entries=20, username = cls.username, password = cls.password)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)
        cls.src.client.close()

    @unittest.skip('collection not yet finished building')
    def test_get_kinlaw_by_environment(self):
        taxon = [9606]
        taxon_wildtype = [True, False]
        ph_range = [6, 8]
        temp_range = [24, 26]
        name_space = {'ec-code': '3.4.21.62'}
        observed_type = [25, 27]
        result, _ = self.src.get_kinlaw_by_environment(
            taxon, taxon_wildtype, ph_range, temp_range, name_space, observed_type)
        self.assertTrue(sorted([i['kinlaw_id'] for i in result]), [47807, 47808, 47809])
        
        result, count = self.src.get_kinlaw_by_environment(
            [], taxon_wildtype, ph_range, temp_range, name_space, observed_type)
        self.assertEqual(count, 37)
        
        result, count = self.src.get_kinlaw_by_environment(
            [], [True], ph_range, temp_range, name_space, observed_type)
        self.assertEqual(count, 34)

        result = self.src.get_kinlaw_by_environment(
            taxon, [True], ph_range, temp_range, {}, observed_type)
        self.assertEqual(len(result), 491)

    def test_get_reaction_doc(self):
        _id = [31, 32]
        result, count = self.src.get_reaction_doc(_id)
        self.assertEqual(count, 2)
        self.assertTrue('kinlaw_id' in result[0])

    def test_get_kinlawid_by_rxn(self):
        substrate_0 = 'InChI=1S/C21H28N7O17P3/c22-17-12-19(25-7-24-17)28(8-26-12)21-16(44-46(33,34)35)14(30)11(43-21)6-41-48(38,39)45-47(36,37)40-5-10-13(29)15(31)20(42-10)27-3-1-2-9(4-27)18(23)32/h1-4,7-8,10-11,13-16,20-21,29-31H,5-6H2,(H7-,22,23,24,25,32,33,34,35,36,37,38,39)/t10-,11-,13-,14-,15-,16-,20-,21-/m1/s1'
        substrate_1 = 'InChI=1S/C6H8O7/c7-3(8)1-2(5(10)11)4(9)6(12)13/h2,4,9H,1H2,(H,7,8)(H,10,11)(H,12,13)'
        product_0 = 'InChI=1S/C5H6O5/c6-3(5(9)10)1-2-4(7)8/h1-2H2,(H,7,8)(H,9,10)'
        product_1 = 'InChI=1S/CO2/c2-1-3'
        result = self.src.get_kinlawid_by_rxn([substrate_0, substrate_1], [product_0, product_1])
        self.assertTrue(7923 in result)