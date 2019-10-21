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
        result = self.src.get_kinlaw_by_environment(
            taxon, taxon_wildtype, ph_range, temp_range, name_space, observed_type)
        self.assertTrue(sorted([i['kinlaw_id'] for i in result]), [47807, 47808, 47809])
        
        result = self.src.get_kinlaw_by_environment(
            [], taxon_wildtype, ph_range, temp_range, name_space, observed_type)
        self.assertEqual(len(result), 37)
        
        result = self.src.get_kinlaw_by_environment(
            [], [True], ph_range, temp_range, name_space, observed_type)
        self.assertEqual(len(result), 34)

        result = self.src.get_kinlaw_by_environment(
            taxon, [True], ph_range, temp_range, {}, observed_type)
        self.assertEqual(len(result), 491)

    def test_get_reaction_doc(self):
        _id = [31, 32]
        result, count = self.src.get_reaction_doc(_id)
        self.assertEqual(count, 2)
        self.assertTrue('kinlaw_id' in result[0])