import unittest
from datanator_query_python.query import query_kegg_orthology
from datanator_query_python.util import file_util
from datanator_query_python.config import config
import tempfile
import shutil


class TestQueryKO(unittest.TestCase):

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
        cls.src = query_kegg_orthology.QueryKO(server=cls.MongoDB, database=cls.db,
                 verbose=True, max_entries=20, username = cls.username, password = cls.password)
        cls.file_manager = file_util.FileUtil()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)
        cls.src.client.close()

    def test_get_ko_by_name(self):
        result_0 = self.src.get_ko_by_name('gyar')
        self.assertEqual('K00015', result_0)
        result_1 = self.src.get_ko_by_name('gyaR')
        self.assertEqual(result_1, result_0)
        result_2 = self.src.get_ko_by_name('yuyyyyyy')
        self.assertEqual(None, result_2)
    
    def test_get_def_by_ko(self):
        result_0 = self.src.get_def_by_kegg_id('K00015')
        self.assertTrue('glyoxylate reductase' in result_0)
        result_1 = self.src.get_def_by_kegg_id('somenonsense')
        self.assertEqual([None], result_1)

    def test_get_gene_ortholog_by_id_org(self):
        _id = 'K00053'
        organism = 'ath'
        result = self.src.get_gene_ortholog_by_id_org(_id, organism)
        self.assertEqual(result, 'AT3G58610')