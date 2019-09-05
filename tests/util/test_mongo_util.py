import unittest
from datanator_query_python.util import mongo_util
from datanator_query_python.config import config
import tempfile
import shutil


class TestMongoUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.db = 'datanator'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.src = mongo_util.MongoUtil(
            cache_dirname = cls.cache_dirname, MongoDB = MongoDB,
            db = cls.db, verbose=True, max_entries=20,
            username = username, password = password)
        cls.collection_str = 'ecmdb'


    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)


    # @unittest.skip('passed')
    def test_list_all_collections(self):
        self.assertTrue('ecmdb' in self.src.list_all_collections())

    # @unittest.skip('passed')
    def test_con_db(self):
        client, db, col = self.src.con_db(self.db)
        self.assertNotEqual(self.src.con_db(self.db), 'Server not available')
        self.assertEqual(str(self.src.client.read_preference), 'Primary()')
        self.assertEqual(str(col.read_preference), 'Primary()')

    # @unittest.skip('passed')
    def test_print_schema(self):
        a = self.src.print_schema('ecmdb')
        self.assertEqual(a['properties']['creation_date'], {'type': 'string'})
        self.assertEqual(a['properties']['synonyms'],  {'type': 'object', 'properties': {'synonym': {'type': 'array', 
            'items': {'type': 'string'}}}, 'required': ['synonym']})


