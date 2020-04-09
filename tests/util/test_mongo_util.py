import unittest
from datanator_query_python.util import mongo_util
from datanator_query_python.config import config
import time
import tempfile
import shutil


class TestMongoUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.db = 'datanator'
        cls.duplicate = 'duplicate_test'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.src = mongo_util.MongoUtil(
            cache_dirname=cls.cache_dirname, MongoDB=MongoDB,
            db=cls.db, verbose=True, max_entries=20,
            username=username, password=password)
        cls.collection_str = 'ecmdb'
        cls.src_test = mongo_util.MongoUtil(
            cache_dirname=cls.cache_dirname, MongoDB=MongoDB,
            db='test', verbose=True, max_entries=20,
            username=username, password=password)
        docs = [{"name": "mike", "num": 0},
                {"name": "jon", "num": 1},
                {"name": "john", "num": 2},
                {"name": "mike", "num": 3}]
        cls.src_test.db_obj[cls.duplicate].insert_many(docs)
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.src_test.db_obj.drop_collection(cls.duplicate)
        shutil.rmtree(cls.cache_dirname)

    # @unittest.skip('passed')
    def test_list_all_collections(self):
        self.assertTrue('ecmdb' in self.src.list_all_collections())

    # @unittest.skip('passed')
    def test_con_db(self):
        client, db, col = self.src.con_db(self.db)
        self.assertNotEqual(self.src.con_db(self.db), 'Server not available')
        self.assertEqual(str(self.src.client.read_preference), 'Nearest(tag_sets=None, max_staleness=-1)')
        self.assertEqual(str(col.read_preference), 'Nearest(tag_sets=None, max_staleness=-1)')

    # @unittest.skip('passed')
    def test_print_schema(self):
        a = self.src.get_schema('ecmdb')
        self.assertEqual(a['properties']['creation_date'], {'type': 'string'})
        self.assertEqual(a['properties']['synonyms'],  {'type': 'object', 'properties': {'synonym': {'type': 'array', 
            'items': {'type': 'string'}}}, 'required': ['synonym']})

    def test_get_duplicates(self):
        num, results = self.src_test.get_duplicates(self.duplicate, "name")
        self.assertEqual(num, 2)
