import unittest
from datanator_query.util import mongo_util
import tempfile
import shutil
import configparser
import os

class TestMongoUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.db = 'datanator'
        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read(os.path.expanduser('~/.wc/datanator.ini'))
        username = parser.get('mongodb', 'user')
        password = parser.get('mongodb', 'password')
        MongoDB = parser.get('mongodb', 'server')
        port = int(parser.get('mongodb', 'port'))
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
        self.assertNotEqual(self.src.con_db(self.db), 'Server not available')

    # @unittest.skip('passed')
    def test_print_schema(self):
        a = self.src.print_schema('ecmdb')
        self.assertEqual(a['properties']['creation_date'], {'type': 'string'})
        self.assertEqual(a['properties']['synonyms'],  {'type': 'object', 'properties': {'synonym': {'type': 'array', 
            'items': {'type': 'string'}}}, 'required': ['synonym']})


