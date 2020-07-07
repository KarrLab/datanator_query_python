import unittest
import pymongo
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
        username = conf.USERNAME
        password = conf.PASSWORD
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
        cls.schema_test = "schema_test"
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.src_test.db_obj.drop_collection(cls.duplicate)
        cls.src_test.db_obj.drop_collection(cls.schema_test)
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
        self.assertEqual(num, 1)

    @unittest.skip('duplicate removed')
    def test_get_duplicates_real(self):
        num, results = self.src.get_duplicates('taxon_tree', 'tax_id', allowDiskUse=True)
        self.assertEqual(num, 1)

    def test_define_schema(self):
        json_schema = "../datanator_pattern_design/compiled/taxon_compiled.json"
        self.src_test.define_schema(self.schema_test, json_schema)
        self.src_test.db_obj[self.schema_test].insert_one({"ncbi_taxonomy_id": 123, "name": "something",
                                                           "canon_ancestors": []})
        try:
            self.src_test.db_obj[self.schema_test].insert_one({"ncbi_taxonomy_id": "123", "name": "something",
                                                            "canon_ancestors": []})
        except pymongo.errors.WriteError as e:
            self.assertEqual(str(e), "Document failed validation")

    def test_update_observation(self):
        entity_name = "test"
        entity_type = "test_type"
        entity_identifiers = [{"namespace": "test", "value": "test value"}]
        obs_identifier = {"namespace": "test", "value": "test value"}
        self.src_test.update_observation(entity_name,
                                         entity_type,
                                         entity_identifiers,
                                         obs_identifier,
                                         obs_source={"namespace": "something", "value": "12356"})