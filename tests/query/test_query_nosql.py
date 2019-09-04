import unittest
from datanator_query_python.query import query_nosql
from datanator_query_python.config import config
import tempfile
import shutil


class TestQueryNoSQL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.db = 'datanator'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.collection_str = 'ecmdb'
        cls.src = query_nosql.DataQuery(
            cache_dirname=cls.cache_dirname, MongoDB=MongoDB, replicaSet=None, db=cls.db,
                 verbose=True, max_entries=20, username = username, password = password)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)

    # @unittest.skip('skip to testing for h1_hesc')
    def test_doc_feeder(self):
        query = {'m2m_id': {
            '$in': ["M2MDB000004", "M2MDB000005", "M2MDB000006"]}}
        col = self.src.doc_feeder(self.collection_str, query=query)
        for doc in col:
            if doc['m2m_id'] == "M2MDB000005":
                self.assertEqual(doc['accession'], "ECMDB00023")
            elif doc['accession'] == "ECMDB00019":
                self.assertEqual(doc['m2m_id'], "M2MDB000004")