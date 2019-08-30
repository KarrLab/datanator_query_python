from datanator_query_python.config import query_manager
import unittest
import tempfile
import shutil
import os

class TestQueryManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.src = query_manager.Manager()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)

    def test_protein_manager(self):
        obj = self.src.protein_manager()
        self.assertTrue(obj.verbose)