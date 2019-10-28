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

    def test_metabolite_manager(self):
        obj = self.src.metabolite_manager()
        self.assertTrue(obj.test_query_manager)

    def test_eymdb_manager(self):
        obj = self.src.eymdb_manager()
        self.assertTrue(obj.verbose)


class TestRxnManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = query_manager.RxnManager()

    def test_rxn_manager(self):
        obj = self.src.rxn_manager()
        self.assertTrue(obj.collection_str=='sabio_rk_old')