from datanator_query_python.config import query_manager
import unittest
import tempfile
import botocore
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


class TestTaxonManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = query_manager.TaxonManager()

    def test_rxn_manager(self):
        obj = self.src.txn_manager()
        self.assertTrue(obj.collection_str=='taxon_tree')


class TestFtxManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = query_manager.FtxManager()

    def test_rxn_manager(self):
        obj = self.src.ftx_manager()
        self.assertEqual(obj._test, 'test')