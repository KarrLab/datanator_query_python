import unittest
from datanator_query_python.query import query_pax
from datanator_query_python.util import file_util
from datanator_query_python.config import config
import tempfile
import shutil


class TestQueryPax(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.db = 'datanator'
        conf = config.TestConfig()
        username = conf.USERNAME
        password = conf.PASSWORD
        MongoDB = conf.SERVER
        cls.MongoDB = MongoDB
        cls.username = username
        cls.password = password
        cls.file_manager = file_util.FileUtil()
        cls.src = query_pax.QueryPax(
            cache_dirname=cls.cache_dirname, MongoDB=cls.MongoDB, db=cls.db,
                 verbose=True, max_entries=20, username = cls.username, password = cls.password)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)
        cls.src.client.close()

    def test_get_all_species(self):
        result = self.src.get_all_species()
        self.assertTrue('Synechocystis.sp. 6803' in result)

    def test_get_abundance_from_uniprot(self):
        uniprot_id = 'F4KDK1'
        result = self.src.get_abundance_from_uniprot(uniprot_id)
        dic = self.file_manager.search_dict_list(result, 'abundance', '15.2')
        exp = [{'organ': 'COTYLEDON', 'abundance': '15.2'}]
        self.assertEqual(dic, exp)
        self.assertEqual({'ncbi_taxonomy_id': 3702, 'species_name': 'A.thaliana', 'ordered_locus_name': '3702.AT5G58200.2'}, result[0] )
        uniprot_id_1 = 'asdfasdf'
        result_1 = self.src.get_abundance_from_uniprot(uniprot_id_1)
        self.assertEqual(result_1, [])

    def test_get_file_by_name(self):
        name = '9606/9606-iPS_(DF19.11)_iTRAQ-114_Phanstiel_2011_gene.txt'
        result = self.src.get_file_by_name([name])
        self.assertEqual(
            result[0]['publication'],
            'http://www.nature.com/nmeth/journal/v8/n10/full/nmeth.1699.html')

    def test_get_file_by_ncbi_id(self):
        taxon = [9606]
        result = self.src.get_file_by_ncbi_id(taxon)    
        self.assertEqual(len(result), 170)

    def test_get_file_by_quality(self):
        organ_0 = 'CELL_LINE'
        tmp = []
        docs, count = self.src.get_file_by_quality(organ_0, score=0, coverage=0,projection={'_id': 0, 'observation': 0})
        for doc in docs:
            tmp.append(doc)
        self.assertEqual(len(tmp), count)
        self.assertEqual(count, 60)

    def test_get_file_by_publication(self):
        pub_0 = "http://www.nature.com/nmeth/journal/v8/n10/full/nmeth.1699.html"
        docs, count = self.src.get_file_by_publication(pub_0, projection={'_id': 0, 'observation': 0})
        tmp = []
        for doc in docs:
            tmp.append(doc)
        self.assertEqual(len(tmp), count)
        pub_1 = "http://www.mcponline.org/cgi/doi/10.1074/mcp.M111.014050"
        docs, count = self.src.get_file_by_publication(pub_1, projection={'_id': 0, 'observation': 0})
        tmp = []
        for doc in docs:
            tmp.append(doc)
        self.assertEqual(len(tmp), count)

    def test_get_file_by_organ(self):
        organ_0 = 'CELL_LINE'
        docs, count = self.src.get_file_by_organ(organ_0, projection={'_id': 0, 'observation': 0})
        tmp = []
        for doc in docs:
            tmp.append(doc)
        self.assertEqual(len(tmp), count)        