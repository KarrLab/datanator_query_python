import unittest
from datanator_query_python.query import query_metabolites_meta
from datanator_query_python.config import config
import tempfile
import shutil


class TestQueryMetabolitesMeta(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.db = 'datanator'
        conf = config.TestConfig()
        username = conf.USERNAME
        password = conf.PASSWORD
        MongoDB = conf.SERVER
        cls.src = query_metabolites_meta.QueryMetabolitesMeta(
            cache_dirname=cls.cache_dirname, MongoDB=MongoDB, db=cls.db,
                 verbose=True, max_entries=20, username = username, password = password)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)

    # @unittest.skip('passed')
    def test_get_metabolite_synonyms(self):
        c = "Isopropylglyoxylic acid"
        compounds = ["Isopropylglyoxylic acid", "3-OH-iso-but", '']
        ran = 'something'
        rxn, syn = self.src.get_metabolite_synonyms(c)
        rxn_ran, syn_ran = self.src.get_metabolite_synonyms(ran)
        rxns, syns = self.src.get_metabolite_synonyms(compounds)
        self.assertTrue("Ketovaline" in syn[c])
        self.assertTrue("3-OH-isobutyrate" in syns['3-OH-iso-but'])
        self.assertEqual(None, syns['synonyms'])
        self.assertTrue('does not exist' in syn_ran[ran])

        empty = ''
        rxn, syn = self.src.get_metabolite_synonyms(empty)
        self.assertEqual(syn, {'synonyms': None})

    # @unittest.skip('passed')
    def test_get_metabolite_inchi(self):
        compounds = ['Ketovaline']
        inchis = self.src.get_metabolite_inchi(compounds)
        self.assertEqual(inchis[0]['inchi'], 'InChI=1S/C5H8O3/c1-3(2)4(6)5(7)8/h3H,1-2H3,(H,7,8)')
        compound = ['3-Hydroxy-2-methylpropanoic acid']
        inchi = self.src.get_metabolite_inchi(compound)
        self.assertEqual(inchi[0]['m2m_id'], 'M2MDB006130')

    def test_get_ids_from_hash(self):
        hashed_inchi_1 = 'QHKABHOOEWYVLI-UHFFFAOYSA-N'
        hasehd_inchi_2 = 'YBJHBAHKTGYVGT-ZKWXMUAHSA-N'
        result_1 = self.src.get_ids_from_hash(hashed_inchi_1)
        result_2 = self.src.get_ids_from_hash(hasehd_inchi_2)
        self.assertEqual(result_1, {'m2m_id': 'M2MDB000606', 'ymdb_id': 'YMDB00365'})
        self.assertEqual(result_2, {'m2m_id': 'M2MDB000008', 'ymdb_id': 'YMDB00282'})

    def test_get_ids_from_hashes(self):
        hashed_inchi_0 = ['QHKABHOOEWYVLI-UHFFFAOYSA-N', 'YBJHBAHKTGYVGT-ZKWXMUAHSA-N', 'some_nonsense_0']
        hashed_inchi_1 = ['some_nonsense_1', 'some_nonsense_2', 'some_nonsense_3']
        result_0 = self.src.get_ids_from_hashes(hashed_inchi_0)
        print(result_0)
        result_1 = self.src.get_ids_from_hashes(hashed_inchi_1)
        self.assertEqual(len(result_0), 2)
        self.assertEqual(result_1, [])

    # @unittest.skip('passed')
    def test_get_metabolite_name_by_hash(self):
        compounds = ['QHKABHOOEWYVLI-UHFFFAOYSA-N',
                    'YBJHBAHKTGYVGT-ZKWXMUAHSA-N']
        result = self.src.get_metabolite_name_by_hash(compounds)
        self.assertEqual(result[0], 'Ketovaline')
        self.assertEqual(result[1], 'Vitamin-h')
        compound = ['TYEYBOSBBBHJIV-UHFFFAOYSA-N']
        result = self.src.get_metabolite_name_by_hash(compound)

    # @unittest.skip('passed')
    def test_get_metabolite_hashed_inchi(self):
        compounds = ['alpha-Ketoisovaleric acid', 'delta-Biotin factor S', 'Rovimix H 2']
        hashed_inchi = self.src.get_metabolite_hashed_inchi(compounds)
        self.assertEqual(hashed_inchi[1], 'YBJHBAHKTGYVGT-ZKWXMUAHSA-N')
        self.assertEqual(hashed_inchi[1], hashed_inchi[2])
        compound = ['3-Hydroxy-2-methylpropanoic acid']
        hashed_inchi = self.src.get_metabolite_hashed_inchi(compound)
        self.assertEqual(hashed_inchi, ['DBXBTMSZEOQQDU-VKHMYHEASA-N'])

    def test_get_unique_metabolites(self):
        result = self.src.get_unique_metabolites()
        self.assertTrue(isinstance(result, int))

    def test_get_metabolites_meta(self):
        self.assertEqual(self.src.get_metabolites_meta('lafj;aj'), {})
        self.assertEqual(self.src.get_metabolites_meta('QHKABHOOEWYVLI-UHFFFAOYSA-N')['chebi_id'], '16530')

    def test_get_eymdb(self):
        inchi_key = 'XCCTYIAWTASOJW-XVFCMESISA-N'
        self.assertEqual(self.src.get_eymeta(inchi_key)['m2m_id'], 'M2MDB000123')
        self.assertIsNone(self.src.get_eymeta('asdlfjalf'))
        
    def test_get_doc_by_name(self):
        names = ['Succinyl-CoA', 'succoa']
        self.assertEqual(self.src.get_doc_by_name(names)['kegg_id'], 'C00091')
        names = ['alpha-D-Ribose-5-phosphate']
        print(self.src.get_doc_by_name(names))