import unittest
from datanator_query_python.query import query_sabiork
import tempfile
import shutil
from datanator_query_python.config import config


class TestQuerySabio(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.db = 'datanator'
        conf = config.TestConfig()
        username = conf.MONGO_TEST_USERNAME
        password = conf.MONGO_TEST_PASSWORD
        MongoDB = conf.SERVER
        cls.MongoDB = MongoDB
        cls.username = username
        cls.password = password
        cls.src = query_sabiork.QuerySabio(
            cache_dirname=cls.cache_dirname, MongoDB=cls.MongoDB, db=cls.db,
                 verbose=True, max_entries=20, username = cls.username, password = cls.password)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)
    
    def test_get_reaction_doc(self):
        _id = [31, 32]
        result = self.src.get_reaction_doc(_id)
        self.assertEqual(len(result), 2)
        self.assertTrue('kinlaw_id' in result[0])

    # @unittest.skip('passed')
    def test_find_reaction_participants(self):
        _id = [31, 32, 33, 34]
        rxns = self.src.find_reaction_participants(_id)
        self.assertEqual(rxns[0], {'substrates': ["2-Hydroxybutyrate", "Riboflavin-5-phosphate"], 
                        'products': ['2-Oxobutyrate', 'Reduced FMN']})

        self.assertEqual(rxns[3], {'substrates': ['Riboflavin-5-phosphate', '4-Chloromandelate'],
                                    'products': ['Reduced FMN', '4-Chloro-2-Oxobenzeneacetic acid'] } )

    # @unittest.skip('passed')
    def test_get_kinlawid_by_inchi(self):
        inchi = ['IWYDHOAUDWTVEP-ZETCQYMHSA-N',
                'FVTCRASFADXXNN-SCRDCRAPSA-N',
                'FAQJJMHZNSSFSM-UHFFFAOYSA-M']
        rxn = self.src.get_kinlawid_by_inchi(inchi)
        self.assertTrue(9 in rxn)
        self.assertTrue(21016 in rxn)

    # @unittest.skip('passed')
    def test_get_kinlawid_by_rxn(self):
        substrates = ['IWYDHOAUDWTVEP-ZETCQYMHSA-N',
                    'FVTCRASFADXXNN-SCRDCRAPSA-N']
        products = ['FAQJJMHZNSSFSM-UHFFFAOYSA-M']
        _id = self.src.get_kinlawid_by_rxn(substrates,products)
        self.assertTrue(21016 in _id)

    def test_get_kinlawid_by_name(self):
        substrates = ["2-Hydroxybutyrate", "Riboflavin-5-phosphate"]
        products = ['2-Oxobutyrate', 'Reduced FMN']
        result = self.src.get_kinlawid_by_name(substrates, products)
        self.assertTrue(31 in result)
        self.assertTrue(33 not in result)
        substrates_1 = ["2-Hydroxybutyrate", "Riboflavin-5-phosphate"]
        products_1 = None
        result_1 = self.src.get_kinlawid_by_name(substrates_1, products_1)
        self.assertTrue(31 in result_1)
        self.assertTrue(len(result) <= len(result_1))

    def test_get_kinlaw_by_environment(self):
        ph_range = [6, 8]
        temp_range = [24, 26]
        name_space = {'ec-code': '3.4.21.62'}
        observed_type = [25, 27]
        result = self.src.get_kinlaw_by_environment(ph_range, temp_range, name_space, observed_type)
        self.assertTrue(len(result) == 66)