import unittest
from datanator_query_python.query import query_sabiork_old
import tempfile
import shutil
from datanator_query_python.config import config


class TestQuerySabioOld(unittest.TestCase):

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
        cls.src = query_sabiork_old.QuerySabioOld(
            cache_dirname=cls.cache_dirname, MongoDB=cls.MongoDB, db=cls.db,
                 verbose=True, max_entries=20, username = cls.username, password = cls.password)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)
        cls.src.client.close()

    # @unittest.skip('collection not yet finished building')
    def test_get_kinlaw_by_environment(self):
        taxon = [9606]
        taxon_wildtype = [True, False]
        ph_range = [6, 8]
        temp_range = [24, 26]
        name_space = {'ec-code': '3.4.21.62'}
        param_type = [25, 27]
        result, _ = self.src.get_kinlaw_by_environment(
            taxon, taxon_wildtype, ph_range, temp_range, name_space, param_type)
        self.assertTrue(sorted([i['kinlaw_id'] for i in result]), [47807, 47808, 47809])

        result, count = self.src.get_kinlaw_by_environment(
            [], taxon_wildtype, ph_range, temp_range, name_space, param_type)
        self.assertEqual(count, 66)

        result, count = self.src.get_kinlaw_by_environment(
            [], [True], ph_range, temp_range, name_space, param_type)
        self.assertEqual(count, 56)

        result, count = self.src.get_kinlaw_by_environment(
            taxon, [True], ph_range, temp_range, {}, param_type)
        self.assertEqual(count, 1305)

    def test_get_reaction_doc(self):
        _id = [31, 32]
        result, count = self.src.get_reaction_doc(_id)
        self.assertEqual(count, 2)
        self.assertTrue('kinlaw_id' in result[0])

    def test_get_kinlawid_by_rxn(self):
        substrate_0 = 'XJLXINKUBYWONI-NNYOXOHSSA-N'
        substrate_1 = 'ODBLHEXUDAPZAU-UHFFFAOYSA-N'
        product_0 = 'GPRLSGONYQIRFK-UHFFFAOYSA-N'
        product_1 = 'KPGXRSRHYNQIFN-UHFFFAOYSA-N'
        result = self.src.get_kinlawid_by_rxn([substrate_0, substrate_1], [product_0, product_1])
        self.assertTrue(7923 in result)
        substrate_2 = 'PQGCEDQWHSBAJP-TXICZTDVSA-I'
        substrate_3 = 'GFFGJBXGBJISGV-UHFFFAOYSA-N'
        product_2 = 'UDMBCSSLTHHNCD-KQYNXXCUSA-L'
        product_3 = 'XPPKVPWEQAFLFU-UHFFFAOYSA-K'
        result_1 = self.src.get_kinlawid_by_rxn([substrate_2, substrate_3],
                                                [product_2, product_3],
                                                dof=1)
        self.assertTrue(15503 in result_1)

    def test_get_kinlaw_by_rxn(self):
        substrate_0 = 'XJLXINKUBYWONI-NNYOXOHSSA-N'
        substrate_1 = 'ODBLHEXUDAPZAU-UHFFFAOYSA-N'
        product_0 = 'GPRLSGONYQIRFK-UHFFFAOYSA-N'
        product_1 = 'KPGXRSRHYNQIFN-UHFFFAOYSA-N'
        count, _ = self.src.get_kinlaw_by_rxn([substrate_0, substrate_1], [product_0, product_1])
        self.assertEqual(193, count)
        substrate_2 = 'PQGCEDQWHSBAJP-TXICZTDVSA-I'
        substrate_3 = 'GFFGJBXGBJISGV-UHFFFAOYSA-N'
        product_2 = 'UDMBCSSLTHHNCD-KQYNXXCUSA-L'
        product_3 = 'XPPKVPWEQAFLFU-UHFFFAOYSA-K'
        count_1, docs_1 = self.src.get_kinlaw_by_rxn([substrate_2, substrate_3],
                                                [product_2, product_3],
                                                dof=1)
        self.assertEqual(26, count_1)
        count_2, _ = self.src.get_kinlaw_by_rxn([substrate_0], [product_0, product_1], bound='loose')
        self.assertTrue(count_2 >= 193)
        count_3, _ = self.src.get_kinlaw_by_rxn([substrate_0], [product_0, product_1], bound='tight')
        self.assertEqual(count_3, 0)

    def test_get_kinlaw_by_entryid(self):
        entry_id_0 = 6593
        result_0 = self.src.get_kinlaw_by_entryid(entry_id_0)
        self.assertTrue(21 in result_0['kinlaw_id'])

    def test_get_info_by_entryid(self):
        entry_id_0 = 6690
        result_0 = self.src.get_info_by_entryid(entry_id_0)
        result_1 = self.src.get_info_by_entryid(entry_id_0, target_organism='homo sapiens')
        self.assertTrue(len(result_0) <= 10)
        self.assertEqual(result_1[0]['taxon_distance'], 8)
        entry_id_1 = 82
        result = self.src.get_info_by_entryid(entry_id_1)
        self.assertTrue(len(result) == 10)

    def test_get_kinlaw_by_rxn_name(self):
        substrate_name_0 = ['Riboflavin-5-phosphate', 'nonsense', '2-Hydroxypentanoate']
        product_name_0 = ['reduced FMN', 'alpha-Ketovaleric acid']
        _, docs_0 = self.src.get_kinlaw_by_rxn_name(substrate_name_0, product_name_0)
        count, _ = self.src.get_kinlaw_by_rxn_name(substrate_name_0, product_name_0, bound='tight')
        ids_0 = []
        for doc in docs_0:
            ids_0.append(doc['kinlaw_id'])
        self.assertTrue(41 in ids_0)
        self.assertEqual(0, count)

    def test_get_unique_reactions(self):
        result = self.src.get_unique_entries()
        self.assertEqual(60193, result)

    def test_get_unique_organisms(self):
        result = self.src.get_unique_organisms()
        self.assertEqual(983, result)

    def test_get_rxn_with_prm(self):
        kinlaw_ids = [48880, 48882, 48887, 48889, 42]
        result = self.src.get_rxn_with_prm(kinlaw_ids)
        self.assertEqual(len(result), 1)