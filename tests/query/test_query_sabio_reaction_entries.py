import unittest
import time
from datanator_query_python.query import query_sabio_reaction_entries
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
        cls.src = query_sabio_reaction_entries.QuerySabioRxn(
                cache_dirname=cls.cache_dirname, MongoDB=cls.MongoDB, db=cls.db,
                 verbose=True, max_entries=20, username=cls.username, password=cls.password)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dirname)
        cls.src.client.close()

    def test_get_ids_by_participant_inchikey(self):
        start = time.time()
        substrate_0 = 'XJLXINKUBYWONI-NNYOXOHSSA-N'
        substrate_1 = 'ODBLHEXUDAPZAU-UHFFFAOYSA-N'
        product_0 = 'GPRLSGONYQIRFK-UHFFFAOYSA-N'
        product_1 = 'KPGXRSRHYNQIFN-UHFFFAOYSA-N'
        result = self.src.get_ids_by_participant_inchikey([substrate_0, substrate_1], [product_0, product_1])
        self.assertTrue(7923 in result)
        substrate_2 = 'PQGCEDQWHSBAJP-TXICZTDVSA-I'
        substrate_3 = 'GFFGJBXGBJISGV-UHFFFAOYSA-N'
        product_2 = 'UDMBCSSLTHHNCD-KQYNXXCUSA-L'
        product_3 = 'XPPKVPWEQAFLFU-UHFFFAOYSA-K'
        result_1 = self.src.get_ids_by_participant_inchikey([substrate_2, substrate_3],
                                                            [product_2, product_3],
                                                            dof=1)
        self.assertTrue(15503 in result_1)
        finish = time.time()
        print('Time elapsed: {}s'.format(finish - start))