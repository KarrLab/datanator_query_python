import unittest
from datanator_query_python.aggregate import tabu
from datanator_query_python.config import config


class TestTabu(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'datanator'
        conf = config.TestConfig()
        username = conf.USERNAME
        password = conf.PASSWORD
        MongoDB = conf.SERVER
        cls.src = tabu.Tabu(MongoDB=MongoDB, username=username, password=password,
                            db=db, max_entries=20, verbose=True, authSource='admin',
                            readPreference='nearest')

    @classmethod
    def tearDownClass(cls):
        cls.src.client.close()

    @unittest.skip("too long")
    def test_uniprot_taxon_dist(self):
        docs = self.src.uniprot_taxon_dist()
        for i, doc in enumerate(docs):
            if i == self.src.max_entries:
                break
            print(doc)

    def test_taxon_dist(self):
        # field = 'taxon_id'
        # collection = 'sabio_rk_old'
        # docs = self.src.taxon_dist(collection, field)
        # for i, doc in enumerate(docs):
        #     if i == self.src.max_entries:
        #         break
        #     print(doc)
        unwind = {"$unwind": "$concentrations"}
        field = 'concentrations.ncbi_taxonomy_id'
        collection = 'metabolite_concentrations'
        docs = self.src.taxon_dist(collection, field, unwind=unwind)
        for i, doc in enumerate(docs):
            if i == self.src.max_entries:
                break
            print(doc)

