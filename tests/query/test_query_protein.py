import unittest
from datanator_query_python.query import query_protein
from datanator_query_python.config import config
import pymongo
import time


class TestQueryProtein(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = 'test'
        conf = config.TestConfig()
        username = conf.USERNAME
        password = conf.PASSWORD
        MongoDB = conf.SERVER
        cls.MongoDB = MongoDB
        cls.username = username
        cls.password = password
        cls.src = query_protein.QueryProtein(server=cls.MongoDB, database=cls.db,
                 verbose=True, max_entries=20, username = cls.username,
                 password = cls.password, collection_str='test_query_protein', readPreference='primary')
        cls.src_1 = query_protein.QueryProtein(server=cls.MongoDB, database='datanator',
                 verbose=True, username = cls.username,
                 password = cls.password, readPreference='nearest')
        cls.src_2 = query_protein.QueryProtein(server=cls.MongoDB, database='datanator-test',
                 verbose=True, username = cls.username,
                 password = cls.password, readPreference='nearest')
        cls.src.db_obj.drop_collection('test_query_protein')

        mock_doc_0 = {'uniprot_id': 'MOCK_0', 'ancestor_taxon_id': [105,104,103,102,101],
                    'ancestor_name': ['name_5', 'name_4','name_3','name_2','name_1'],
                    'ko_number': 'MOCK_0', 'ncbi_taxonomy_id': 100, 'abundances': 0}

        mock_doc_1 = {'uniprot_id': 'MOCK_1', 'ko_number': 'MOCK_0'} # missing ancestor_taxon_id

        mock_doc_2 = {'uniprot_id': 'MOCK_2', 'ancestor_taxon_id': [105,104,103],
                    'ancestor_name': ['name_5', 'name_4','name_3'],
                    'ko_number': 'MOCK_0', 'ncbi_taxonomy_id': 102, 'abundances': 2}

        mock_doc_3 = {'uniprot_id': 'MOCK_3', 'ancestor_taxon_id': [105,104],
                    'ancestor_name': ['name_5', 'name_4'],
                    'ko_number': 'MOCK_1', 'ncbi_taxonomy_id': 103, 'abundances': 3} # different ko_number

        mock_doc_4 = {'uniprot_id': 'MOCK_4', 'ancestor_taxon_id': [105],
                    'ancestor_name': ['name_5'],
                    'ko_number': 'MOCK_0', 'ncbi_taxonomy_id': 104, 'abundances': 4}

        mock_doc_5 = {'uniprot_id': 'MOCK_5', 'ancestor_taxon_id': [105],
                    'ancestor_name': ['name_5'],
                    'ncbi_taxonomy_id': 104, 'abundances': 5}

        mock_doc_6 = {'uniprot_id': 'MOCK_6', 'ancestor_taxon_id': [105],
                    'ancestor_name': ['name_5'],
                    'ko_number': 'MOCK_0', 'ncbi_taxonomy_id': 104, 'abundances': 6}

        dic_0 = {'ncbi_taxonomy_id': 0, 'species_name': 's0', 'ancestor_taxon_id': [5,4,3,2,1], 'ancestor_name': ['s5', 's4', 's3', 's2', 's1'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot0', "protein_name": 'special name one', 'kinetics': [{'ncbi_taxonomy_id': 100, 'kinlaw_id': 1},
        {'ncbi_taxonomy_id': 101, 'kinlaw_id': 2}], 'abundances': [], 'ko_name': ['KO0 name']}
        dic_1 = {'ncbi_taxonomy_id': 1, 'species_name': 's1', 'ancestor_taxon_id': [5,4,3,2], 'ancestor_name': ['s5', 's4', 's3', 's2'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot1', "protein_name": 'nonspeciali name one'}
        dic_2 = {'ncbi_taxonomy_id': 2, 'species_name': 's2', 'ancestor_taxon_id': [5,4,3], 'ancestor_name': ['s5', 's4', 's3'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot2', "protein_name": 'nonspeciali name two'}
        dic_3 = {'ncbi_taxonomy_id': 3, 'species_name': 's3', 'ancestor_taxon_id': [5,4], 'ancestor_name': ['s5', 's4'], 'ko_number': 'ko3',
        'uniprot_id': 'uniprot3', "protein_name": 'your name one'}
        dic_4 = {'ncbi_taxonomy_id': 4, 'species_name': 's4', 'ancestor_taxon_id': [5], 'ancestor_name': ['s5'], 'ko_number': 'KO0', 'uniprot_id': 'uniprot4'}
        dic_5 = {'ncbi_taxonomy_id': 5, 'species_name': 's5', 'ancestor_taxon_id': [], 'ancestor_name': [], 'ko_number': 'KO0', 'uniprot_id': 'uniprot5'}
        dic_6 = {'ncbi_taxonomy_id': 6, 'species_name': 's6', 'ancestor_taxon_id': [5,4,3,2], 'ancestor_name': ['s5', 's4', 's3', 's2'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot6', "protein_name": 'your name two', 'ko_name': 'ko name 0', 'abundances': []}
        dic_15 = {'ncbi_taxonomy_id': 6, 'species_name': 's6', 'ancestor_taxon_id': [5,4,3,2], 'ancestor_name': ['s5', 's4', 's3', 's2'],
        'ko_number': 'KO1', 'uniprot_id': 'uniprot15', "protein_name": 'your name fifteen', 'ko_name': ['ko name 1']}
        dic_14 = {'ncbi_taxonomy_id': 14, 'species_name': 's6 something', 'ancestor_taxon_id': [5,4,3,2], 'ancestor_name': ['s5', 's4', 's3', 's2'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot6', "protein_name": 'your name three'}
        dic_7 = {'ncbi_taxonomy_id': 7, 'species_name': 's7', 'ancestor_taxon_id': [5,4,3,2,6], 'ancestor_name': ['s5', 's4', 's3', 's2', 's6'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot7', "protein_name": 'special name two'}
        dic_8 = {'ncbi_taxonomy_id': 8, 'species_name': 's8', 'ancestor_taxon_id': [5,4,3,2,6,7], 'ancestor_name': ['s5', 's4', 's3', 's2', 's6', 's7'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot8'}
        dic_9 = {'ncbi_taxonomy_id': 9, 'species_name': 's9', 'ancestor_taxon_id': [5,4,3], 'ancestor_name': ['s5', 's4', 's3'], 'ko_number': 'KO0',
        'uniprot_id': 'uniprot9'}
        dic_10 = {'ncbi_taxonomy_id': 10, 'species_name': 's10', 'ancestor_taxon_id': [5,4,3,9], 'ancestor_name': ['s5', 's4', 's3', 's9'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot10'}
        dic_11 = {'ncbi_taxonomy_id': 11, 'species_name': 's11', 'ancestor_taxon_id': [5,4,3,2,1,0], 'ancestor_name': ['s5', 's4', 's3', 's2', 's1', 's0'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot11'}
        dic_12 = {'ncbi_taxonomy_id': 12, 'species_name': 's12', 'ancestor_taxon_id': [5,4,3,2,1,0], 'ancestor_name': ['s5', 's4', 's3', 's2', 's1', 's0'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot12'}
        dic_13 = {'ncbi_taxonomy_id': 13, 'species_name': 's13', 'ancestor_taxon_id': [5,4,3,2,1], 'ancestor_name': ['s5', 's4', 's3', 's2', 's1'],
        'ko_number': 'KO0', 'uniprot_id': 'uniprot13', 'kinetics':[{'ncbi_taxonomy_id': 100, 'kinlaw_id': 1}, {'ncbi_taxonomy_id': 101, 'kinlaw_id': 2}]}
        dic_16 = {'ncbi_taxonomy_id': 6, 'species_name': 's6', 'ancestor_taxon_id': [5,4,3,2], 'ancestor_name': ['s5', 's4', 's3', 's2'],
        'ko_number': 'KO1', 'uniprot_id': 'uniprot16', "protein_name": 'your name fifteen'}

        cls.src.collection.insert_many([mock_doc_0, mock_doc_1, mock_doc_2, mock_doc_3, mock_doc_4,mock_doc_5,mock_doc_6])
        cls.src.collection.insert_many([dic_0,dic_1,dic_2,dic_3,dic_4,dic_5,dic_6,dic_7,dic_8,dic_9,dic_10,dic_11,dic_12,dic_13,dic_14,dic_15,dic_16])

        # cls.src.collection.create_index("uniprot_id", background=False, collation=cls.src.collation)
        # cls.src.collection.create_index([("protein_name", pymongo.TEXT)])

    @classmethod
    def tearDownClass(cls):
        cls.src.db_obj.drop_collection('test_query_protein')
        cls.src_1.client.close()
        cls.src_2.client.close()
        cls.src.client.close()

    def test_get_protein_meta(self):
        _id_0 = ['MOCK_0', 'MOCK_1']
        result_0 = self.src.get_meta_by_id(_id_0)
        _id_1 = ['asdfa']
        result_1 = self.src.get_meta_by_id(_id_1)
        self.assertEqual(result_0[0]['ko_number'], 'MOCK_0')
        self.assertEqual(result_0[1]['ko_number'], 'MOCK_0')
        self.assertTrue(isinstance(result_1, dict))

    @unittest.skip("takes too long.")
    def test_get_meta_by_name_taxon(self):
        name_0 = 'special name'
        taxon_id_0 = 0
        taxon_id_1 = 2432
        result_0 = self.src.get_meta_by_name_taxon(name_0, taxon_id_0)
        self.assertEqual(len(result_0), 1)
        result_1 = self.src.get_meta_by_name_taxon(name_0, taxon_id_1)
        self.assertEqual(result_1, [])

    @unittest.skip("takes too long.")
    def test_get_meta_by_name_name(self):
        species_name_0 = 'escherichia coli'
        protein_name_0 = 'phosphofructokinase'
        result_0 = self.src_1.get_meta_by_name_name(protein_name_0, species_name_0)
        self.assertEqual(len(result_0), 6)

    @unittest.skip("takes too long.")
    def test_get_info_by_text(self):
        name = 'special name'
        result = self.src.get_info_by_text(name)
        self.assertEqual(result[0]['ko_name'], ['no name'])

    @unittest.skip("takes too long.")
    def test_get_info_by_text_abundances(self):
        name = 'special name'
        result = self.src.get_info_by_text_abundances(name)
        self.assertEqual(result[0]['uniprot_ids'], {'uniprot7': False, 'uniprot0': True})
        name = 'Nucleoside diphosphate kinase'
        result = self.src_1.get_info_by_text_abundances(name)

    def test_get_info_by_taxonid(self):
        _id = 6
        result = self.src.get_info_by_taxonid(_id)
        time.sleep(0.5)
        self.assertEqual(result[1]['ko_name'], ['ko name 1'])

    def test_get_info_by_taxon_id_abundance(self):
        _id = 6
        results = self.src.get_info_by_taxonid_abundance(_id)
        time.sleep(0.5)
        self.assertEqual(results[0]['uniprot_ids'], {'uniprot6': True})

    def test_get_info_by_ko(self):
        ko = 'KO0'
        result = self.src.get_info_by_ko(ko)
        self.assertEqual(len(result[0]['uniprot_ids']), 14)

    def test_get_info_by_ko_abundance(self):
        ko = 'KO0'
        result = self.src.get_info_by_ko_abundance(ko)
        self.assertTrue(result[0]['uniprot_ids']['uniprot0'])

    @unittest.skip("passed")
    def test_get_id_by_name(self):
        name = 'special name'
        result = self.src.get_id_by_name(name)
        self.assertEqual(len(result), 2)

    def test_get_kinlaw_by_id(self):
        _id = ['uniprot12', 'uniprot13', 'nonsense']
        result_0 = self.src.get_kinlaw_by_id(_id)
        self.assertEqual(len(result_0), 2)
        self.assertEqual(result_0[0]['similar_functions'], None)
        self.assertEqual(len(result_0[1]['similar_functions']), 2)

    @unittest.skip("avoid building text index")
    def test_get_kinlaw_by_name(self):
        result_0 = self.src.get_kinlaw_by_name('special name one')
        self.assertEqual(result_0[0]['similar_functions'], [{'ncbi_taxonomy_id': 100, 'kinlaw_id': 1}, {'ncbi_taxonomy_id': 101, 'kinlaw_id': 2}])
        result_1 = self.src.get_kinlaw_by_name('uniprot12')
        self.assertEqual(result_1, [])

    @unittest.skip('passed')
    def test_get_abundance_by_id(self):
        _id_0 = ['MOCK_0']
        result_0 = self.src.get_abundance_by_id(_id_0)
        _id_1 = ['MOCK_0', 'MOCK_1']
        result_1 = self.src.get_abundance_by_id(_id_1)
        _id_2 = ['asdfafd', 'qewr']
        result_2 = self.src.get_abundance_by_id(_id_2)
        self.assertEqual(result_0, [{'uniprot_id': 'MOCK_0', 'abundances': 0}])
        self.assertEqual(result_1, [{'uniprot_id': 'MOCK_0', 'abundances': 0}])
        self.assertEqual(result_2, [{'abundances': [], 'uniprot_id': 'No proteins that match input',
                                    "species_name": "No proteins that match input"}])

    def test_get_proximity_abundance_taxon(self):
        result_0 = self.src.get_proximity_abundance_taxon('MOCK_0', max_distance=0)
        self.assertEqual('Please use get_abundance_by_id to check self abundance values', result_0)

        result_1 = self.src.get_proximity_abundance_taxon('MOCK_0', max_distance=2)
        self.assertEqual(result_1[0]['documents'], [])
        self.assertEqual(len(result_1[1]['documents']), 1)

        result_2 = self.src.get_proximity_abundance_taxon('MOCK_1', max_distance=1)
        self.assertEqual(result_2, 'This protein has no ancestor information to base upon')

        result_3 = self.src.get_proximity_abundance_taxon('MOCK_0', max_distance=3)
        self.assertEqual(result_3[2]['documents'], [])

    def test_get_equivalent_protein(self):

        result = self.src.get_equivalent_protein(['uniprot0'], 2, max_depth=2)
        self.assertEqual(len(result[1]['documents']), 1)
        result = self.src.get_equivalent_protein(['uniprot0'], 3, max_depth=2)
        self.assertEqual(len(result[2]['documents']), 0)

    def test_get_equivalent_protein_with_anchor(self):

        result = self.src.get_equivalent_protein_with_anchor('uniprot0', 2, max_depth=2)
        self.assertEqual(len(result[1]['documents']), 1)
        result = self.src.get_equivalent_protein_with_anchor('uniprot0',
                                                             3,
                                                             max_depth=2)
        self.assertEqual(len(result[2]['documents']), 0)

    def test_get_abundance_by_taxon(self):
        result = self.src.get_abundance_by_taxon(104)
        self.assertEqual(len(result), 3)

    def test_get_uniprot_by_ko(self):
        result_0 = self.src.get_uniprot_by_ko('MOCK_0')
        self.assertTrue('MOCK_2' in result_0)
        result_1 = self.src.get_uniprot_by_ko('somenonsense')
        self.assertEqual('No information available for this KO.', result_1)

    def test_get_abundance_with_same_ko(self):
        result_0 = self.src.get_abundance_with_same_ko('MOCK_0')
        self.assertEqual(len(result_0), 4)
        self.assertEqual(result_0[0]['ko_number'], 'MOCK_0')
        result_1 = self.src.get_abundance_with_same_ko('asfasf')
        self.assertEqual(result_1, 'No such protein in the database.')
        result_2 = self.src.get_abundance_with_same_ko('MOCK_5')
        self.assertEqual(result_2, 'No kegg information available for this protein.')

    def test_get_abundance_by_ko(self):
        result_0 = self.src.get_abundance_by_ko('MOCK_0')
        self.assertEqual(len(result_0), 4)
        result_1 = self.src.get_abundance_by_ko('asfasf')
        self.assertEqual(result_1, [])
        result_2 = self.src.get_abundance_by_ko('MOCK_5')
        self.assertEqual(result_2, [])

    def test_get_kegg_orthology(self):
        uniprot_id_0 = 'uniprot0'
        ko_number_0, ko_name_0 = self.src.get_kegg_orthology(uniprot_id_0)
        self.assertEqual(ko_name_0, ['KO0 name'])
        uniprot_id_1 = 'aldfja;lfj;'
        ko_number_1, ko_name_1 = self.src.get_kegg_orthology(uniprot_id_1)
        self.assertEqual(ko_number_1, None)
        self.assertEqual(ko_name_1, [])

    def test_get_equivalent_kegg_with_anchor_obsolete(self):
        result_0 = self.src_1.get_equivalent_kegg_with_anchor_obsolete('K03154','Thermus thermophilus HB27', 3, max_depth=2)
        self.assertTrue(len(result_0[0]['documents']) > 0)

    @unittest.skip('passed')
    def test_get_unique_protein(self):
        result = self.src_1.get_unique_protein()
        print(result)

    def test_get_unique_organism(self):
        result = self.src_1.get_unique_organism()
        print(result)

    @unittest.skip("skipping")
    def test_get_all_kegg(self):
        result_0 = self.src_1.get_all_kegg('K00850','Escherichia coli', 10)
        print(result_0)        

    # @unittest.skip("skipping")
    def test_get_all_ortho(self):
        # result_0 = self.src_2.get_all_ortho('494933at2759','Escherichia coli', 10)
        # print(result_0)
        result_1 = self.src_2.get_all_ortho("1555708at2", "Bacillus subtilis subsp. subtilis", 40)
        print(result_1)

    @unittest.skip("passed")
    def test_get_info_by_orthodb(self):
        result = self.src_2.get_info_by_orthodb("643917at2")
        print(result)

    def test_get_ortho_by_id(self):
        result = self.src_2.get_ortho_by_id("P53984")
        print(result)
