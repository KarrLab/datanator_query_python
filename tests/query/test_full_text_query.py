import unittest
import tempfile
import shutil
from datanator_query_python.query import full_text_search
from karr_lab_aws_manager.elasticsearch_kl import util as es_util
import time
import requests


class TestFTX(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.src = full_text_search.FTX(profile_name='es-poweruser', credential_path='~/.wc/third_party/aws_credentials',
                config_path='~/.wc/third_party/aws_config', elastic_path='~/.wc/third_party/elasticsearch.ini',
                cache_dir=cls.cache_dir, service_name='es', max_entries=float('inf'), verbose=True)
        cls.index_0 = 'test_0'
        cls.index_1 = 'test_1'
        cls.url_0 = cls.src.es_endpoint + '/' + cls.index_0
        cls.url_1 = cls.src.es_endpoint + '/' + cls.index_1
        cursor_0 = [{'number': 0, 'mock_key_bulk': 'mock_value_0', 'uniprot_id': 'P0'},
                  {'number': 1, 'mock_key_bulk': 'mock_value_1', 'uniprot_id': 'P1'},
                  {'number': 2, 'mock_key_bulk': 'mock_value_2', 'uniprot_id': 'P2'},
                  {'number': 3, 'mock_key_bulk': 'mock_value_3', 'uniprot_id': 'P3'}]
        cursor_1 = [{'number': 4, 'mock_key_bulk': 'mock_value_4', 'uniprot_id': 'P4'},
                  {'number': 5, 'mock_key_bulk': 'mock_value_5', 'uniprot_id': 'P5'},
                  {'number': 6, 'mock_key_bulk': 'mock_value_6', 'uniprot_id': 'P6'},
                  {'number': 7, 'mock_key_bulk': 'mock_value_7', 'uniprot_id': 'P7'},
                  {'number': 8, 'mock_key_bulk': 'mock_value_7', 'uniprot_id': 'P8'}]
        _ = cls.src.data_to_es_bulk(cursor_0, cls.index_0, bulk_size=1)
        _ = cls.src.data_to_es_bulk(cursor_1, cls.index_1, bulk_size=1)
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir)
        requests.delete(cls.url_0, auth=cls.src.awsauth)
        requests.delete(cls.url_1, auth=cls.src.awsauth)

    def test_request_body_search(self):
        query_0 = 'mock_value_3'
        field_0 = ['mock_key_bulk', 'number']
        body_0 = self.src.build_simple_query_string_body(query_0, fields=field_0, lenient=True,
        analyze_wild_card=True)
        index_0 = 'test_0,test_1'
        es = self.src._build_es()
        es_0 = es.search(index=index_0, body=body_0)
        self.assertEqual(es_0['hits']['hits'][0]['_source'], {'number': 3, 'mock_key_bulk': 'mock_value_3', 'uniprot_id': 'P3'})

    def test_simple_query_string(self):
        query_0 = 'mock_value_3'
        field_0 = ['mock_key_bulk', 'number']
        index_0 = 'test_0,test_1'
        r = self.src.simple_query_string(query_0, index_0, fields=field_0, lenient=True,
        analyze_wild_card=True)
        self.assertEqual(r['hits']['hits'][0]['_source'], {'number': 3, 'mock_key_bulk': 'mock_value_3', 'uniprot_id': 'P3'})

    def test_simple_query_string_real(self):
        query_0 = 'alcohol dehydrogenase'
        field_0 = ['protein_name', 'synonyms', 'enzymes', 'ko_name', 'gene_name']
        index_0 = 'ecmdb,ymdb,protein,metabolites_meta'
        r_0 = self.src.simple_query_string(query_0, index_0, fields=field_0, lenient=True,
        analyze_wild_card=True)
        r_1 = self.src.simple_query_string(query_0, index_0, fields=field_0, lenient=True,
        analyze_wild_card=True, from_=20)
        self.assertEqual(r_1['hits']['hits'][0]['_id'], 'Q66JJ3')
        self.assertEqual(r_0['hits']['hits'][0]['_id'], 'A1B4L2')

    def test_get_metabolites(self):
        query_0 = 'glucose'
        query_1 = 'somenonsense'
        fields_0 = ['protein_name', 'synonyms', 'enzymes', 'ko_name', 'gene_name', 'name',
                    'reaction_participant.substrate.substrate_name', 'reaction_participant.substrate.substrate_synonym',
                    'reaction_participant.product.product_name', 'reaction_participant.product.substrate_synonym',
                    'enzymes.enzyme.enzyme_name', 'enzymes.subunit.canonical_sequence']
        r_0 = self.src.get_metabolites(query_0, 'ecmdb,ymdb')
        self.assertEqual(len(r_0), 10)
        r_1 = self.src.get_metabolites(query_0, 'ecmdb,ymdb,protein,sabio_rk', fields=fields_0)
        self.assertEqual(len(r_1), 10)
        r_2 = self.src.get_metabolites(query_1, 'ecmdb,ymdb')
        self.assertEqual(r_2, [])