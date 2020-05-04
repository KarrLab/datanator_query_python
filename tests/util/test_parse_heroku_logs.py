from datanator_query_python.util import parse_heroku_logs
import unittest
import shutil
import tempfile
from pathlib import Path


class TestParseLogs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._dir = tempfile.mkdtemp()
        cls.file_location = cls._dir + '/test_logs.txt'
        s_0 = ("2020-04-29T13:00:15.687982+00:00 heroku[router]:"
        "at=info method=GET path=\"/ftx/text_search/num_of_index/?index=metabolites_meta&query_message="
        "glucose&from_=10&size=10&fields=protein_name&fields=synonyms&fields=enzymes&fields=ko_name&fields="
        "gene_name&fields=name&fields=enzyme_name&fields=product_names&fields=substrate_names&fields="
        "enzymes.subunit.canonical_sequence&fields=species' host=api.datanator.info request_id="
        "9444dc4d-648b-427b-9cf1-1a5f235caacb fwd='23.20.143.235' dyno=web.1 connect=0ms service="
        "123ms status=200 bytes=418988 protocol=https\n")
        s_1 = ("2020-04-29T13:00:15.687982+00:00 heroku[router]:"
        "at=info method=GET path=\"/something/?index=metabolites_meta&query_message="
        "glucose&from_=10&size=10&fields=protein_name&fields=synonyms&fields=enzymes&fields=ko_name&fields="
        "gene_name&fields=name&fields=enzyme_name&fields=product_names&fields=substrate_names&fields="
        "enzymes.subunit.canonical_sequence&fields=species' host=api.datanator.info request_id="
        "9444dc4d-648b-427b-9cf1-1a5f235caacb fwd='23.20.143.235' dyno=web.1 connect=0ms service="
        "200ms status=200 bytes=418988 protocol=https\n")
        s_2 = ("2020-04-29T13:00:15.687982+00:00 heroku[router]:"
        "at=info method=GET path=\"/something/?index=metabolites_meta&query_message="
        "glucose&from_=10&size=10&fields=protein_name&fields=synonyms&fields=enzymes&fields=ko_name&fields="
        "gene_name&fields=name&fields=enzyme_name&fields=product_names&fields=substrate_names&fields="
        "enzymes.subunit.canonical_sequence&fields=species' host=api.datanator.info request_id="
        "9444dc4d-648b-427b-9cf1-1a5f235caacb fwd='23.20.143.235' dyno=web.1 connect=0ms service="
        "202ms status=200 bytes=418988 protocol=https\n")
        s_3 = ("what")
        with open(cls.file_location, 'w+') as f:
            f.writelines([s_0, s_1, s_2, s_3])    
        cls.src = parse_heroku_logs.ParseLogs(file_location=cls.file_location, verbose=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._dir)

    def test_parse_regex(self):
        s = ("2020-04-29T13:00:15.687982+00:00 heroku[router]:"
        "at=info method=GET path='/ftx/text_search/num_of_index/?index=metabolites_meta&query_message="
        "glucose&from_=10&size=10&fields=protein_name&fields=synonyms&fields=enzymes&fields=ko_name&fields="
        "gene_name&fields=name&fields=enzyme_name&fields=product_names&fields=substrate_names&fields="
        "enzymes.subunit.canonical_sequence&fields=species' host=api.datanator.info request_id="
        "9444dc4d-648b-427b-9cf1-1a5f235caacb fwd='23.20.143.235' dyno=web.1 connect=0ms service="
        "123ms status=200 bytes=418988 protocol=https")
        regex_0 = r'(\/.*\w+\/)'
        regex_1 = r'service=(\d*)ms'
        result_0 = self.src.parse_regex(s, regex_0)
        result_1 = self.src.parse_regex(s, regex_1)
        self.assertEqual(result_0.group(0), "/ftx/text_search/num_of_index/")
        self.assertEqual(result_1.group(1), "123")

    def test_parse_router(self):
        result = self.src.parse_router()
        self.assertEqual(result, {'/ftx/text_search/num_of_index/': [123], '/something/': [200, 202]})
        result = self.src.parse_router(lines=1)
        self.assertEqual(result, {'/ftx/text_search/num_of_index/': [123]})