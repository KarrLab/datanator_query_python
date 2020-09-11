import unittest
import pymongo
from datanator_query_python.util import mongo_util
from datanator_query_python.config import config
import time
import tempfile
import shutil


class TestMongoUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dirname = tempfile.mkdtemp()
        cls.db = 'datanator'
        cls.duplicate = 'duplicate_test'
        conf = config.TestConfig()
        username = conf.USERNAME
        password = conf.PASSWORD
        MongoDB = conf.SERVER
        cls.src = mongo_util.MongoUtil(
            cache_dirname=cls.cache_dirname, MongoDB=MongoDB,
            db=cls.db, verbose=True, max_entries=20,
            username=username, password=password)
        cls.collection_str = 'ecmdb'
        cls.src_test = mongo_util.MongoUtil(
            cache_dirname=cls.cache_dirname, MongoDB=MongoDB,
            db='test', verbose=True, max_entries=20,
            username=username, password=password)
        docs = [{"name": "mike", "num": 0},
                {"name": "jon", "num": 1},
                {"name": "john", "num": 2},
                {"name": "mike", "num": 3}]
        cls.src_test.db_obj[cls.duplicate].insert_many(docs)
        cls.schema_test = "schema_test"
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.src_test.db_obj.drop_collection(cls.duplicate)
        cls.src_test.db_obj.drop_collection(cls.schema_test)
        shutil.rmtree(cls.cache_dirname)

    # @unittest.skip('passed')
    def test_list_all_collections(self):
        self.assertTrue('ecmdb' in self.src.list_all_collections())

    # @unittest.skip('passed')
    def test_con_db(self):
        client, db, col = self.src.con_db(self.db)
        self.assertNotEqual(self.src.con_db(self.db), 'Server not available')
        self.assertEqual(str(self.src.client.read_preference), 'Nearest(tag_sets=None, max_staleness=-1, hedge=None)')
        self.assertEqual(str(col.read_preference), 'Nearest(tag_sets=None, max_staleness=-1, hedge=None)')

    # @unittest.skip('passed')
    def test_print_schema(self):
        a = self.src.get_schema('ecmdb')
        self.assertEqual(a['properties']['creation_date'], {'type': 'string'})
        self.assertEqual(a['properties']['synonyms'],  {'type': 'object', 'properties': {'synonym': {'type': 'array', 
            'items': {'type': 'string'}}}, 'required': ['synonym']})

    def test_get_duplicates(self):
        num, results = self.src_test.get_duplicates(self.duplicate, "name")
        self.assertEqual(num, 1)

    @unittest.skip('duplicate removed')
    def test_get_duplicates_real(self):
        num, results = self.src.get_duplicates('taxon_tree', 'tax_id', allowDiskUse=True)
        self.assertEqual(num, 1)

    def test_define_schema(self):
        json_schema = "../datanator_pattern_design/compiled/taxon_compiled.json"
        self.src_test.define_schema(self.schema_test, json_schema)
        self.src_test.db_obj[self.schema_test].insert_one({"ncbi_taxonomy_id": 123, "name": "something",
                                                           "canon_ancestors": []})
        try:
            self.src_test.db_obj[self.schema_test].insert_one({"ncbi_taxonomy_id": "123", "name": "something",
                                                            "canon_ancestors": []})
        except pymongo.errors.WriteError as e:
            self.assertEqual(str(e), "Document failed validation, full error: {'index': 0, 'code': 121, 'errmsg': 'Document failed validation'}")

    def test_update_observation(self):
        self.src_test.update_observation({"identifier": {"namespace": "something", "value": "a"},
                                          "something": []},
                                         {"namespace": "something", "value": "a"},
                                         op="test")

    def test_update_entity(self):
        null = None
        obj = {
            "type": "protein",
            "name": "Oryza sativa",
            "synonyms": [],
            "identifiers": [
                {
                    "namespace": "uniprot",
                    "value": "Q75IW1"
                },
                {
                    "namespace": "gene_name_alt",
                    "value": null
                },
                {
                    "namespace": "gene_name_orf",
                    "value": "OsJ_11271 OSJNBb0059G13.19"
                },
                {
                    "namespace": "gene_name_oln",
                    "value": "Os03g0416300 LOC_Os03g30260"
                },
                {
                    "namespace": "pro_id",
                    "value": "PR:000024921"
                },
                {
                    "namespace": "pro_id",
                    "value": "PR:000036675"
                },
                {
                    "namespace": "entrez_id",
                    "value": "4333115"
                },
                {
                    "namespace": "entry_name",
                    "value": "COBL2_ORYSJ"
                }
            ],
            "related": [
                {
                    "namespace": "ec",
                    "value": null
                },
                {
                    "namespace": "gene_name",
                    "value": "BC1L2"
                },
                {
                    "namespace": "ko_name",
                    "value": null
                },
                {
                    "namespace": "ko_number",
                    "value": null
                }
            ],
            "genotype": {
                "taxon": {
                    "ncbi_taxonomy_id": 39947,
                    "obj": "Oryza sativa Japonica Group",
                    "canon_ancestors": [
                        {
                            "ncbi_taxonomy_id": 131567,
                            "name": "cellular organisms"
                        },
                        {
                            "ncbi_taxonomy_id": 2759,
                            "name": "Eukaryota"
                        },
                        {
                            "ncbi_taxonomy_id": 33090,
                            "name": "Viridiplantae"
                        },
                        {
                            "ncbi_taxonomy_id": 35493,
                            "name": "Streptophyta"
                        },
                        {
                            "ncbi_taxonomy_id": 4447,
                            "name": "Liliopsida"
                        },
                        {
                            "ncbi_taxonomy_id": 38820,
                            "name": "Poales"
                        },
                        {
                            "ncbi_taxonomy_id": 4479,
                            "name": "Poaceae"
                        },
                        {
                            "ncbi_taxonomy_id": 4527,
                            "name": "Oryza"
                        },
                        {
                            "ncbi_taxonomy_id": 4530,
                            "name": "Oryza sativa"
                        }
                    ]
                }
            },
            "structures": [
                {
                    "format": "canonical_sequence",
                    "value": "MARFLLGAAAIALLAGVSSLLLMVPFAEAYDPLDPNGNITIKWDITQWTPDGYVAVVTIYNFQKYRHIQAPGWSLGWAWAKKEIIWSMAGGQATEQGDCSAFKANIPHCCKRDPRVVDLVPGAPYNMQFGNCCKGGVLTSWVQDPLNAVASFQITVGHSGTSNKTVKAPKNFTLKAPGPGYSCGLAQEVKPPTRFISLDGRRTTQAHVTWNVTCTYSQFVAQRAPTCCVSLSSFYNETIVNCPKCACGCQNKKPGSCVEGNSPYLASVVNGPGKGSLTPLVQCTPHMCPIRVHWHVKLNYRDYWRVKVTITNWNYRMNYSQWNLVVQHPNFENVSTVFSFNYKSLNPYGVINDTAMMWGVKYYNDLLMVAGPDGNVQSELLFRKDRSTFTFDKGWAFPRRIYFNGESCVMPSPDLYPWLPPSSTPRFRTVFLLMSFLVCGTLAFLHNHLVLDKNCGKC"
                },
                {
                    "format": "processed_sequence_iubmb",
                    "value": null,
                    "molecular_weight": null,
                    "charge": null,
                    "formula": null,
                    "source": [
                        {
                            "namespace": "pro_id",
                            "value": "PR:000024921",
                            "level": "secondary"
                        },
                        {
                            "namespace": "doi",
                            "value": "10.1093/nar/gkw1075",
                            "level": "primary"
                        }
                    ]
                },
                {
                    "format": "modified_sequence_abbreviated_bpforms",
                    "value": "SERFPNDVDPIETRDWLQAIESVIREEGVERAQYLIDQLLAEARKGGVNVAAGTGISNYINTIPVEEQPEYPGNLELERRIRSAIRWNAIMTVLRASKKDLELGGHMASFQSSATIYDVCFNHFFRARNEQDGGDLVYFQGHISPGVYARAFLEGRLTQEQLDNFRQEVHGNGLSSYPHPKLMPEFWQFPTVSMGLGPIGAIYQAKFLKYLEHRGLKDTSKQTVYAFLGDGEMDEPESKGAITIATREKLDNLVFVINCNLQRLDGPVTGNGKIINELEGIFEGAGWNVIKVMWGSRWDELLRKDTSGKLIQLMNETVDGDYQTFKSKDGAYVREHFFGKYPETAALVADWTDEQIWALNRGGHDPKKIYAAFKKAQETKGKATVILAHTIKGYGMGDAAEGKNIAHQVKKMNMDGVRHIRDRFNVPVSDADIEKLPYITFPEGSEEHTYLHAQRQKLHGYLPSRQPNFTEKLELPSLQDFGALLEEQSKEISTTIAFVRALNVMLKNKSIKDRLVPIIADEARTFGMEGLFRQIGIYSPNGQQYTPQDREQVAYYKEDEKGQILQEGINELGAGCSWLAAATSYSTNNLPMIPFYIYYSMFGFQRIGDLCWAAGDQQARGFLIGGTSGRTTLNGEGLQHEDGHSHIQSLTIPNCISYDPAYAYEVAVIMHDGLERMYGEKQENVYYYITTLNENYHMPAMPEGAEEGIRKGIY{AA0055}LETIEGSKGKVQLLGSGSILRHVREAAEILAKDYGVGSDVYSVTSFTELARDGQDCERWNMLHPLETPRVPYIAQVMNDAPAVASTDYMKLFAEQVRTYVPADDYRVLGTDGFGRSDSRENLRHHFEVDASYVVVAALGELAKRGEIDKKVVADAIAKFNIDADKVNPRLA",
                    "molecular_weight": 97709.46800000001,
                    "charge": 97,
                    "formula": "C4438H6966N1217O1217S27",
                    "modification": {
                        "description": "K --> MOD:00064 (716)",
                        "formula": "C2HO",
                        "weight": 41.028999999994994,
                        "charge": -1
                    },
                    "source": [
                        {
                            "namespace": "pro_id",
                            "value": "PR:000024921",
                            "level": "secondary"
                        },
                        {
                            "namespace": "doi",
                            "value": "10.1093/nar/gkw1075",
                            "level": "primary"
                        }
                    ]
                },
                {
                    "format": "modified_sequence_bpforms",
                    "value": "SERFPNDVDPIETRDWLQAIESVIREEGVERAQYLIDQLLAEARKGGVNVAAGTGISNYINTIPVEEQPEYPGNLELERRIRSAIRWNAIMTVLRASKKDLELGGHMASFQSSATIYDVCFNHFFRARNEQDGGDLVYFQGHISPGVYARAFLEGRLTQEQLDNFRQEVHGNGLSSYPHPKLMPEFWQFPTVSMGLGPIGAIYQAKFLKYLEHRGLKDTSKQTVYAFLGDGEMDEPESKGAITIATREKLDNLVFVINCNLQRLDGPVTGNGKIINELEGIFEGAGWNVIKVMWGSRWDELLRKDTSGKLIQLMNETVDGDYQTFKSKDGAYVREHFFGKYPETAALVADWTDEQIWALNRGGHDPKKIYAAFKKAQETKGKATVILAHTIKGYGMGDAAEGKNIAHQVKKMNMDGVRHIRDRFNVPVSDADIEKLPYITFPEGSEEHTYLHAQRQKLHGYLPSRQPNFTEKLELPSLQDFGALLEEQSKEISTTIAFVRALNVMLKNKSIKDRLVPIIADEARTFGMEGLFRQIGIYSPNGQQYTPQDREQVAYYKEDEKGQILQEGINELGAGCSWLAAATSYSTNNLPMIPFYIYYSMFGFQRIGDLCWAAGDQQARGFLIGGTSGRTTLNGEGLQHEDGHSHIQSLTIPNCISYDPAYAYEVAVIMHDGLERMYGEKQENVYYYITTLNENYHMPAMPEGAEEGIRKGIY{AA0055}LETIEGSKGKVQLLGSGSILRHVREAAEILAKDYGVGSDVYSVTSFTELARDGQDCERWNMLHPLETPRVPYIAQVMNDAPAVASTDYMKLFAEQVRTYVPADDYRVLGTDGFGRSDSRENLRHHFEVDASYVVVAALGELAKRGEIDKKVVADAIAKFNIDADKVNPRLA"
                },
                {
                    "format": "processed_sequence_iubmb",
                    "value": null,
                    "molecular_weight": null,
                    "charge": null,
                    "formula": null,
                    "source": [
                        {
                            "namespace": "pro_id",
                            "value": "PR:000036675",
                            "level": "secondary"
                        },
                        {
                            "namespace": "doi",
                            "value": "10.1093/nar/gkw1075",
                            "level": "primary"
                        }
                    ]
                },
                {
                    "format": "modified_sequence_abbreviated_bpforms",
                    "value": "SERFPNDVDPIETRDWLQAIESVIREEGVERAQYLIDQLLAEARKGGVNVAAGTGISNYINTIPVEEQPEYPGNLELERRIRSAIRWNAIMTVLRASKKDLELGGHMASFQSSATIYDVCFNHFFRARNEQDGGDLVYFQGHISPGVYARAFLEGRLTQEQLDNFRQEVHGNGLSSYPHPKLMPEFWQFPTVSMGLGPIGAIYQAKFLKYLEHRGLKDTSKQTVYAFLGDGEMDEPESKGAITIATREKLDNLVFVINCNLQRLDGPVTGNGKIINELEGIFEGAGWNVIKVMWGSRWDELLRKDTSGKLIQLMNETVDGDYQTFKSKDGAYVREHFFGKYPETAALVADWTDEQIWALNRGGHDPKKIYAAFKKAQETKGKATVILAHTIKGYGMGDAAEGKNIAHQVKKMNMDGVRHIRDRFNVPVSDADIEKLPYITFPEGSEEHTYLHAQRQKLHGYLPSRQPNFTEKLELPSLQDFGALLEEQSKEISTTIAFVRALNVMLKNKSIKDRLVPIIADEARTFGMEGLFRQIGIYSPNGQQYTPQDREQVAYYKEDEKGQILQEGINELGAGCSWLAAATSYSTNNLPMIPFYIYYSMFGFQRIGDLCWAAGDQQARGFLIGGTSGRTTLNGEGLQHEDGHSHIQSLTIPNCISYDPAYAYEVAVIMHDGLERMYGEKQENVYYYITTLNENYHMPAMPEGAEEGIRKGIY{AA0055}LETIEGSKGKVQLLGSGSILRHVREAAEILAKDYGVGSDVYSVTSFTELARDGQDCERWNMLHPLETPRVPYIAQVMNDAPAVASTDYMKLFAEQVRTYVPADDYRVLGTDGFGRSDSRENLRHHFEVDASYVVVAALGELAKRGEIDKKVVADAIAKFNIDADKVNPRLA",
                    "molecular_weight": 97709.46800000001,
                    "charge": 97,
                    "formula": "C4438H6966N1217O1217S27",
                    "modification": {
                        "description": "K --> MOD:00064 (716)",
                        "formula": "C2HO",
                        "weight": 41.028999999994994,
                        "charge": -1
                    },
                    "source": [
                        {
                            "namespace": "pro_id",
                            "value": "PR:000036675",
                            "level": "secondary"
                        },
                        {
                            "namespace": "doi",
                            "value": "10.1093/nar/gkw1075",
                            "level": "primary"
                        }
                    ]
                },
                {
                    "format": "modified_sequence_bpforms",
                    "value": "SERFPNDVDPIETRDWLQAIESVIREEGVERAQYLIDQLLAEARKGGVNVAAGTGISNYINTIPVEEQPEYPGNLELERRIRSAIRWNAIMTVLRASKKDLELGGHMASFQSSATIYDVCFNHFFRARNEQDGGDLVYFQGHISPGVYARAFLEGRLTQEQLDNFRQEVHGNGLSSYPHPKLMPEFWQFPTVSMGLGPIGAIYQAKFLKYLEHRGLKDTSKQTVYAFLGDGEMDEPESKGAITIATREKLDNLVFVINCNLQRLDGPVTGNGKIINELEGIFEGAGWNVIKVMWGSRWDELLRKDTSGKLIQLMNETVDGDYQTFKSKDGAYVREHFFGKYPETAALVADWTDEQIWALNRGGHDPKKIYAAFKKAQETKGKATVILAHTIKGYGMGDAAEGKNIAHQVKKMNMDGVRHIRDRFNVPVSDADIEKLPYITFPEGSEEHTYLHAQRQKLHGYLPSRQPNFTEKLELPSLQDFGALLEEQSKEISTTIAFVRALNVMLKNKSIKDRLVPIIADEARTFGMEGLFRQIGIYSPNGQQYTPQDREQVAYYKEDEKGQILQEGINELGAGCSWLAAATSYSTNNLPMIPFYIYYSMFGFQRIGDLCWAAGDQQARGFLIGGTSGRTTLNGEGLQHEDGHSHIQSLTIPNCISYDPAYAYEVAVIMHDGLERMYGEKQENVYYYITTLNENYHMPAMPEGAEEGIRKGIY{AA0055}LETIEGSKGKVQLLGSGSILRHVREAAEILAKDYGVGSDVYSVTSFTELARDGQDCERWNMLHPLETPRVPYIAQVMNDAPAVASTDYMKLFAEQVRTYVPADDYRVLGTDGFGRSDSRENLRHHFEVDASYVVVAALGELAKRGEIDKKVVADAIAKFNIDADKVNPRLA"
                }
            ],
            "schema_version": "2.0"
        }
        x, y = self.src_test.update_entity(obj,
                                           {"namespace": "uniprot", "value": "Q75IW1"},
                                           op="test")
        print(y)

    def test_build_taxon_object(self):
        self.assertEqual(self.src.build_taxon_object(0), {})
        _id = 9606
        self.assertEqual(self.src.build_taxon_object(_id)["canon_ancestors"][0]["name"], "cellular organisms")