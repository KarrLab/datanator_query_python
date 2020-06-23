from datanator_query_python.config import config
import unittest
import tempfile

class TestConfig(unittest.TestCase):
	
	@classmethod
	def setUpClass(cls):
		cls.src_basic = config.Config()
		cls.src_prod = config.ProductionConfig()
		cls.src_test = config.TestConfig()
		cls.src_ftx = config.FtxConfig()
		cls.src_justin = config.Justin()

	@classmethod
	def tearDownClass(cls):
		pass

	def test_basic_config(self):
		result = self.src_basic
		print(result.PRODUCTION)
		self.assertEqual(result.PRODUCTION != 'False', True)
		self.assertNotEqual(result.USERNAME, 'someusername')
		self.assertFalse(isinstance(self.src_basic.SERVER, list))
		
	def test_prod_config(self):
		result = self.src_prod
		self.assertEqual(result.PRODUCTION, True)

	def test_test_config(self):
		result = self.src_test
		self.assertTrue(result.MONGO_TEST_USERNAME is not None)
		self.assertTrue(result.MONGO_TEST_PASSWORD is not None)

	def test_ftx_config(self):
		result = self.src_ftx
		self.assertEqual(result.TEST_FTX_PROFILE_NAME, 'mock')

	def test_read_justin(self):
		self.assertEqual(self.src_justin.USERNAME, "justin")
		