from datanator_query.config import config
import unittest
import tempfile

class TestConfig(unittest.TestCase):
	
	@classmethod
	def setUpClass(cls):
		cls.src_basic = config.Config()
		cls.src_prod = config.ProductionConfig()

	@classmethod
	def tearDownClass(cls):
		pass

	def test_basic_config(self):
		result = self.src_basic
		self.assertEqual(result.PRODUCTION != 'False', False)
		self.assertNotEqual(result.USERNAME, 'someusername')
		
	def test_prod_config(self):
		result = self.src_prod
		self.assertEqual(result.PRODUCTION, True)