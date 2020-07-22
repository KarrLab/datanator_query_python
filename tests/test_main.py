""" Tests of datanator_query_python command line interface (datanator_query_python.__main__)

:Author: Name <email>
:Date: 2019-8-26
:Copyright: 2019, Karr Lab
:License: MIT
"""

from datanator_query_python import __main__
from datanator_query_python.util import mongo_util
from datanator_query_python.config import config
import datanator_query_python
import capturer
import mock
import unittest


class CliTestCase(unittest.TestCase):

    def test_cli(self):
        with mock.patch('sys.argv', ['datanator_query_python', '--help']):
            with self.assertRaises(SystemExit) as context:
                __main__.main()
                self.assertRegex(context.Exception,
                                 'usage: datanator_query_python')

    def test_help(self):
        with self.assertRaises(SystemExit):
            with __main__.App(argv=['--help']) as app:
                app.run()

    def test_version(self):
        with __main__.App(argv=['-v']) as app:
            with capturer.CaptureOutput(merged=False, relay=False) as captured:
                with self.assertRaises(SystemExit):
                    app.run()
                self.assertEqual(captured.stdout.get_text(),
                                 datanator_query_python.__version__)
                self.assertEqual(captured.stderr.get_text(), '')

        with __main__.App(argv=['--version']) as app:
            with capturer.CaptureOutput(merged=False, relay=False) as captured:
                with self.assertRaises(SystemExit):
                    app.run()
                self.assertEqual(captured.stdout.get_text(),
                                 datanator_query_python.__version__)
                self.assertEqual(captured.stderr.get_text(), '')

    def test_define_schema(self):
        with capturer.CaptureOutput(merged=False, relay=False) as captured:
            with __main__.App(argv=['mongo-def-schema',
                                    'test',
                                    'cli_test',
                                    '../datanator_pattern_design/compiled/taxon_compiled.json']) as app:
                # run app
                app.run()

                # test that the arguments to the CLI were correctly parsed
                self.assertEqual(app.pargs.db, 'test')
                self.assertTrue(app.pargs.collection, 'cli_test')

                # test that the CLI produced the correct output
                self.assertEqual(captured.stdout.get_text(), 'done')
                self.assertEqual(captured.stderr.get_text(), '')
        conf = getattr(config, app.pargs.config_name)
        mongo_util.MongoUtil(MongoDB=conf.SERVER,
                            db=app.pargs.db,
                            username=conf.USERNAME,
                            password=conf.PASSWORD).db_obj.drop_collection(app.pargs.collection)