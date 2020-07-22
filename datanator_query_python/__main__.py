""" datanator_query_python command line interface

:Author: Name <email>
:Date: 2019-8-26
:Copyright: 2019, Karr Lab
:License: MIT
"""

import cement
from datanator_query_python.util import mongo_util
from datanator_query_python.config import config
import datanator_query_python


class BaseController(cement.Controller):
    """ Base controller for command line application """

    class Meta:
        label = 'base'
        description = "datanator_query_python"
        arguments = [
            (['-v', '--version'], dict(action='version',
                                       version=datanator_query_python.__version__)),
        ]

    @cement.ex(hide=True)
    def _default(self):
        self._parser.print_help()


class DefineSchema(cement.Controller):
    """Karrlab elasticsearch delete index. """

    class Meta:
        label = 'mongo-def-schema'
        description = 'Define jsonschema of a collection'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['db'], dict(
                type=str, help='Name of the database in which the collection resides.')),
            (['collection'], dict(
                type=str, help='Name of the collection to be defined.')),
            (['jsonschema'], dict(
                type=str, help='Location of jsonschema')),
            (['--config_name', '-cn'], dict(
                type=str, default='TestConfig',
                help='Config class to be used.'))
        ]

    @cement.ex(hide=True)
    def _default(self):
        ''' Delete elasticsearch index

            Args:
                index (:obj:`str`): name of index in es
                _id (:obj:`int`): id of the doc in index (optional)
        '''
        args = self.app.pargs
        conf = getattr(config, args.config_name)
        mongo_util.MongoUtil(MongoDB=conf.SERVER,
                             db=args.db,
                             username=conf.USERNAME,
                             password=conf.PASSWORD).define_schema(args.collection, args.jsonschema)
        print("done")


class App(cement.App):
    """ Command line application """
    class Meta:
        label = 'datanator_query_python'
        base_controller = 'base'
        handlers = [
            BaseController,
            DefineSchema
        ]


def main():
    with App() as app:
        app.run()

if __name__=='__main__':
    main()