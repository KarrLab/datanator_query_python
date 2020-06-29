""" datanator_query_python command line interface

:Author: Name <email>
:Date: 2019-8-26
:Copyright: 2019, Karr Lab
:License: MIT
"""

import cement
from datanator_query_python.util import mongo_util
from datanator_query_python.config import config
import datanator_query_python.core


class BaseController(cement.Controller):
    """ Base controller for command line application """

    class Meta:
        label = 'base'
        description = "datanator_query_python"
        arguments = [
            (['-v', '--version'], dict(action='version',
                                       version=datanator_query_python.__version__)),
        ]

    @cement.ex(help='command_1 description')
    def cmd1(self):
        """ command_1 description """
        print('command_1 output')

    @cement.ex(help='command_2 description')
    def cmd2(self):
        """ command_2 description """
        print('command_2 output')

    @cement.ex(hide=True)
    def _default(self):
        self._parser.print_help()


class Command3WithArgumentsController(cement.Controller):
    """ Command3 description """

    class Meta:
        label = 'command-3'
        description = 'Command3 description'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['arg_1'], dict(
                type=str, help='Description of arg_1')),
            (['arg_2'], dict(
                type=str, help='Description of arg_2')),
            (['--opt-arg-3'], dict(
                type=str, default='default value of opt-arg-1', help='Description of opt-arg-3')),
            (['--opt-arg-4'], dict(
                type=float, default=float('nan'), help='Description of opt-arg-4')),
        ]

    @cement.ex(hide=True)
    def _default(self):
        args = self.app.pargs
        args.arg_1
        args.arg_2
        args.opt_arg_3
        args.opt_arg_4


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
            Command3WithArgumentsController,
            DefineSchema
        ]


def main():
    with App() as app:
        app.run()
