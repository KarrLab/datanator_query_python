from datanator_query_python.util import mongo_util, file_util
from pymongo.collation import Collation, CollationStrength
import json


class QuerySabioCompound(mongo_util.MongoUtil):

    def __init__(self, username=None, password=None, server=None, authSource='admin',
                 database='datanator', max_entries=float('inf'), verbose=True, collection_str='sabio_compound',
                 readPreference='nearest', replicaSet=replicaSet):

        super().__init__(MongoDB=server,
                         db=database,
                         verbose=verbose, max_entries=max_entries, username=username,
                         password=password, authSource=authSource, readPreference=readPreference,
                         replicaSet=replicaSet)
        self.file_manager = file_util.FileUtil()
        self.max_entries = max_entries
        self.verbose = verbose
        self.db = self.db_obj
        self.collection = self.db[collection_str]
        self.collation = Collation(locale='en', strength=CollationStrength.SECONDARY)
        self.collection_str = collection_str

    def get_id_by_name(self, names):
        """Get sabio compound id given compound name
        
        Args:
            name (:obj:`list` of :obj:`str`): names of the compound

        Return:
            (:obj:`list` of :obj:`int`): sabio compound ids
        """
        result = []
        name_field = 'name'
        synonym_field = 'synonyms'
        pos_0 = {name_field: {'$in': names}}
        pos_1 = {synonym_field: {'$in': names}}
        query = {'$or': [pos_0, pos_1]}
        projection = {'_id': 1}
        docs = self.collection.find(filter=query, projection=projection, collation=self.collation)
        for doc in docs:
            result.append(doc['_id'])
        return result

    def get_inchikey_by_name(self, names):
        """Get compound InChIKey using compound names.
        
        Args:
            names (:obj:`list` of :obj:`str`): Names of compounds.

        Return:
            (:obj:`list` of :obj:`str`): List of inchikeys (not in the order of the input list).
        """
        result = []
        synonym_field = 'synonyms'
        pos_0 = {'name': {'$in': names}}
        pos_1 = {synonym_field: {'$in': names}}
        query = {'$or': [pos_0, pos_1]}
        projection = {'inchi_key': 1}        
        docs = self.collection.find(filter=query, projection=projection, collation=self.collation)
        if docs is None:
            return result
        else:
            for doc in docs:
                result.append(doc['inchi_key'])
            return result
