from datanator_query_python.util import mongo_util
from pymongo.collation import Collation, CollationStrength
import re
from pymongo import ASCENDING, DESCENDING


class QuerySabioRxn(mongo_util.MongoUtil):
    '''Queries specific to sabio_reaction_entries collection
    '''

    def __init__(self, cache_dirname=None, MongoDB=None, replicaSet=None, db='datanator',
                 collection_str='sabio_reaction_entries', verbose=False, max_entries=float('inf'), username=None,
                 password=None, authSource='admin', readPreference='nearest'):
        self.max_entries = max_entries
        super().__init__(cache_dirname=cache_dirname, MongoDB=MongoDB,
                        replicaSet=replicaSet, db=db,
                        verbose=verbose, max_entries=max_entries, username=username,
                        password=password, authSource=authSource, readPreference=readPreference)
        self.collection = self.db_obj[collection_str]
        self.collection_str = collection_str
        self.collation = Collation(locale='en', strength=CollationStrength.SECONDARY)

    def get_ids_by_participant_inchikey(self, substrates, products, dof=1):
        ''' Find the kinlaw_id defined in sabio_rk using 
            rxn participants' inchikey

            Args:
                substrates (:obj:`list`): list of substrates' inchikey
                products (:obj:`list`): list of products' inchikey
                dof (:obj:`int`, optional): degree of freedom allowed (number of parts of
                                  inchikey to truncate); the default is 0 

            Return:
                rxns: list of kinlaw_ids that satisfy the condition
                [id0, id1, id2,...,  ]
        '''
        result = []
        bounded_s = {'$or': [{'substrates': {'$size': len(substrates)}}, {'substrates': {'$size': len(substrates) + 1}}]}
        bounded_p = {'$or': [{'products': {'$size': len(products)}}, {'products': {'$size': len(products) + 1}}]}
        projection = {'kinlaw_id': 1, '_id': 0}
        if dof == 0:
            substrates = substrates
            products = products
        elif dof == 1:
            substrates = [re.compile('^' + x[:-2]) for x in substrates]
            products = [re.compile('^' + x[:-2]) for x in products]
        else:
            substrates = [re.compile('^' + x[:14]) for x in substrates]
            products = [re.compile('^' + x[:14]) for x in products]

        constraint_0 = {'substrates': {'$all': substrates}}
        constraint_1 = {'products': {'$all': products}}
        query = {'$and': [constraint_0, constraint_1, bounded_s, bounded_p]}
        docs = self.collection.find(filter=query, projection=projection)
        count = self.collection.count_documents(query)
        if count == 0:
            return result
        elif count == 1:
            return docs[0]['kinlaw_id']
        else:
            for doc in docs:
                result += doc['kinlaw_id']
            return result
