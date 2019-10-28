from datanator_query_python.util import mongo_util, chem_util, file_util
from . import query_nosql
import json
import re


class QuerySabioOld(query_nosql.DataQuery):
    '''Queries specific to sabio_rk collection
    '''

    def __init__(self, cache_dirname=None, MongoDB=None, replicaSet=None, db='datanator',
                 collection_str='sabio_rk_old', verbose=False, max_entries=float('inf'), username=None,
                 password=None, authSource='admin', readPreference='nearest'):
        self.max_entries = max_entries
        super().__init__(cache_dirname=cache_dirname, MongoDB=MongoDB,
                        replicaSet=replicaSet, db=db,
                        verbose=verbose, max_entries=max_entries, username=username,
                        password=password, authSource=authSource, readPreference=readPreference)
        self.chem_manager = chem_util.ChemUtil()
        self.file_manager = file_util.FileUtil()
        self.client, self.db_obj, self.collection = self.con_db(collection_str)
        self.collection_str = collection_str

    def get_kinlaw_by_environment(self, taxon=None, taxon_wildtype=None, ph_range=None, temp_range=None,
                          name_space=None, param_type=None, projection={'_id': 0}):
        """get kinlaw info based on experimental conditions
        
        Args:
            taxon (:obj:`list`, optional): list of ncbi taxon id
            taxon_wildtype (:obj:`list` of :obj:`bool`, optional): True indicates wildtype and False indicates mutant
            ph_range (:obj:`list`, optional): range of pH
            temp_range (:obj:`list`, optional): range of temperature
            name_space (:obj:`dict`, optional): cross_reference key/value pair, i.e. {'ec-code': '3.4.21.62'}
            param_type (:obj:`list`, optional): possible values for parameters.type
            projection (:obj:`dict`, optional): mongodb query result projection

        Returns:
            (:obj:`tuple`) consisting of 
            docs (:obj:`list` of :obj:`dict`): list of docs;
            count (:obj:`int`): number of documents found 
        """
        all_constraints = []
        taxon_wildtype = [int(x) for x in taxon_wildtype]
        if taxon:
            all_constraints.append({'taxon_id': {'$in': taxon}})
        if taxon_wildtype:
            all_constraints.append({'taxon_wildtype': {'$in': taxon_wildtype}})
        if ph_range:
            all_constraints.append({'ph': {'$gte': ph_range[0], '$lte': ph_range[1]}})
        if temp_range:
            all_constraints.append({'temperature': {'$gte': temp_range[0], '$lte': temp_range[1]}})
        if name_space:
            key = list(name_space.keys())[0]
            val = list(name_space.values())[0]
            all_constraints.append({"resource": {'$elemMatch': {'namespace': key, 'id': val}}})
        if param_type:
            all_constraints.append({'parameter': {'$elemMatch': {'type': {'$in': param_type}}}})

        query = {'$and': all_constraints}
        docs = self.collection.find(filter=query, projection=projection)
        count = self.collection.count_documents(query)
        return docs, count

    def get_reaction_doc(self, kinlaw_id, projection={'_id': 0}):
        '''Find a document on reaction with the kinlaw_id
        Args:
            kinlaw_id (:obj:`list` of :obj:`int`) list of kinlaw_id to search for
            projection (:obj:`dict`): mongodb query result projection

        Returns:
            (:obj:`tuple`) consisting of 
            docs (:obj:`list` of :obj:`dict`): list of docs;
            count (:obj:`int`): number of documents found
        '''
        query = {'kinlaw_id': {'$in': kinlaw_id}}
        docs = self.collection.find(filter=query, projection=projection)
        count = self.collection.count_documents(query)
        return docs, count

    def get_kinlawid_by_rxn(self, substrates, products, dof=0):
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
        substrate = 'reaction_participant.substrate_aggregate'
        product = 'reaction_participant.product_aggregate'
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

        constraint_0 = {substrate: {'$all': substrates}}
        constraint_1 = {product: {'$all': products}}
        query = {'$and': [constraint_0, constraint_1]}
        docs = self.collection.find(filter=query, projection=projection)
        for doc in docs:
            result.append(doc['kinlaw_id'])
        return result

    def get_kinlaw_by_rxn(self, substrates, products, dof=0,
                          projection={'kinlaw_id': 1, '_id': 0}):
        ''' Find the kinlaw_id defined in sabio_rk using 
            rxn participants' inchikey

            Args:
                substrates (:obj:`list`): list of substrates' inchikey
                products (:obj:`list`): list of products' inchikey
                dof (:obj:`int`, optional): degree of freedom allowed (number of parts of
                                  inchikey to truncate); the default is 0
                projection (:obj:`dict`): pymongo query projection 

            Return:
                (:obj:`list` of :obj:`dict`): list of kinlaws that satisfy the condition
        '''
        result = []
        substrate = 'reaction_participant.substrate_aggregate'
        product = 'reaction_participant.product_aggregate'
        if dof == 0:
            substrates = substrates
            products = products
        elif dof == 1:
            substrates = [re.compile('^' + x[:-2]) for x in substrates]
            products = [re.compile('^' + x[:-2]) for x in products]
        else:
            substrates = [re.compile('^' + x[:14]) for x in substrates]
            products = [re.compile('^' + x[:14]) for x in products]

        constraint_0 = {substrate: {'$all': substrates}}
        constraint_1 = {product: {'$all': products}}
        query = {'$and': [constraint_0, constraint_1]}
        docs = self.collection.find(filter=query, projection=projection)
        count = self.collection.count_documents(query)
        return count, result