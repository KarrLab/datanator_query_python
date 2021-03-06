from datanator_query_python.util import mongo_util, chem_util, file_util
import numpy as np
from pymongo.collation import Collation, CollationStrength


class QueryMetabolitesMeta(mongo_util.MongoUtil):
    '''Queries specific to metabolites_meta collection
    '''

    def __init__(self, cache_dirname=None, MongoDB=None, replicaSet=None, db=None,
                 collection_str='metabolites_meta', verbose=False, max_entries=float('inf'), username=None,
                 password=None, authSource='admin', readPreference='nearest'):
        self._collection_str = collection_str
        self.verbose = verbose
        super().__init__(cache_dirname=cache_dirname, MongoDB=MongoDB,
                        replicaSet=replicaSet, db=db,
                        verbose=verbose, max_entries=max_entries, username=username,
                        password=password, authSource=authSource,
                        readPreference=readPreference)
        self._collection = self.client.get_database("datanator-test")[collection_str]
        self.e_client, self.e_db_obj, self.e_collection = self.con_db('ecmdb')
        self.y_client, self.y_db_obj, self.y_collection = self.con_db('ymdb')        
        self.file_manager = file_util.FileUtil()
        self.chem_manager = chem_util.ChemUtil()
        self.collation = Collation(locale='en', strength=CollationStrength.SECONDARY)

    def get_metabolite_synonyms(self, compounds):
        ''' Find synonyms of a compound

            Args:
                compound (list): name(s) of the compound e.g. "ATP", ["ATP", "Oxygen", ...]

            Returns:
                synonyms: dictionary of synonyms of the compounds
                        {'ATP': [], 'Oxygen': [], ...}
                rxns: dictionary of rxns in which each compound is found
                    {'ATP': [12345,45678,...], 'Oxygen': [...], ...}
        '''
        synonyms = {}
        rxns = {}

        def find_synonyms_of_str(c):
            if len(c) != 0:
                query = {'synonyms': c}
                projection = {'synonyms': 1, '_id': -1, 'kinlaw_id': 1}
                collation = {'locale': 'en', 'strength': 2}
                doc = self._collection.find_one(
                    filter=query, projection=projection, collation=collation)
                synonym = {}
                rxn = {}
                try:
                    synonym[c] = doc['synonyms']
                    rxn[c] = doc['kinlaw_id']
                except TypeError as e:
                    synonym[c] = (c + ' does not exist in ' +
                                  self._collection_str)
                    rxn[c] = (c + ' does not exist in ' + self._collection_str)
                return rxn, synonym
            else:
                return ({'reactions': None}, {'synonyms': None})

        if len(compounds) == 0:
            return ({'reactions': None}, {'synonyms': None})
        elif isinstance(compounds, str):
            rxn, syn = find_synonyms_of_str(compounds)
            synonyms.update(syn)
            rxns.update(rxn)
        else:
            for c in compounds:
                rxn, syn = find_synonyms_of_str(c)
                synonyms.update(syn)
                rxns.update(rxn)
        return rxns, synonyms

    def get_metabolite_inchi(self, compounds):
        '''Given a list of compound name(s) Return the corrensponding inchi string

            Args:
                compounds: list of compounds
                ['ATP', '2-Ketobutanoate']

            Returns:
                ['....', 'InChI=1S/C4H6O3/c1-2-3(5)4(6)7/...']
        '''
        inchi = []
        projection = {'_id': 0, 'inchi': 1, 'm2m_id': 1, 'ymdb_id': 1}
        collation = {'locale': 'en', 'strength': 2}
        for compound in compounds:
            cursor = self._collection.find_one({'$or': [{'synonyms': compound},
                                                       {'name': compound}]},
                                              projection=projection, collation=collation)
            if cursor is None:
                inchi.append(
                    {"inchi": 'No inchi found.', "m2m_id": 'No ECMDB record found.',
                    "ymdb_id": 'No YMDB record found.'})
            else:                
                inchi.append(
                    {"inchi": cursor['inchi'], "m2m_id": cursor.get('m2m_id', None),
                    "ymdb_id": cursor.get('ymdb_id', None)})
        return inchi

    def get_ids_from_hash(self, hashed_inchi):
        ''' Given a hashed inchi string, find its
            corresponding m2m_id and/or ymdb_id
            Args:
                hashed_inchi (`obj`: str): string of hashed inchi
            Returns:
                result (`obj`: dict): dictionary of ids and their keys
                    {'m2m_id': ..., 'ymdb_id': ...}
        '''
        query = {'InChI_Key': hashed_inchi}
        projection = {'_id': 0}
        doc = self._collection.find_one(filter=query, projection=projection)
        result = {}
        result['m2m_id'] = doc.get('m2m_id', None)
        result['ymdb_id'] = doc.get('ymdb_id', None)

        return result

    def get_ids_from_hashes(self, hashed_inchi):
        ''' Given a list of hashed inchi string, find their
            corresponding m2m_id and/or ymdb_id
            Args:
                hashed_inchi (`obj`: list of `obj`: str): list of hashed inchi
            Returns:
                result (`obj`: list of `obj`: dict): dictionary of ids and their keys
                    [{'m2m_id': ..., 'ymdb_id': ..., 'InChI_Key': ...}, {}, ..]
        '''
        query = {'InChI_Key': {'$in': hashed_inchi}}
        projection = {'m2m_id': 1, 'ymdb_id': 1, 'InChI_Key': 1}
        docs = self._collection.find(filter=query, projection=projection)
        result = []
        if docs is None: 
            return result
        else:
            for doc in docs:
                dic = {}
                dic['m2m_id'] = doc.get('m2m_id', None)
                dic['ymdb_id'] = doc.get('ymdb_id', None)
                dic['InChI_Key'] = doc.get('InChI_Key', None)
                result.append(dic)
            return result

    def get_metabolite_hashed_inchi(self, compounds):
        ''' Given a list of compound name(s)
            Return the corresponding hashed inchi string
            Args:
                compounds: ['ATP', '2-Ketobutanoate']
            Return:
                hashed_inchi: ['3e23df....', '7666ffa....']
        '''
        hashed_inchi = []
        projection = {'_id': 0, 'InChI_Key': 1}
        collation = {'locale': 'en', 'strength': 2}
        for compound in compounds:
            cursor = self._collection.find_one({'$or': [{'synonyms': compound},
                                                        {'name': compound}]},
                                              projection=projection, collation=collation)
            if cursor is None:
                hashed_inchi.append('No inchi key found.')
            else:
                hashed_inchi.append(cursor['InChI_Key'])
        return hashed_inchi

    def get_metabolite_name_by_hash(self, compounds):
        ''' Given a list of hashed inchi, 
            return a list of name (one of the synonyms)
            for each compound
            Args:
                compounds: list of compounds in inchikey format
            Return:
                result: list of names
                    [name, name, name]
        '''
        result = []
        projection = {'_id': 0, 'synonyms': 1}
        collation = {'locale': 'en', 'strength': 2}
        for compound in compounds:
            cursor = self._collection.find_one({'InChI_Key': compound},
                                              projection=projection)
            if cursor is None:
                result.append(['None'])
                continue
            if not isinstance(cursor['synonyms'], list):
                cursor['synonyms'] = [cursor['synonyms']]
            result.append(cursor.get('synonyms', ['None']))
            # except TypeError:
            #     result.append(['None'])
        return [x[-1] for x in result]

    def get_unique_metabolites(self):
        """Get number of unique metabolites.

        Return:
            (:obj:`int`): number of unique metabolites.
        """
        return len(self._collection.distinct('InChI_Key', collation=self.collation))

    def get_metabolites_meta(self, inchi_key):
        """Get metabolite's meta information given inchi_key.

        Args:
            (:obj:`str`): InChI Key of metabolites

        Return:
            (:obj:`dict`): meta information object.
        """
        projection = {'_id': 0, 'reaction_participants': 0, 'similar_compounds': 0}
        query = {'InChI_Key': inchi_key}
        doc = self._collection.find_one(filter=query, projection=projection, collation=self.collation)
        if doc is None:
            return {}
        else:
            return doc

    def get_eymeta(self, inchi_key):
        """Get meta info from ECMDB or YMDB
        
        Args:
            inchi_key (:obj:`str`): inchikey / name of metabolite molecule.

        Return:
            (:obj:`Obj`): meta information.
        """
        projection = {'_id': 0, 'concentrations': 0}
        con_0 = {'name': inchi_key}
        con_1 = {'synonyms.synonym': inchi_key}
        con_2 = {'inchikey': inchi_key}
        query = {'$or': [con_0, con_1, con_2]}
        doc = self.e_collection.find_one(filter=query, projection=projection, collation=self.collation)
        if doc is not None:
            return doc
        else:
            doc = self.y_collection.find_one(filter=query, projection=projection, collation=self.collation)
            return doc

    def get_doc_by_name(self, names):
        """Get document by metabolite's list of possible names.

        Args:
            names(:obj:`list` of :obj:`str`): Name of possible names.

        Return:
            (:obj:`Obj`)
        """
        con_0 = {'name': {'$in': names}}
        con_1 = {'synonyms': {'$in': names}}
        query = {'$or': [con_0, con_1]}
        return self._collection.find_one(filter=query, collation=self.collation)
