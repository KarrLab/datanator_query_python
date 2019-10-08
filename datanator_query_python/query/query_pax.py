from datanator_query_python.util import chem_util, file_util
from . import query_nosql
from pymongo.collation import Collation, CollationStrength


class QueryPax(query_nosql.DataQuery):
    '''Queries specific to pax collection
    '''

    def __init__(self, cache_dirname=None, MongoDB=None, replicaSet=None, db='datanator',
                 collection_str='pax', verbose=False, max_entries=float('inf'), username=None,
                 password=None, authSource='admin', readPreference='primary'):
        super(query_nosql.DataQuery, self).__init__(cache_dirname=cache_dirname, MongoDB=MongoDB,
                                                    replicaSet=replicaSet, db=db,
                                                    verbose=verbose, max_entries=max_entries, username=username,
                                                    password=password, authSource=authSource, readPreference=readPreference)
        self.collation = Collation(locale='en',
                                   strength=CollationStrength.SECONDARY)
        self.chem_manager = chem_util.ChemUtil()
        self.file_manager = file_util.FileUtil()
        self.max_entries = max_entries
        self.verbose = verbose
        self.client, self.db_obj, self.collection = self.con_db(collection_str)

    def get_all_species(self):
        '''
            Get a list of all species in pax collection

            Returns:
                results (:obj:`list` of :obj:`str`): list of specie names
                                            with no duplicates
        '''
        results = []
        query = {}
        projection = {'species_name': 1}
        docs = self.collection.find(filter=query, projection=projection)
        for doc in docs:
            results.append(doc['species_name'])
        return list(set(results))

    def get_abundance_from_uniprot(self, uniprot_id):
        '''
            Get all abundance data for uniprot_id

            Args:
                uniprot_id (:obj:`str`) protein uniprot_id.

            Returns:
                result (:obj:`list` of :obj:`dict`): result containing
                [{'ncbi_taxonomy_id': , 'species_name': , 'ordered_locus_name': },
                {'organ': , 'abundance'}, {'organ': , 'abundance'}].
        '''
        query = {'observation.protein_id.uniprot_id': uniprot_id}
        projection = {'ncbi_id': 1, 'species_name': 1,
                      'observation.$': 1, 'organ': 1}
        collation = Collation(locale='en', strength=CollationStrength.SECONDARY)
        docs = self.collection.find(filter=query, projection=projection, collation=collation)
        count = self.collection.count_documents(query)
        try:
            result = [{'ncbi_taxonomy_id': docs[0]['ncbi_id'],
            'species_name': docs[0]['species_name']}]
        except IndexError:
            return []
        for i, doc in enumerate(docs):
            if i > self.max_entries:
                break
            if self.verbose and i % 50 == 0:
                print('Processing pax document {} out of {}'.format(i, count))
            organ = doc['organ']
            abundance = doc['observation'][0]['abundance']
            ordered_locus_name = doc['observation'][0]['string_id']
            result[0]['ordered_locus_name'] = ordered_locus_name
            dic = {'organ': organ,
            'abundance': abundance}
            result.append(dic)
        return result

    def get_file_by_name(self, file_name: list, projection={'_id': 0}, collation=None) -> list:
        """Given file name, get the information attached to the file.   
        
        Args:
            file_name (list): list of file names, e.g. ['9606/9606-iPS_(DF19.11)_iTRAQ-114_Phanstiel_2011_gene.txt']
        
        Returns:
            list: files that meet the requirement. 
        """
        result = []
        if collation is None:
            collation = self.collation
        query = {'file_name': {'$in': file_name}}
        projection = projection
        docs = self.collection.find(filter=query, projection=projection)
        for doc in docs:
            result.append(doc)
        return result