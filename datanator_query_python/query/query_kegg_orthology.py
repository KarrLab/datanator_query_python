from datanator_query_python.util import mongo_util
from pymongo.collation import Collation, CollationStrength


class QueryKO:

    def __init__(self, username=None, password=None, server=None, authSource='admin',
                 database='datanator', max_entries=float('inf'), verbose=True,
                 readPreference='nearest'):

        mongo_manager = mongo_util.MongoUtil(MongoDB=server, username=username,
                                             password=password, authSource=authSource, db=database,
                                             readPreference=readPreference)
        self.max_entries = max_entries
        self.verbose = verbose
        self.client, self.db, self.collection = mongo_manager.con_db(
            'kegg_orthology_new')
        self.collation = Collation(locale='en', strength=CollationStrength.SECONDARY)

    def get_ko_by_name(self, name):
        '''Get a gene's ko number by its gene name

        Args:
            name: (:obj:`str`): gene name
                
        Returns:
            result: (:obj:`str`): ko number of the gene
        '''
        query = {'gene_name': name}
        projection = {'gene_name': 1, 'kegg_orthology_id': 1}
        collation = {'locale': 'en', 'strength': 2}
        docs = self.collection.find_one(
            filter=query, projection=projection, collation=collation)
        if docs != None:
        	return docs['kegg_orthology_id']
        else:
        	return None

    def get_def_by_kegg_id(self, kegg_id):
        """Get kegg definition by kegg id
        
        Args:
            kegg_id (:obj:`str`): kegg orthology

        Returns:
            (:obj:`list` of :obj:`str`): list of kegg orthology definitions
        """
        query = {'kegg_orthology_id': kegg_id}
        projection = {'definition.name': 1, '_id': 0}
        doc = self.collection.find_one(filter=query, projection=projection)
        if doc is None:
            return [None]
        definitions = doc['definition']['name']
        return definitions

    def get_gene_ortholog_by_id_org(self, kegg_id, org):
        """Get kegg gene id given kegg_id and organism code.
        
        Args:
            kegg_id (:obj:`str`): Kegg ortholog id.
            org (:obj:`str`): Kegg organism code.

        Return:
            (:obj:`str`): gene id.
        """
        con_0 = {'kegg_orthology_id': kegg_id}
        con_1 = {'gene_ortholog.organism': org}
        query = {'$and': [con_0, con_1]}
        projection = {'_id': 0, 'gene_ortholog.$': 1}
        doc = self.collection.find_one(filter=query, projection=projection, collation=self.collation)
        if doc is None:
            return {}
        else:
            obj = doc['gene_ortholog'][0]['gene_id']
            if isinstance(obj, str):
                return obj.split('(')[0]
            else:
                return obj[0].split('(')[0]