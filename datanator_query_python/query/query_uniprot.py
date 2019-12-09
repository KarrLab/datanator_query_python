from datanator_query_python.util import mongo_util
from pymongo.collation import Collation, CollationStrength


class QueryUniprot:

    def __init__(self, username=None, password=None, server=None, authSource='admin',
                 database='datanator', collection_str=None):

        self.mongo_manager = mongo_util.MongoUtil(MongoDB=server, username=username,
                                                  password=password, authSource=authSource, db=database)
        self.client, self.db, self.collection = self.mongo_manager.con_db(collection_str)
        self.collation = Collation(locale='en', strength=CollationStrength.SECONDARY)

    def get_doc_by_locus(self, locus, projection={'_id':0}):
        """Get preferred gene name by locus name
        
        Args:
            locus (:obj:`str`): Gene locus name
            projection (:obj:`dict`, optional): MongoDB query projection. Defaults to {'_id':0}.

        Return:
            (:obj:`tuple` of :obj:`Iter` and `int`): pymongo cursor object and number of documents.
        """
        con_0 = {'gene_name': locus}
        con_1 = {'gene_name_alt': locus}
        con_2 = {'gene_name_orf': locus}
        con_3 = {'gene_name_oln': locus}
        query = {'$or': [con_0, con_1, con_2, con_3]}
        docs = self.collection.find(filter=query, projection=projection, collation=self.collation)
        count = self.collection.count_documents(query, collation=self.collation)
        return docs, count

    def get_gene_protein_name_by_oln(self, oln, species=None, projection={'_id': 0}):
        """Get documents by ordered locus name
        
        Args:
            oln (:obj:`str`): Ordered locus name.
            species (:obj:`list`): NCBI taxonomy id. Defaults to None.
            projection (:obj:`dict`, optional): Pymongo projection. Defaults to {'_id': 0}.

        Return:
            (:obj:`tuple` of :obj:`str`): gene_name and protein_name
        """
        if species is None:
            query = {'gene_name_oln': oln}
        else:
            query = {'$and': [{'gene_name_oln': oln}, {'ncbi_taxonomy_id': {'$in': species}}]}
        doc = self.collection.find_one(filter=query, projection=projection, collation=self.collation)
        if doc is None:
            return None, None
        else:
            return doc['gene_name'], doc['protein_name']

    def get_protein_name_by_gn(self, gene_name, species=None, projection={'_id': 0}):
        """Get documents by gene name.
        
        Args:
            gene_name (:obj:`str`): gene name.
            species (:obj:`list`): NCBI taxonomy id. Defaults to None.
            projection (:obj:`dict`, optional): Pymongo projection. Defaults to {'_id': 0}.

        Return:
            (:obj:`tuple` of :obj:`str`): gene_name and protein_name
        """
        con_0 = {'gene_name': gene_name}
        con_1 = {'gene_name_alt': gene_name}
        con_2 = {'gene_name_orf': gene_name}
        con_3 = {'gene_name_oln': gene_name}
        name_search = {'$or': [con_0, con_1, con_2, con_3]}
        if species is None:
            query = {'gene_name': gene_name}
        else:
            query = {'$and': [name_search, {'ncbi_taxonomy_id': {'$in': species}}]}
        doc = self.collection.find_one(filter=query, projection=projection, collation=self.collation)
        if doc is None:
            return None
        else:
            return doc['protein_name']
        