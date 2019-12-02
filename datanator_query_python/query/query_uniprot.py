from datanator_query_python.util import mongo_util
from pymongo.collation import Collation, CollationStrength


class QueryUniprot:

    def __init__(self, username=None, password=None, server=None, authSource='admin',
                 database='datanator', collection_str=None):

        self.mongo_manager = mongo_util.MongoUtil(MongoDB=server, username=username,
                                                  password=password, authSource=authSource, db=database)
        self.client, self.db, self.collection = self.mongo_manager.con_db(collection_str)
        self.collation = Collation(locale='en', strength=CollationStrength.SECONDARY)

    def get_gene_name_by_locus(self, locus, projection={'_id':0}):
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
        count = self.collection.count_documents(query)
        return docs, count
        