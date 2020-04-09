import pymongo
from pymongo.read_preferences import Primary, PrimaryPreferred, Secondary, SecondaryPreferred, Nearest
from bson import decode_all
import hashlib
from genson import SchemaBuilder


class MongoUtil:

    def __init__(self, cache_dirname=None, MongoDB=None, replicaSet=None, db='test',
                 verbose=False, max_entries=float('inf'), username=None, 
                 password=None, authSource='admin', readPreference='nearest'):
        self.client = pymongo.MongoClient(
            host=MongoDB, replicaSet=replicaSet, 
            username=username, password=password,
            authSource=authSource, readPreference=readPreference)
        self.db_obj = self.client.get_database(db)

    def list_all_collections(self):
        '''List all non-system collections within database
        '''
        return self.db_obj.list_collection_names()

    def con_db(self, collection_str):
        try:
            collection = self.db_obj[collection_str]
            return (self.client, self.db_obj, collection)
        except pymongo.errors.ConnectionFailure:
            return ('Server not available')
        except pymongo.errors.ServerSelectionTimeoutError:
            return ('Server timeout')

    def get_schema(self, collection_str):
        '''Get schema of a collection
           removed '_id' from collection due to its object type
           and universality 
        '''
        _, _, collection = self.con_db(collection_str)
        doc = collection.find_one({})
        builder = SchemaBuilder()
        del doc['_id']
        builder.add_object(doc)
        return builder.to_schema()

    def flatten_collection(self, collection_str):
        '''Flatten a collection

            c is ommitted because it does not have a non-object 
            value associated with it
        '''
        _, _, collection = self.con_db(collection_str)

        pipeline = [
            { "$addFields": { "subdoc.a": "$a" } },
            { "$replaceRoot": { "newRoot": "$subdoc" }  }
        ]
        flat_col = collection.aggregate(pipeline)
        return flat_col

    def get_duplicates(self, collection_str, _key):
        """Get duplicate key entries in collection.
        
        Args:
            collection_str (:obj:`str`): Name of collection.
            _key (:obj:`str`): Name of field in wichi to find duplicate values.

        Return:
            (:obj:`tuple` of :obj:`int` and :obj:`CommandCursor`):
            number of duplcate documents and documents iterable.
        """
        collection = self.db_obj[collection_str]
        _id = "$"+_key
        pipeline = [{"$group": {"_id": _id, "count": {"$sum": 1}}},
                    {"$match": {"count": {"$gt": 1}}},
                    {"$sort": {"count": -1}},
                    {"$project": {"name": "$_id", "_id": 0, "count": 1}}]
        count_pipeline = pipeline.copy()
        count_pipeline[-1]['$project'] = {'total_docs': {"$sum": "$count"}}
        counts = collection.aggregate(count_pipeline)
        for i, count in enumerate(counts):
            if i == 0:
                num = count["total_docs"]
        return num, collection.aggregate(pipeline)
 