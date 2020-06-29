import pymongo
from pymongo.read_preferences import Primary, PrimaryPreferred, Secondary, SecondaryPreferred, Nearest
import copy
import json
from genson import SchemaBuilder


class MongoUtil:

    def __init__(self, cache_dirname=None, MongoDB=None, replicaSet=None, db='test',
                 verbose=False, max_entries=float('inf'), username=None, 
                 password=None, authSource='admin', readPreference='nearest'):
        string = "mongodb+srv://{}:{}@{}/{}?authSource={}&retryWrites=true&w=majority&readPreference={}".format(username, password, MongoDB, db, authSource, readPreference)
        self.client = pymongo.MongoClient(string)
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

    def get_duplicates(self, collection_str, _key, **kwargs):
        """Get duplicate key entries in collection.
        
        Args:
            collection_str (:obj:`str`): Name of collection.
            _key (:obj:`str`): Name of field in wichi to find duplicate values.

        Return:
            (:obj:`tuple` of :obj:`int` and :obj:`CommandCursor`):
            length of cursor and documents iterable.
        """
        collection = self.db_obj[collection_str]
        _id = "$"+_key
        pipeline = [{"$group": {"_id": {_key: _id}, "count": {"$sum": 1}, "uniqueIds": {"$addToSet": "$_id"}}},
                    {"$match": {"count": {"$gt": 1}}},
                    {"$sort": {"count": -1}}]
        count_pipeline = copy.deepcopy(pipeline)
        count_pipeline[-1] = {"$count": "total_return"}
        counts = collection.aggregate(count_pipeline, **kwargs)
        for i, count in enumerate(counts):
            if i == 0:
                num = count["total_return"]                    
        return num, collection.aggregate(pipeline, **kwargs)
 
    def split_cursors(self, count, db, collection, query={},
                      projection={'_id': 0}):
        """Manually return multiple cursors.

        Args:
            count(:obj:`int`): Number of cursors wanted.
            db(:obj:`int`): Name of database.
            collection_str(:obj:`str`): Name of collection.
            query(:obj:`Obj`, optional): mongodb query to be performed.
            projection(:obj:`Obj`, optional): mongodb query projection.
            
        Return:
            (:obj:`list` of :obj:`pymongo.Cursor`)
        """
        from_collection = self.client.get_database(db)[collection]
        total_docs = from_collection.count_documents(query)
        step_size = int((total_docs % count + total_docs) / count)
        result = []
        for i in range(0, count):
            result.append(from_collection.find(filter=query, skip=i * step_size, limit=step_size,
                                               projection=projection))
        return result

    def define_schema(self, collection_str, json_schema):
        """Define collection's $jsonSchema
        (https://docs.mongodb.com/manual/reference/operator/query/jsonSchema/)

        Args:
            collection_str (:obj:`str`): Name of the collection to be defined.
            json_schema (:obj:`str`): location of the jsonSchema definition.
        """
        with open(json_schema) as j:
            schema = json.load(j)
        self.db_obj.create_collection(collection_str,
                                      validator={"$jsonSchema": schema},
                                      validationLevel="moderate")