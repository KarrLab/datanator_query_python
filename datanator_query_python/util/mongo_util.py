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

    def define_schema(self, collection_str, json_schema):
        """Define collection's $jsonSchema
        (https://docs.mongodb.com/manual/reference/operator/query/jsonSchema/)

        Args:
            collection_str (:obj:`str`): Name of the collection to be defined.
            json_schema (:obj:`str` or :obj:`dict`): location of the jsonSchema definition.
        """
        if isinstance(json_schema, str):
            with open(json_schema) as j:
                schema = json.load(j)
        else:
            schema = json_schema
        self.db_obj.create_collection(collection_str,
                                      validator={"$jsonSchema": schema},
                                      validationLevel="moderate")

    def update_observation(self, 
                           entity_name: str,
                           entity_type: str,
                           entity_identifiers: list,
                           obs_identifier: dict,
                           obs_source: dict,
                           obs_values=[],
                           _source=[],
                           genotype={},
                           entity_synonyms=[],
                           entity_related=[],
                           entity_description="",
                           schema_version="2.0",
                           col="observation"):
        """Update observation collection

        Args:
            entity_name(:obj:`str`): name of entity.
            entity_type(:obj:`str`): type of entity.
            entity_identifiers(:obj:`list`): list of entity identifiers.
            obs_identifier(:obj:`Obj`): identifier object of observation.
            obs_source(:obj:`Obj`): source object of observation used for querying.
            obs_values(:obj:`list`): values of observation.  
            _source(:obj:`list`): source array.
            genotype(:obj:`Obj`): genotype object of observation.                        
            entity_synonyms(:obj:`list`): list of entity synonyms.            
            entity_related(:obj:`list`): list of related items.
            entity_description(:obj:`str`): description of entity.
            schema_version(:obj:`str`): version of observation schema.
            col(:obj:`str`): name of the observation collection.
        """
        query = {"$and": [obs_identifier,
                          {"source": {"$elemMatch": obs_source}}]}
        _update = {"$set": {"genotype": genotype,
                            "entity.type": entity_type,
                            "entity.name": entity_name,
                            "entity.description": entity_description,
                            "schema_version": schema_version,
                            "identifier": obs_identifier},
                   "$addToSet": {"values": {"$each": obs_values},
                                 "source": {"$each": _source},
                                 "entity.synonyms": {"$each": entity_synonyms},
                                 "entity.identifiers": {"$each": entity_identifiers},
                                 "entity.related": {"$each": entity_related}}}
        self.db_obj[col].update_one(query,
                                    _update,
                                    upsert=True)        