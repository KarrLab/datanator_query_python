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
                           obj,
                           source,
                           db="test",
                           op="update",
                           col="observation",
                           query={}):
        """Update observation collection

        Args:
            obj(:obj:`Obj`): obs object to be updated with.
            source(:obj:`Obj`): one of the sources used for matching.
            db(:obj:`Obj`): Name of database to be updated. 
            op(:obj:`str`): Operation to be done.
            col(:obj:`str`): Name of collection to be updated.
            query(:obj:`Obj`): Filter for updating operation.
        """
        _set = {}
        add_to_set = {}
        for key, val in obj.items():
            if isinstance(val, list):
                add_to_set[key] = {"$each": val}
            else:
                _set[key] = val
        if query == {}:
            query = {"$and": [{"identifier": _set["identifier"]},
                              {"source": {"$elemMatch": source}}]}
        _update = {"$set": _set,
                   "$addToSet": add_to_set}
        if op == "update":
            self.client[db][col].update_one(query,
                                            _update,
                                            upsert=True)
        else:
            return query, _update

    def update_entity(self,
                      obj,
                      match,
                      db="test",
                      col="entity",
                      op="update",
                      query={}):
        """Update entity collection.

        Args:
            obj(:obj:`Obj`): object to be updated with.
            match(:obj:`Obj`): Identifier used to match existing document in collection.
            db(:obj:`str`): Name of database to be updated.
            col(:obj:`str`): Name of collection to be updated.
            op(:obj:`str`): operation to be done, e.g. update or bulk.
            query(:obj:`Obj`): Filter for updating operation.
        """
        _set = {}
        add_to_set = {}
        for key, val in obj.items():
            if isinstance(val, list):
                add_to_set[key] = {"$each": val}
            else:
                _set[key] = val
        if query == {}:
            query = {"identifiers": {"$elemMatch": match}}
        _update = {"$set": _set,
                   "$addToSet": add_to_set}
        if op == "update":
            self.client[db][col].update_one(query,
                                            _update,
                                            upsert=True)
        else:
            return query, _update

    def build_taxon_object(self, _id, _format="tax_id"):
        """Build taxon object from taxon_id.
        (https://github.com/KarrLab/datanator_pattern_design/blob/master/components/taxon.json)

        Args:
            _id (:obj:`int`): Organism's taxonomy ID.

        Return:
            (:obj:`Obj`)
        """
        obj = self.client["datanator-test"]["taxon_tree"].find_one({_format: _id},
                                                                    projection={"canon_anc_ids": 1,
                                                                                "canon_anc_names": 1,
                                                                                "tax_name": 1})
        if obj is None:
            return {}
        else:
            canon_ancestors = []
            canon_anc_ids = obj["canon_anc_ids"]
            canon_anc_names = obj["canon_anc_names"]
            for _id, name in zip(canon_anc_ids, canon_anc_names):
                canon_ancestors.append({"ncbi_taxonomy_id": _id,
                                        "name": name})
            return {"ncbi_taxonomy_id": taxon_id,
                    "name": obj["tax_name"],
                    "canon_ancestors": canon_ancestors}                
    