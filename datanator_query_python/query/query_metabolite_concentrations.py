from datanator_query_python.util import mongo_util, file_util
from pymongo.collation import Collation, CollationStrength
import numpy as np


class QueryMetaboliteConcentrations(mongo_util.MongoUtil):

    def __init__(self, MongoDB=None, db=None, collection_str=None, username=None,
                 password=None, authSource='admin', readPreference='nearest',
                 verbose=True, replicaSet=None):
        super().__init__(MongoDB=MongoDB, db=db, verbose=verbose, username=username,
                         password=password, authSource=authSource, readPreference=readPreference,
                         replicaSet=replicaSet)
        self.file_manager = file_util.FileUtil()
        self._collection = self.db_obj[collection_str]
        self.collation = Collation(locale='en', strength=CollationStrength.SECONDARY)

    def get_similar_concentrations(self, metabolite, threshold=0.6):
        """Get metabolite's similar compounds' concentrations above
        threshold tanimoto value.

        Args:
            metabolite(:obj:`str`): InChIKey of metabolite.
            threshold(:obj:`float`, optional): Threshold value (inclusive).

        Return:
            (:obj:`list` of :obj:`Obj`): [{'inchikey': xxxx, 'similarity_score': ..., 'concentrations': []}]
        """
        result = []
        meta_collection = self.client.get_database("datanator-test")['metabolites_meta']
        doc = meta_collection.find_one(filter={'InChI_Key': metabolite},
                                        projection={'similar_compounds': 1},
                                        collation=self.collation)
        if not doc:
            return result
        r_inchikeys = []
        scores = []
        for obj in doc["similar_compounds"]:
            if obj["similarity_score"] >= threshold:
                r_inchikeys.append(obj["inchikey"])
                scores.append(obj["similarity_score"])
            else:
                break
        pipeline = [
            {"$match": {"inchikey": {"$in": r_inchikeys}}},
            {"$addFields": {"__order": {"$indexOfArray": [r_inchikeys, "$inchikey" ]}}},
            {"$sort": {"__order": -1}}
        ]
        docs = self._collection.aggregate(pipeline)
        if not docs:
            return result
        for i, doc in enumerate(docs):
            inchikey = doc['inchikey']
            name = doc['metabolite']
            result.append({'inchikey': inchikey,
                           'similarity_score': scores[i],
                           'metabolite': name,
                           'concentrations': doc['concentrations']})
            print(inchikey)
        return result

    def get_conc_count(self):
        """Get total number of concentration data points.
        """
        project = {"$project": {"conc_len": {"$size": "$concentrations"}}}
        group = {"$group": {"_id": None,
                            "total": {"$sum": "$conc_len"},
                            "count": {"$sum": 1}}}
        pipeline = [project, group]
        docs = self._collection.aggregate(pipeline)
        for doc in docs:
            return doc['total']

    def get_conc_by_taxon(self, _id):
        """Get concentrations by ncbi taxonomy ID.

        Args:
            _id(:obj:`int`): NCBI Taxonomy ID.

        Return:
            (:obj:`Pymongo.Cursor`)
        """
        query = {"concentrations.ncbi_taxonomy_id": _id}
        return self._collection.find(filter=query)
