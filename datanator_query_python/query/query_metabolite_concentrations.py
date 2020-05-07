from datanator_query_python.util import mongo_util, file_util
from pymongo.collation import Collation, CollationStrength
import numpy as np


class QueryMetaboliteConcentrations(mongo_util.MongoUtil):

    def __init__(self, MongoDB=None, db=None, collection_str=None, username=None,
                 password=None, authSource='admin', readPreference='nearest',
                 verbose=True):
        super().__init__(MongoDB=MongoDB, db=db, verbose=verbose, username=username,
                         password=password, authSource=authSource, readPreference=readPreference)
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
        meta_collection = self.db_obj['metabolites_meta']
        doc = meta_collection.find_one(filter={'InChI_Key': metabolite},
                                        projection={'similar_compounds': 1},
                                        collation=self.collation)
        if not doc:
            return result
        obj = self.file_manager.merge_dict(doc.get('similar_compounds'))
        inchikeys = list(obj.keys())
        inchikeys.reverse()
        values = list(obj.values())
        values.reverse()
        threshold_index = np.searchsorted(np.asarray(values), threshold, side='left')
        r_inchikeys = inchikeys[threshold_index:] # relevant inchikeys
        pipeline = [
            {"$match": {"inchikey": {"$in": r_inchikeys}}},
            {"$addFields": {"__order": {"$indexOfArray": [r_inchikeys, "$inchikey" ]}}},
            {"$sort": {"__order": -1}}
        ]
        docs = self._collection.aggregate(pipeline)
        for doc in docs:
            inchikey = doc['inchikey']
            result.append({'inchikey': inchikey,
                           'similarity_score': obj[inchikey],
                           'concentrations': doc['concentrations']})
        return result
