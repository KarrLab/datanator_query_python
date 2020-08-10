from datanator_query_python.config import query_schema_2_manager
from pymongo import ASCENDING, DESCENDING


class QueryObs(query_schema_2_manager.QM):
    def __init__(self,
                 db="datanator-demo"):
        super().__init__()
        self.db = db

    def get_entity_datatype(self, 
                              identifier,
                              entity="protein",
                              datatype="half-life",
                              collection="observation",
                              limit=10,
                              skip=0,
                              projection={"_id": 0}):
        """Get entity datatype.

        Args:
            identifier(:obj:`Obj`): identifier used for the entity.
            entity(:obj:`Obj`, optional): entity type. i.e. "protein", "RNA", etc. 
            datatype(:obj:`Obj`, optional): Datatype to be retrieved.
            collection(:obj:`str`, optional): name of collection in which values reside.
            limit(:obj:`int`, optional): number of results to return.
            skip(:obj:`int`, optional): number of documents to skip.

        Return:
            (:obj:`list`): pymongo iterables.
        """
        results = []
        col = self.client[self.db][collection]
        con_0 = {"entity.type": entity}
        con_1 = {}
        if entity == "protein" and datatype != "localization":
            con_1["values.type"] = datatype
        elif entity == "protein" and datatype == "localization":
            words = ["intramembrane_localization", "secretome location"]
            con_1["values.type"] = {"$in": words}
        elif entity == "RNA" and datatype == "localization":
            con_1["values.type"] = "subcellular_localization"
        query = {"$and": [{"identifier": identifier}, con_0, con_1]}
        docs = col.find(filter=query, limit=limit, skip=skip,
                        collation=self.collation,
                        projection=projection,
                        hint=[("identifier", ASCENDING), 
                              ("entity.type", ASCENDING),
                              ("values.type", ASCENDING)])
        for doc in docs:
            results.append(doc)
        return results
