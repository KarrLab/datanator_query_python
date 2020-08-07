from datanator_query_python.config import query_schema_2_manager
import re


class QueryObs(query_schema_2_manager.QM):
    def __init__(self,
                 db="datanator-demo"):
        super().__init__()
        self.db = db

    def get_protein_datatype(self, 
                              identifier,
                              datatype="half-life",
                              collection="observation",
                              limit=10,
                              skip=0,
                              projection={"_id": 0}):
        """Get protein datatype.

        Args:
            identifier(:obj:`Obj`): identifier used for the protein.
            datatype(:obj:`Obj`): Datatype to be retrieved.
            collection(:obj:`str`, optional): name of collection in which values reside.
            limit(:obj:`int`, optional): number of results to return.
            skip(:obj:`int`, optional): number of documents to skip.

        Return:
            (:obj:`list`): pymongo iterables.
        """
        results = []
        col = self.client[self.db][collection]
        con_0 = {"entity.type": "protein"}
        con_1 = {}
        if datatype != "localization":
            con_1["values.type"] = datatype
        else:
            words = ["intramembrane_localization", "secretome location"]
            con_1["values.type"] = {"$in": words}
        query = {"$and": [{"identifier": identifier}, con_0, con_1]}
        docs = col.find(filter=query, limit=limit, skip=skip,
                        collation=self.collation,
                        projection=projection)
        for doc in docs:
            results.append(doc)
        return results