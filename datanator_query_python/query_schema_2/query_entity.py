from datanator_query_python.config import query_schema_2_manager
from pymongo import ASCENDING


class QueryEn(query_schema_2_manager.QM):
    def __init__(self,
                 db="datanator-demo"):
        super().__init__()
        self.db = db

    def query_entity(self, 
                    identifier,
                    collection="entity",
                    limit=10,
                    skip=0,
                    projection={"_id": 0}):
        """Get entity with identifier.

        Args:
            identifier(:obj:`Obj`): identifier used for the entity.
            collection(:obj:`str`): name of the collection in which data resides.
            limit(:obj:`int`, optional): number of results to return.
            skip(:obj:`int`, optional): number of documents to skip.
            projection(:obj:`Obj`, optional): MongoDB projection.

        Return:
            (:obj:`list`): pymongo iterables.
        """
        col = self.client[self.db][collection]
        query = {"identifiers": {"$elemMatch": identifier}}
        result = []
        docs = col.find(filter=query,
                        limit=limit,
                        skip=skip,
                        projection=projection)
        for doc in docs:
            result.append(doc)
        return result