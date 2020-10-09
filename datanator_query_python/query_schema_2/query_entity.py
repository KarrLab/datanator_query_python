from datanator_query_python.config import query_schema_2_manager


class QueryEn(query_schema_2_manager.QM):
    def __init__(self,
                 db="datanator-demo"):
        super().__init__()
        self.db = db

    def query_entity(self, 
                    identifier,
                    datatype="metabolite",
                    collection="entity",
                    limit=10,
                    skip=0,
                    projection={"_id": 0}):
        """Get entity with identifier.

        Args:
            identifier(:obj:`Obj`): identifier used for the entity.
            datatype(:obj:`Obj`, optional): Datatype to be retrieved.
            collection(:obj:`str`): name of the collection in which data resides.
            limit(:obj:`int`, optional): number of results to return.
            skip(:obj:`int`, optional): number of documents to skip.
            projection(:obj:`Obj`, optional): MongoDB projection.

        Return:
            (:obj:`list`): pymongo iterables.
        """
        col = self.client[self.db][collection]
        con_0 = {"identifiers": {"$elemMatch": identifier}}
        con_1 = {"type": datatype}
        query = {"$and": [con_0, con_1]}
        result = []
        docs = col.find(filter=query,
                        limit=limit,
                        skip=skip,
                        projection=projection)
        for doc in docs:
            result.append(doc)
        return result