from datanator_query_python.config import query_schema_2_manager


class FTX(query_schema_2_manager.QM):
    def __init__(self):
        super().__init__()

    def search_taxon(self,
                     msg,
                     skip=0,
                     limit=10,
                     token_order="sequential",
                     db="datanator-test"):
        """Search for taxon names.
        (https://docs.atlas.mongodb.com/reference/atlas-search)

        Args:
            msg(:obj:`str`): query message.
            skip(:obj:`int`, optional): number of records to skip.
            limit(:obj:`int`, optional): max number of documents to return.
            token_order(:obj:`str`, optional): token order, i.e. sequential or any.
            db(:obj:`str`, optional): name of database in which the result resides.

        Return:
            (:obj:`CommandCursor`): MongDB CommandCursor after aggregation.
        """
        collection = self.client[db]["taxon_tree"]
        path = ["tax_name", "name_txt"]
        return collection.aggregate([
                                        {
                                            "$search": {
                                                "autocomplete": {
                                                    "path": path,
                                                    "query": msg,
                                                    "tokenOrder": token_order
                                                }
                                            }
                                        },
                                        {
                                            "$limit": limit,
                                            "$skip": skip
                                        },
                                        {
                                            "$project": {
                                                "_id": 0,
                                                "tax_name": 1
                                            }
                                        }
                                    ])