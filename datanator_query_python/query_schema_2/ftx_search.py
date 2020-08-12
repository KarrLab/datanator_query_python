from datanator_query_python.config import query_schema_2_manager


class FTX(query_schema_2_manager.QM):
    def __init__(self):
        super().__init__()

    def search_taxon(self,
                     msg,
                     skip=0,
                     limit=10,
                     db="datanator-test"):
        """Search for taxon names.

        Args:
            msg(:obj:`str`): query message.
            skip(:obj:`int`): number of records to skip.
            limit(:obj:`int`): max number of documents to return.
            db(:obj:`str`, optional): name of database in which the result resides.

        Return:
            (:obj:`CommandCursor`): MongDB CommandCursor after aggregation.
        """