from datanator_query_python.config import query_schema_2_manager


class QueryUniprot:

    def __init__(self, db=None, collection=None):
        self.db_obj = query_schema_2_manager.QM().conn_protein(db)
        self.collection = self.db_obj[collection]