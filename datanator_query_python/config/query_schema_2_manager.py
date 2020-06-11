from datanator_query_python.config import config
from datanator_query_python.util import mongo_util
from pymongo import ReadPreference


class QM(mongo_util.MongoUtil):

    def __init__(self, 
                server=config.AtlasConfig.SERVER,
                username=config.AtlasConfig.USERNAME,
                password=config.AtlasConfig.PASSWORD,
                authSource=config.AtlasConfig.AUTHDB,
                replicaSet=config.AtlasConfig.REPLSET,
                readPreference=config.AtlasConfig.READ_PREFERENCE 
                ):
        super().__init__(MongoDB=server, replicaSet=replicaSet,
                        username=username, password=password,
                        authSource=authSource, readPreference=readPreference)
        self.read_preference = self._convert_read_p(readPreference)

    def _convert_read_p(self, read_preference):
        """Convert string read preference to pymongo

        Args:
            read_preference(:obj:`str`): env variable in string.

        Return:
            (:obj:pymongo.ReadPreference)
        """
        if read_preference == 'nearest':
            return ReadPreference.NEAREST
        elif read_preference == 'primary':
            return ReadPreference.PRIMARY
    
    def conn_protein(self, db):
        """Establish connection with protein queries.

        Args:
            db(:obj:`str`): name of database.
            collection(:obj:`str`): name of collection.

        Return:
            (:obj:`pymongo.database.Database`)
        """
        return self.client.get_database(name=db, read_preference=self.read_preference)