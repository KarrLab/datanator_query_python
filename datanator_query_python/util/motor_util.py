import motor.motor_asyncio
import copy
from genson import SchemaBuilder


class MotorUtil:

    def __init__(self, MongoDB=None, replicaSet=None, db='test',
                 verbose=False, max_entries=float('inf'), username=None, 
                 password=None, authSource='admin', readPreference='nearest'):
        string = "mongodb+srv://{}:{}@{}/{}?authSource={}&retryWrites=true&w=majority&readPreference={}".format(username, password, MongoDB, db, authSource, readPreference)
        self.client = motor.motor_asyncio.AsyncIOMotorClient(string)
