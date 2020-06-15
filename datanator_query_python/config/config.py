from dotenv import load_dotenv
from pathlib import Path, PurePath
import os

home_path = PurePath(Path.home(), '.wc/datanator.env')
root_path = "/.wc/danatator.env"
if os.path.exists(home_path):
    dotenv_path = home_path
else:
    dotenv_path = root_path

load_dotenv(dotenv_path)


class Config:
    '''
        API for frontend with read permission to 'datanator'
    '''
    PRODUCTION = os.getenv("PRODUCTION", False)
    USERNAME = os.getenv("MONGO_USERNAME")
    PASSWORD = os.getenv("MONGO_PASSWORD")
    SERVER = os.getenv("MONGO_ATLAS_SERVER")
    PORT = os.getenv("MONGO_PORT")
    REPLSET = os.getenv("MONGO_REPL")
    AUTHDB = os.getenv("MONGO_AUTHDB")
    SESSION_KEY = os.getenv("FLASK_SESSION_KEY")
    READ_PREFERENCE = os.getenv("READ_PREFERENCE")


class ProductionConfig(Config):

    PRODUCTION = True


class H1hescConfig(Config):
    """config for h1_hesc package
    
    Args:
        Config (:obj:`Obj`): Config class.
    """
    H1_HESC_USERNAME = os.getenv("MONGO_H1_HESC_USER")
    H1_HESC_PASSWORD = os.getenv("MONGO_H1_HESC_PWD")


class TestConfig(Config):
    '''
        test user with read permission to 'datanator' and
        readWrite permission to 'test'
    '''
    MONGO_TEST_USERNAME = os.getenv("MONGO_TEST_USERNAME")
    MONGO_TEST_PASSWORD = os.getenv("MONGO_TEST_PASSWORD")
    MONGO_TEST_PASSWORD_READ_PREFERENCE = os.getenv("MONGO_TEST_READ_PREFERENCE")


class UserAccountConfig(Config):
    '''
        API user account manager with readWrite permission
        to 'registered_users'
    '''

    USERDAEMON = os.getenv("MONGO_USER_DAMON")
    USERDAEMON_PASSWORD = os.getenv("MONGO_USER_PASSWORD")
    USERDAEMON_AUTHDB = os.getenv("MONGO_USER_AUTHDB")


class AtlasConfig:
    PRODUCTION = os.getenv("PRODUCTION", False)
    USERNAME = os.getenv("MONGO_USERNAME")
    PASSWORD = os.getenv("MONGO_PASSWORD")
    SERVER = os.getenv("MONGO_ATLAS_SERVER")
    PORT = os.getenv("MONGO_ATLAS_PORT")
    REPLSET = os.getenv("MONGO_ATLAS_REPL")
    AUTHDB = os.getenv("MONGO_ATLAS_AUTHDB")
    READ_PREFERENCE = os.getenv("MONGO_ATLAS_READPREFERENCE")    


class FtxConfig(Config):
    """Environment variables for AWS Elasticsearch service
    """
    FTX_AWS_PROFILE = os.getenv("FTX_AWS_PROFILE")
    TEST_FTX_PROFILE_NAME = os.getenv("TEST_FTX_PROFILE_NAME")
    REST_FTX_AWS_PROFILE = os.getenv("REST_FTX_AWS_PROFILE")


class SchemaMigration:
    USERNAME=os.getenv('MONGO_SCHEMA_MIGRATOR')
    PASSWORD=os.getenv('MONGO_SCHEMA_MIGRATOR_PASSWORD')


class DatanatorTest:
    USERNAME=os.getenv('MONGO_DATANATOR_TEST')
    PASSWORD=os.getenv('MONGO_DATANATOR_TEST_PASSWORD')