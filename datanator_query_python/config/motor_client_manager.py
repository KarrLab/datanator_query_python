from datanator_query_python.util import motor_util
import os
from dotenv import load_dotenv
from pathlib import Path, PurePath


home_path = PurePath(Path.home(), '.wc/datanator.env')
root_path = "/.wc/danatator.env"
if os.path.exists(home_path):
    dotenv_path = home_path
else:
    dotenv_path = root_path
load_dotenv(dotenv_path)

where = os.getenv("WHERE") # API_TEST; API_PROD; MONGO_DATANATOR_TEST; MONGO_DATANATOR_PROD;
client = motor_util.MotorUtil(username=os.getenv(where), password=os.getenv("{}_PASSWORD".format(where)),
                              MongoDB=os.getenv("MONGO_ATLAS_SERVER")).client
