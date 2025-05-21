from pymongo import MongoClient
from ..exceptions.custom_exceptions import ExistingMongoConnection
from ..config_loader import get_config
from ..utils import logger
default_mongo_client = None
class MongoDBConnector:
    """
    A singleton class for connecting to mongodb.
    This class provides a singleton instance of a mongo client, ensuring that only one instance is created
    and used throughout the application. It utilizes the MongoClient to connect to mongodb.
    Usage:
        connector = MongoDBConnector.get_instance()
    Methods:
        get_instance():
            It returns the mongo connector
    Note:
        It's recommended to carefully consider the use of the Singleton pattern and potential alternatives,
        as it can introduce global state and make code harder to test and reason about.
    """
    mongo_client = None
    def __init__(self):
        # Since it is a singleton connection, We should not allow second connection
        # If the connection is already set up, we should use that one using get_instance().
        if MongoDBConnector.mongo_client is not None:
            raise ExistingMongoConnection()
        else:
            default_mongo_client = MongoClient(get_config("ALBANERO_MONGO_DB_URI"))
            # Trigger a 'ping' command to ensure the connection is established
            default_mongo_client.admin.command('ping')
            MongoDBConnector.mongo_client = default_mongo_client
            logger.info("MongoDB connection is established.")
    @staticmethod
    def get_instance():
        """Static Access Method"""
Uncovered code
        if MongoDBConnector.mongo_client is None:
            MongoDBConnector()
        return MongoDBConnector.mongo_client