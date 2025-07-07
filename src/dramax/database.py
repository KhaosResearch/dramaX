from typing import Optional

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError
from structlog import get_logger

from dramax.settings import settings

log = get_logger("dramax.database")


class DatabaseClient:
    client: Optional[MongoClient] = None


# Create a singleton instance of MongoClient. This will be used to store the client connection across requests.
_db = DatabaseClient()


def connect_to_mongo() -> MongoClient:
    """
    Connect to Mongo.
    """
    log.debug("Connecting to Mongo...")
    client = MongoClient(settings.mongo_dns)
    try:
        client.server_info()
        log.debug("Connected to Mongo!!")
    except ServerSelectionTimeoutError as e:
        raise ConnectionError(
            f"Could not connect to database with connection string {settings.mongo_dns}"
        ) from e
    except Exception as e:
        raise (e)
    return client


def close_mongo() -> None:
    """
    Close the Mongo connection.
    """
    _db.client.close()


def get_mongo() -> Database:
    """
    Get or create a connection to Mongo.
    """
    if _db.client is None:
        _db.client = connect_to_mongo()
    return _db.client["dramax"]
