from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError
from structlog import get_logger

from dramax.common.settings import settings

log = get_logger("dramax.database")


class MongoService:
    _client: MongoClient | None = None

    @classmethod
    def connect(cls) -> MongoClient:
        """Initialize and return the MongoDB client singleton."""
        if cls._client is not None:
            return cls._client

        log.debug("Connecting to MongoDB...", dns=settings.mongo_dns)
        try:
            cls._client = MongoClient(settings.mongo_dns)
            cls._client.server_info()  # Trigger connection test
            log.debug("MongoDB connection established.")
        except ServerSelectionTimeoutError as e:
            msg = f"Could not connect to MongoDB at {settings.mongo_dns}"
            log.exception("MongoDB connection failed.", error=str(e))
            raise ConnectionError(msg) from e
        except Exception as e:
            log.exception("Unexpected MongoDB error.", error=str(e))
            raise

        return cls._client

    @classmethod
    def get_database(cls, name: str = "dramax") -> Database:
        """Get the specified MongoDB database. Defaults to 'dramax'."""
        client = cls.connect()
        return client[name]

    @classmethod
    def disconnect(cls) -> None:
        """Close the MongoDB connection."""
        if cls._client:
            log.debug("Closing MongoDB connection...")
            cls._client.close()
            cls._client = None
            log.debug("MongoDB connection closed.")
