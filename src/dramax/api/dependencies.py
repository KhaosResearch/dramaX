from fastapi import HTTPException, Security
from fastapi.security import APIKeyCookie, APIKeyHeader, APIKeyQuery
from pymongo.database import Database
from starlette.status import HTTP_403_FORBIDDEN

from dramax.common.settings import settings
from dramax.services.mongo import MongoService

api_key_query = APIKeyQuery(name=settings.api_key_name, auto_error=False)
api_key_header = APIKeyHeader(name=settings.api_key_name, auto_error=False)
api_key_cookie = APIKeyCookie(name=settings.api_key_name, auto_error=False)


def fastapi_get_database() -> Database:
    return MongoService.get_database()


async def get_api_key(
    api_key_query: str = Security(api_key_query),
    api_key_header: str = Security(api_key_header),
    api_key_cookie: str = Security(api_key_cookie),
) -> str:
    """Validate the presence of an API key.

    The API key can be provided through one of the following methods:

    - Query parameter
    - Header
    - Cookie
    """
    if api_key_query == settings.api_key:
        return api_key_query
    if api_key_header == settings.api_key:
        return api_key_header
    if api_key_cookie == settings.api_key:
        return api_key_cookie
    raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Invalid API key")
