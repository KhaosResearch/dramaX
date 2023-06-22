from fastapi import HTTPException, Security
from fastapi.security import APIKeyCookie, APIKeyHeader, APIKeyQuery
from starlette.status import HTTP_403_FORBIDDEN

from dramax.settings import settings

api_key_query = APIKeyQuery(name=settings.api_key_name, auto_error=False)
api_key_header = APIKeyHeader(name=settings.api_key_name, auto_error=False)
api_key_cookie = APIKeyCookie(name=settings.api_key_name, auto_error=False)


async def get_api_key(
    api_key_query: str = Security(api_key_query),
    api_key_header: str = Security(api_key_header),
    api_key_cookie: str = Security(api_key_cookie),
):
    """
    Checks the presence of an API key in the following places:
    * Query parameter
    * Header value
    * Cookie
    """
    if api_key_query == settings.api_key:
        return api_key_query
    elif api_key_header == settings.api_key:
        return api_key_header
    elif api_key_cookie == settings.api_key:
        return api_key_cookie
    else:
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Invalid API key")
