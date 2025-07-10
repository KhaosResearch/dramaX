from contextlib import asynccontextmanager

import uvicorn
from fastapi import APIRouter, Depends, FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse
from starlette import status
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, Response
from structlog import get_logger

from dramax import __version__
from dramax.api.dependencies import get_api_key
from dramax.api.routes.workflow import router
from dramax.common.settings import settings
from dramax.services.mongo import close_mongo, connect_to_mongo  # !CHANGE THISN IMPORTS

log = get_logger("dramax.api")


@asynccontextmanager
async def lifespan(_: FastAPI):  # noqa: ANN201
    """Context manager to initialize and close resources for the application."""
    connect_to_mongo()
    yield
    close_mongo()


app = FastAPI(
    title="dramaX API",
    description="",
    version=__version__,
    # Remove the default docs and redoc endpoints to avoid conflicts with the
    # custom ones below.
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

main_router = APIRouter()


@main_router.get(
    "/healthz",
    name="Health check",
    status_code=status.HTTP_200_OK,
    tags=["health"],
)
async def health() -> Response:
    """Health check endpoint. Useful for liveness and readiness probes."""
    return Response(status_code=status.HTTP_200_OK)


@main_router.get(
    "/api/openapi.json",
    tags=["documentation"],
    include_in_schema=False,
    dependencies=[Depends(get_api_key)],
)
async def get_open_api_endpoint() -> JSONResponse:
    return JSONResponse(
        get_openapi(
            title="dramaX",
            version=__version__,
            routes=app.routes,
        ),
    )


@main_router.get(
    "/api/docs",
    tags=["documentation"],
    include_in_schema=False,
    dependencies=[Depends(get_api_key)],
)
async def get_documentation() -> HTMLResponse:
    return get_swagger_ui_html(
        openapi_url=f"/api/openapi.json?{settings.api_key_name}={settings.api_key}",
        title="Documentation",
    )


main_router.include_router(router, prefix="/api/v2/workflow")

app.include_router(main_router, prefix=settings.base_path)


def run_server() -> None:
    log.info(
        "Deploying server at http://%s:%s%s",
        settings.api_host,
        settings.api_port,
        settings.base_path,
    )
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level="trace" if settings.api_debug else "critical",
    )
