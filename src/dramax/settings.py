import platform
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import AnyUrl, BaseModel, BaseSettings
from structlog import get_logger

log = get_logger("dramax.settings")


class MongoDns(AnyUrl):
    allowed_schemes = {"mongodb"}
    user_required = True


class RabbitDns(AnyUrl):
    allowed_schemes = {"amqp"}
    user_required = True


class ActorOpts(BaseModel):
    queue_name: str = "default"
    max_retries: int = 0
    time_limit: int = 3600000 * 7
    notify_shutdown: bool = True


class Settings(BaseSettings):
    base_path: str = ""

    api_host: str = "0.0.0.0"
    api_port: int = 8001
    api_debug: bool = False
    api_key: str = "dev"
    api_key_name: str = "access_token"

    mongo_dns: MongoDns = "mongodb://root:root@localhost:27017"

    rabbit_dns: RabbitDns = "amqp://rabbit:rabbit@localhost:5672"

    minio_endpoint: str = "localhost:9000"
    minio_bucket: str = "dramax"
    minio_access_key: str = "minio"
    minio_secret_key: str = "minio123"
    minio_use_ssl: bool = False

    timezone: ZoneInfo = ZoneInfo("Europe/Madrid")
    # Actor options, as defined in dramatiq.actor.ActorOptions.
    # >>> export DEFAULT_ACTOR_OPTS='{"max_retries": 1}'
    default_actor_opts: ActorOpts = ActorOpts()

    # Directory to store temporary files.
    # TODO: Use Path instead of str.
    data_dir: str = str(
        Path("/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()),
    )

    class Config:
        # Later files in the list will take priority over earlier files.
        env_file = [
            ".env.local",
            ".env.prod",
            "/etc/dramax/.env.local",
            "/etc/dramax/.env.prod",
        ]
        for f in reversed(env_file):
            if Path(f).exists():
                log.info("Loading environment variables from file", env_file=f)
                break
        env_file_encoding = "utf-8"


settings = Settings()
