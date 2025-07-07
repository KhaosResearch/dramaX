from typing import Any

from dramatiq import set_broker
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.middleware import CurrentMessage, Retries
from structlog import get_logger

from dramax.models.databases.minio import MinioService
from dramax.settings import settings


def _setup_worker() -> tuple[Any, RabbitmqBroker, MinioService]:
    log = get_logger("dramax.worker")
    log.info("Setting up RabbitMQ broker", url=settings.rabbit_dns)

    broker = RabbitmqBroker(url=settings.rabbit_dns)
    broker.add_middleware(CurrentMessage())
    broker.add_middleware(Retries(max_retries=0))

    set_broker(broker)
    log.info("Connected to queue", queue_name=settings.default_actor_opts.queue_name)

    minio_service = MinioService()

    log.info("Worker is ready")

    return log, broker, minio_service
