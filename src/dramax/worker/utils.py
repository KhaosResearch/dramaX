from collections.abc import Callable
from datetime import datetime
from typing import Any

import dramatiq
from dramatiq import MessageProxy, set_broker
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.middleware import CurrentMessage, Retries
from structlog import get_logger

from dramax.common.configure_logger import configure_logger
from dramax.common.settings import settings
from dramax.models.dramatiq.manager import TaskManager, WorkflowManager
from dramax.models.dramatiq.task import Result, Status
from dramax.models.dramatiq.workflow import WorkflowStatus
from dramax.services.minio import MinioService

configure_logger()


def setup_worker() -> tuple[Any, RabbitmqBroker, MinioService]:
    log = get_logger("dramax.worker")
    log.info("Setting up RabbitMQ broker", url=settings.rabbit_dns)

    broker = RabbitmqBroker(url=settings.rabbit_dns)
    broker.add_middleware(CurrentMessage())
    broker.add_middleware(Retries(max_retries=5))

    set_broker(broker)
    log.info("Connected to queue", queue_name=settings.default_actor_opts.queue_name)

    minio_service = MinioService.get_instance()

    log.info("Worker is ready")

    return broker, minio_service


def set_workflow_run_state(workflow_id: str) -> None:
    """Set workflow state based on task statuses."""
    log = get_logger("dramax.worker")
    workflow_in_db = WorkflowManager().find_one(id=workflow_id)
    if not workflow_in_db:
        log.error(
            "Workflow not found. This should not happen.",
            workflow_id=workflow_id,
        )
        msg = f"Workflow `{workflow_id}` not found"
        raise ValueError(msg)

    tasks = TaskManager().find(parent=workflow_id)

    task_status_only = []
    for task in tasks:
        status = task.status
        task_status_only.append(status)

    def check(comp: Callable, stats: list) -> bool:
        return comp([s in stats for s in task_status_only])

    if workflow_in_db.is_revoked:
        workflow_status = WorkflowStatus.STATUS_REVOKED
    elif check(all, [Status.STATUS_DONE]):
        workflow_status = WorkflowStatus.STATUS_DONE
    elif check(all, [Status.STATUS_PENDING]):
        workflow_status = WorkflowStatus.STATUS_PENDING
    elif check(any, [Status.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_FAILED
    elif check(any, [Status.STATUS_PENDING]) and not check(any, [Status.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_PENDING
    elif check(any, [Status.STATUS_RUNNING]) and not check(any, [Status.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_RUNNING
    else:
        workflow_status = WorkflowStatus.STATUS_PENDING

    WorkflowManager().create_or_update_from_id(
        workflow_id=workflow_id,
        updated_at=datetime.now(tz=settings.timezone),
        status=workflow_status,
    )


def set_running(task_id: str, workflow_id: str) -> None:
    TaskManager().create_or_update_from_id(
        task_id,
        workflow_id,
        updated_at=datetime.now(tz=settings.timezone),
        status=Status.STATUS_RUNNING,
    )
    set_workflow_run_state(workflow_id=workflow_id)


def set_success(task_id: str, workflow_id: str, result_data: str) -> None:
    task_result = Result(log=result_data)
    TaskManager().create_or_update_from_id(
        task_id,
        workflow_id,
        updated_at=datetime.now(tz=settings.timezone),
        result=task_result.dict(),
        status=Status.STATUS_DONE,
    )
    set_workflow_run_state(workflow_id=workflow_id)


@dramatiq.actor(queue_name=settings.default_actor_opts.queue_name)
def set_failure(message: MessageProxy, exception_data: str) -> None:
    log = get_logger("dramax.worker")
    log.error(message["options"]["traceback"])
    actor_opts = message["options"]["options"]
    workflow_id = actor_opts["workflow_id"]
    task_result = Result(message=exception_data)
    TaskManager().create_or_update_from_id(
        actor_opts["task_id"],
        workflow_id,
        updated_at=datetime.now(tz=settings.timezone),
        result=task_result.dict(),
        status=Status.STATUS_FAILED,
    )
    set_workflow_run_state(workflow_id=workflow_id)
