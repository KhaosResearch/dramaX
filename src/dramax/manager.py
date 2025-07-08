from typing import Any

import dramatiq
import structlog
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from pymongo.database import Database

from dramax.models.databases.mongo import MongoService
from dramax.models.dramatiq.task import Status, Task
from dramax.models.dramatiq.workflow import TaskInDatabase, WorkflowInDatabase


class BaseManager:
    def __init__(self, db: Database | None = None) -> None:
        self.db = db if db is not None else MongoService.get_database()
        self.log = structlog.get_logger("dramax.manager")


class TaskManager(BaseManager):
    def find(self, **query) -> list:
        """
        Get task(s) from database based on `query`.
        """
        tasks: dict = self.db.task.find(query)
        return [TaskInDatabase(**task) for task in tasks]

    def find_one(self, **query) -> TaskInDatabase | None:
        """
        Get task from database based on `query`.
        """
        task_in_db: dict = self.db.task.find_one(query)
        if task_in_db:
            return TaskInDatabase(**task_in_db)
        return None

    def create(self, task_id: str, **extra_fields) -> None:
        self.db.task.insert_one(
            {
                "id": task_id,
                **extra_fields,
            },
        )

    def create_or_update_from_id(
        self,
        task_id: str,
        workflow_id: str,
        **extra_fields,
    ) -> None:
        self.db.task.update_one(
            {"id": task_id, "parent": workflow_id},
            {"$set": extra_fields},
            upsert=True,
        )

    def check_upstream(
        self,
        task: Task,
        workflow_id: str,
        message: dramatiq.Message[Any],
        log,  # noqa: ANN001
        broker: RabbitmqBroker,
    ) -> None:
        depends_on = task.depends_on
        if depends_on:
            log.debug("Checking upstream tasks")
            tasks_in_db = self.find(parent=workflow_id)
            for task_in_db in tasks_in_db:
                if task_in_db.id in depends_on:
                    if task_in_db.status == Status.STATUS_FAILED:
                        msg = "Upstream task failed"
                        raise ValueError(msg)
                    if task_in_db.status in (
                        Status.STATUS_PENDING,
                        Status.STATUS_RUNNING,
                    ):
                        # We abruptly stop the current task execution and enqueue it again.
                        log.debug(
                            "Re-enqueueing task because upstream task is not done yet",
                            depends_on=task.id,
                        )
                        broker.enqueue(message)
                        return
                    log.debug("Upstream task is done", upstream_task_id=task_in_db.id)


class WorkflowManager(BaseManager):
    def find_one(self, **query) -> WorkflowInDatabase | None:
        workflow_in_db: dict = self.db.workflow.find_one(query)
        if workflow_in_db:
            return WorkflowInDatabase(**workflow_in_db)
        return None

    def create_or_update_from_id(self, workflow_id: str, **extra_fields) -> None:
        self.db.workflow.update_one(
            {"id": workflow_id},
            {"$set": extra_fields},
            upsert=True,
        )
