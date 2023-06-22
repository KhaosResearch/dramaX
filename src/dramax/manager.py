from typing import Optional

from pymongo.database import Database

from dramax.database import get_mongo
from dramax.models.workflow import TaskInDatabase, WorkflowInDatabase


class BaseManager:
    def __init__(self, db: Optional[Database] = None):
        self.db = db
        if self.db is None:  # TODO: Not sure if this is the best way to do this.
            self.db = get_mongo()


class TaskManager(BaseManager):
    def find(self, **query) -> list:
        """
        Get task(s) from database based on `query`.
        """
        tasks: dict = self.db.task.find(query)
        tasks_in_db = []
        for task in tasks:
            tasks_in_db.append(TaskInDatabase(**task))
        return tasks_in_db

    def find_one(self, **query):
        """
        Get task from database based on `query`.
        """
        task_in_db: dict = self.db.task.find_one(query)
        if task_in_db:
            return TaskInDatabase(**task_in_db)

    def create_or_update_from_id(self, task_id: str, **extra_fields):
        self.db.task.update_one(
            {"id": task_id},
            {"$set": extra_fields},
            upsert=True,
        )


class WorkflowManager(BaseManager):
    def find_one(self, **query) -> Optional[WorkflowInDatabase]:
        workflow_in_db: dict = self.db.workflow.find_one(query)
        if workflow_in_db:
            return WorkflowInDatabase(**workflow_in_db)

    def create_or_update_from_id(self, workflow_id: str, **extra_fields) -> None:
        self.db.workflow.update_one(
            {"id": workflow_id},
            {"$set": extra_fields},
            upsert=True,
        )
