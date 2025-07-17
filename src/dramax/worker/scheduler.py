from collections import defaultdict
from datetime import datetime

import structlog
from pymongo.database import Database

from dramax.common.settings import settings
from dramax.models.dramatiq.manager import TaskManager, WorkflowManager
from dramax.models.dramatiq.task import Status, Task
from dramax.models.dramatiq.workflow import Workflow, WorkflowStatus
from dramax.services.mongo import MongoService
from dramax.worker import set_failure, worker


class Scheduler:
    def __init__(self, db: Database | None = None) -> None:
        self.db = db or MongoService.get_database()
        self.log = structlog.get_logger("dramax.scheduler")
        self.log.info("Scheduler Initialized")

    def run(self, workflow: Workflow) -> None:
        """Execute workflow."""
        # Create workflow in database. We split this from the task creation
        # so that we can have a workflow in the database before any task is
        # created.

        WorkflowManager(self.db).create_or_update_from_id(
            workflow.id,
            metadata=workflow.metadata.dict(),
            created_at=datetime.now(tz=settings.timezone),
            status=WorkflowStatus.STATUS_PENDING,
        )

        for task in workflow.tasks:
            task.metadata.update(workflow.metadata.dict())

        inverted_index = {}
        for task in workflow.tasks:
            inverted_index[task.id] = task

        sorted_tasks = self.sorted_tasks(workflow)
        if len(sorted_tasks) != len(workflow.tasks):
            msg = "Some tasks are missing"
            raise ValueError(msg)

        for task_id in sorted_tasks:
            self.enqueue(task=inverted_index[task_id], workflow_id=workflow.id)

    def enqueue(self, task: Task, workflow_id: str) -> None:
        task_dict = task.dict()
        self.log.debug("Enqueuing task", task_id=task.id, workflow_id=workflow_id)

        TaskManager().create(
            task.id,
            parent=workflow_id,
            created_at=datetime.now(tz=settings.timezone),
            status=Status.STATUS_PENDING,
            **task_dict,
        )
        self.log.info("Entering worker")
        worker.send_with_options(
            args=(task_dict, workflow_id),
            on_failure=set_failure,
            queue_name=task.options.queue_name
            or settings.default_actor_opts.queue_name,
            options={"task_id": task.id, "workflow_id": workflow_id},
        )
        self.log.info("Finished worker")

    @staticmethod
    def sorted_tasks(workflow: Workflow) -> list:
        def iterative_topological_sort(graph: defaultdict, start: list) -> list:
            seen = set()
            stack = []  # path variable is gone, stack and order are new
            order = []  # order will be in reverse order at first
            q = list(start)
            while q:
                v = q.pop()
                if v not in seen:
                    seen.add(v)  # no need to append to path any more
                    q.extend(graph[v])

                    while stack and v not in graph[stack[-1]]:  # new stuff here!
                        order.append(stack.pop())
                    stack.append(v)

            return stack + order[::-1]  # new return value!

        sources = []
        graph = defaultdict(list)

        for task in workflow.tasks:
            if not task.inputs:
                sources.append(task.id)
            else:
                for depends_on in task.depends_on:
                    graph[depends_on].append(task.id)

        return iterative_topological_sort(graph, sources)
