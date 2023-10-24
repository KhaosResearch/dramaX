from collections import defaultdict
from datetime import datetime
from typing import Optional

from dramatiq.brokers.rabbitmq import RabbitmqBroker
from pymongo.database import Database
from structlog import get_logger

from dramax.database import get_mongo
from dramax.manager import TaskManager, WorkflowManager
from dramax.models.task import Status, Task
from dramax.models.workflow import Workflow, WorkflowInDatabase, WorkflowStatus
from dramax.settings import settings
from dramax.worker import set_failure, worker


class Scheduler:
    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_mongo()
        self.log = get_logger()

    def run(self, workflow: Workflow):
        """
        Execute workflow.
        """
        # Create workflow in database. We split this from the task creation
        # so that we can have a workflow in the database before any task is
        # created.
        WorkflowManager(self.db).create_or_update_from_id(
            workflow.id,
            metadata=workflow.metadata.dict(),
            created_at=datetime.now(),
            status=WorkflowStatus.STATUS_PENDING,
        )

        for task in workflow.tasks:
            task.metadata.update(workflow.metadata.dict())

        inverted_index = {}
        for task in workflow.tasks:
            inverted_index[task.id] = task

        sorted_tasks = self.sorted_tasks(workflow)
        if len(sorted_tasks) != len(workflow.tasks):
            raise ValueError("Some tasks are missing")

        for id in sorted_tasks:
            self.enqueue(task=inverted_index[id], workflow_id=workflow.id)

    def enqueue(self, task: Task, workflow_id: str):
        task_id = task.id
        task_dict = task.dict()
        self.log.debug("Enqueuing task", task_id=task_id, workflow_id=workflow_id)

        TaskManager().create_or_update_from_id(
            task_id,
            parent=workflow_id,
            created_at=datetime.now(),
            status=Status.STATUS_PENDING,
            **task_dict,
        )

        message = worker.message_with_options(
            args=(task_dict, workflow_id),
            on_failure=set_failure,
            # options are passed to the actor, including on_failure.
            options={"task_id": task_id, "workflow_id": workflow_id},
        )

        # Determines where to send the message (queue) based on task options.
        queue_name = task.options.queue_name or settings.default_actor_opts.queue_name
        message = message.copy(queue_name=queue_name)

        broker = RabbitmqBroker(url=settings.rabbit_dns)
        broker.enqueue(message)

    def status(self, workflow_id: str) -> WorkflowInDatabase:
        workflow = WorkflowManager(self.db).find_one(id=workflow_id)
        workflow.tasks = TaskManager(self.db).find(parent=workflow_id)
        return workflow

    @staticmethod
    def sorted_tasks(workflow: Workflow) -> list:
        def iterative_topological_sort(graph, start):
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

        ordered_workflow_tasks = iterative_topological_sort(graph, sources)

        return ordered_workflow_tasks
