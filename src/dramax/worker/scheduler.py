from collections import defaultdict
from datetime import datetime
from typing import Optional

from dramatiq.brokers.rabbitmq import RabbitmqBroker
from pymongo.database import Database
from structlog import get_logger

from dramax.database import get_mongo
from dramax.manager import TaskManager, WorkflowManager
from dramax.models.task import Task, Status
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
        WorkflowManager(self.db).create_or_update_from_id(
            workflow.id,
            metadata=workflow.metadata.dict(),
            created_at=datetime.now(),
            status=WorkflowStatus.STATUS_PENDING,
        )

        # Add workflow metadata to tasks.
        for task in workflow.tasks:
            task.metadata.update(workflow.metadata.dict())

        inverted_index = {}
        for task in workflow.tasks:
            inverted_index[task.name] = task

        # Enqueue tasks in execution order.
        sorted_tasks = self.sorted_tasks(workflow)
        if len(sorted_tasks) != len(workflow.tasks):
            raise ValueError("Some tasks have missing dependencies.")
        for name in sorted_tasks:
            self.enqueue(task=inverted_index[name], workflow_id=workflow.id)

    def enqueue(self, task: Task, workflow_id: str):
        """
        Send task request to main `dramax` actor.
        """
        task_dict = task.dict()

        self.log.debug("Enqueuing task", task=task_dict, workflow_id=workflow_id)

        # Triggers actor execution and sets failure callback.
        message = worker.message_with_options(
            args=(task_dict, workflow_id),
            on_failure=set_failure,
        )

        # Determines where to send the message (queue) based on task options.
        queue_name = task.options.queue_name or settings.default_actor_opts.queue_name
        message = message.copy(queue_name=queue_name)

        # Actually enqueues the message.
        broker = RabbitmqBroker(url=settings.rabbit_dns)
        message = broker.enqueue(message)

        # Creates task in database.
        task_id = message.message_id
        TaskManager().create_or_update_from_id(
            task_id,
            name=task.name,
            label=task.label,
            parent=workflow_id,
            image=task.image,
            parameters=[param.dict() for param in task.parameters],
            inputs=task.inputs,
            outputs=task.outputs,
            options=task.options.dict(),
            metadata=task.metadata,
            status=Status.STATUS_PENDING,
            created_at=datetime.now(),
        )
        self.log.debug("Task created", task_id=task_id, workflow_id=workflow_id)

    def status(self, workflow_id: str) -> WorkflowInDatabase:
        workflow = WorkflowManager(self.db).find_one(id=workflow_id)
        workflow.tasks = TaskManager(self.db).find(parent=workflow_id)
        return workflow

    def revoke(self, workflow_id: str) -> None:
        """
        Cancel workflow execution.
        """
        WorkflowManager().create_or_update_from_id(workflow_id, updated_at=datetime.now(), is_revoked=True)

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
                sources.append(task.name)
            else:
                for task_input in task.inputs:
                    task_input_name = task_input["path"].split("/")[0]
                    # TODO: Check if task_input_name is in workflow.tasks.
                    graph[task_input_name].append(task.name)

        ordered_workflow_tasks = iterative_topological_sort(graph, sources)

        return ordered_workflow_tasks

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
