from dramax.models.dramatiq.task import Task
from dramax.models.dramatiq.workflow import Workflow
from dramax.models.executor import APIExecutor
from dramax.worker.scheduler import Scheduler

tasks = [
    Task(
        id="t1",
        name="first_task",
        executor=APIExecutor(
            url="",
            method="POST",
            headers={"Content-Type": "multipart/form-data"},
            body={},
        ),
        inputs=[
            {
                "path": "/mnt/shared/input.tsv",
                "source": "t1",
                "sourcePath": "/mnt/shared/cities10.tsv",
            },
        ],
    ),
]

workflow = Workflow(
    tasks=tasks,
    metadata={"author": "anonymous", "other-key": "other-value"},
)

scheduler = Scheduler()
scheduler.run(workflow)
