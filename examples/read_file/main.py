from dramax.models.task import Task
from dramax.models.workflow import Workflow
from dramax.worker.scheduler import Scheduler

tasks = [
    Task(
        id="t1",
        name="first_task",
        image="busybox",
        parameters=[
            {
                "name": "wget",
                "value": "-P /mnt/shared/ https://raw.githubusercontent.com/solidsnack/tsv/master/cities10.tsv",
            }
        ],
        outputs=[
            {
                "path": "/mnt/shared/cities10.tsv",
            }
        ],
    ),
    Task(
        id="t2",
        name="second_task",
        image="busybox",
        parameters=[{"name": "cat", "value": "/mnt/shared/input.tsv"}],
        inputs=[
            {
                "path": "/mnt/shared/input.tsv",
                "source": "t1",
                "sourcePath": "/mnt/shared/cities10.tsv",
            }
        ],
        depends_on=["t1"],
    ),
]

workflow = Workflow(
    tasks=tasks,
    metadata={"author": "anonymous", "other-key": "other-value"},
)

print("Workflow JSON:")
print(workflow.json(indent=2))

scheduler = Scheduler()
scheduler.run(workflow)
