from dramax.models.task import Task
from dramax.models.workflow import Workflow
from dramax.worker.scheduler import Scheduler

tasks = [
    Task(
        name="t1",
        image="busybox",
        parameters=[
            {
                "name": "wget",
                "value": "-P /mnt/outputs https://raw.githubusercontent.com/solidsnack/tsv/master/cities10.tsv",
            }
        ],
        outputs=[
            {
                "name": "cities10.tsv",
            }
        ],
    ),
    Task(
        name="t2",
        image="busybox",
        parameters=[{"name": "cat", "value": "/mnt/inputs/df.tsv"}],
        inputs=[
            {
                "path": "t1/outputs/cities10.tsv",
                "name": "df.tsv",
            }
        ],
    ),
]

workflow = Workflow(
    tasks=tasks,
    metadata={"author": "anonymous", "other-key": "other-value"},
)

print("Workflow JSON:")
print(workflow.json(indent=2))

with Scheduler() as scheduler:
    scheduler.run(workflow)
