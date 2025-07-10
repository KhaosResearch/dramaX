from dramax.models.dramatiq.task import Task
from dramax.models.dramatiq.workflow import Workflow
from dramax.models.executor import DockerExecutor
from dramax.worker.scheduler import Scheduler

docker_cmd_t1 = [
    {
        "name": "wget",
        "value": "--output-document /mnt/shared/cities10.tsv https://raw.githubusercontent.com/solidsnack/tsv/master/cities10.tsv",
    },
]


tasks = [
    Task(
        id="t1",
        name="first_task",
        executor=DockerExecutor(type="docker", image="busybox"),
        parameters=docker_cmd_t1,
        outputs=[
            {
                "path": "/mnt/shared/cities10.tsv",
            },
        ],
        on_fail_remove_local_dir=False,
    ),
    Task(
        id="t2",
        name="second_task",
        executor=DockerExecutor(
            image="busybox",
        ),
        parameters=[{"name": "cat", "value": "/mnt/shared/input.tsv"}],
        inputs=[
            {
                "path": "/mnt/shared/input.tsv",
                "source": "t1",
                "sourcePath": "/mnt/shared/cities10.tsv",
            },
        ],
        depends_on=["t1"],
        on_fail_remove_local_dir=False,
    ),
]

workflow = Workflow(
    tasks=tasks,
    metadata={"author": "anonymous", "other-key": "other-value"},
)

scheduler = Scheduler()
scheduler.run(workflow)
