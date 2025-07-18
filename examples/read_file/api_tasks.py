from dramax.models.dramatiq.task import Task
from dramax.models.dramatiq.workflow import Workflow
from dramax.worker.scheduler import Scheduler

tasks = [
    Task(
        id="t1",
        name="first_task",
        url="http://localhost:8002/download_csv",
        parameters=[
            {
                "method": "GET",
                "headers": {"Content-Type": "text/csv"},
                "auth": ["user-9753", "Hapy>+'|"],
            }
        ],
        outputs=[
            {
                "path": "/api/shared/data.csv",
            },
        ],
    ),
    Task(
        id="t2",
        name="second_task",
        url="http://localhost:8002/calculate_spei",
        parameters=[
            {
                "method": "POST",
                "headers": {"Content-Type": "multipart/form-data"},
                "auth": ["user-9753", "Hapy>+'|"],
            }
        ],
        inputs=[
            {
                "path": "/api/shared/data.csv",
                "source": "t1",
                "sourcePath": "/api/shared/data.csv",
            },
        ],
        outputs=[
            {
                "path": "/api/shared/data.csv",
            },
        ],
        depends_on=["t1"],
    ),
]

workflow = Workflow(
    tasks=tasks,
    metadata={"author": "anonymous", "other-key": "other-value"},
)

scheduler = Scheduler()
scheduler.run(workflow)
