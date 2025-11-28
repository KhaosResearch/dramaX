from dramax.models.dramatiq.task import Task
from dramax.models.dramatiq.workflow import Workflow
from dramax.worker.scheduler import Scheduler

tasks = [
    Task(
        id="upload_file",
        name="upload_file",
        image="http://192.168.212.38:8002/statistics_timeseries",
        parameters=[
            {
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "auth": ["a-", "a>+'|"],
                "api_key": "s3cr3t",
                "province": 23,
                "municipality": 900,
                "polygon": 35,
                "parcel": 23,
                "precinct": 2,
                "start_date": "2024-01",
                "end_date": "2024-12",
                "indexes": ["ndvi", "evi"],
                "statistics": ["mean"],
            }
        ],
        outputs=[
            {
                "path": "/api/shared/meteo_timeseries.csv",
            },
        ],
    ),
    Task(
        id="sigpac",
        name="sigpac",
        url="http://192.168.212.38:8002/statistics_timeseries",
        parameters=[
            {
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "auth": ["a-", "a>+'|"],
                "api_key": "s3cr3t",
                "province": 23,
                "municipality": 900,
                "polygon": 35,
                "parcel": 23,
                "precinct": 2,
                "start_date": "2024-01",
                "end_date": "2024-12",
                "indexes": ["ndvi", "evi"],
                "statistics": ["mean"],
            }
        ],
        outputs=[
            {
                "path": "/api/shared/meteo_timeseries.csv",
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


# aemet: http://192.168.212.38:8001/meteo_timeseries
# sigpac: http://192.168.212.38:8002/statistics_timeseries
# spei: http://192.168.212.38:8003/spei_timeseries
# yield:  http://192.168.212.38:8004/ds-sarimax-predict
