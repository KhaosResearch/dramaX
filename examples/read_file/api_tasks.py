from dramax.models.dramatiq.task import Task
from dramax.models.dramatiq.workflow import Workflow
from dramax.worker.scheduler import Scheduler

tasks = [
    Task(
        id="aemettimeseries",
        name="aemettimeseries",
        url="http://192.168.219.56:8001/meteo_timeseries",
        parameters=[
            {
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "auth": ["a-", "a>+'|"],
                "api_key": "s3cr3t",
                "aemet_key": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmZ2FyY2lhY29ib0B1bWEuZXMiLCJqdGkiOiI2MjA2NDc1NC03MWYyLTRjMTQtOTdlMC03NjI3ZDY3MGYzNjQiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc1ODY5NjY3NywidXNlcklkIjoiNjIwNjQ3NTQtNzFmMi00YzE0LTk3ZTAtNzYyN2Q2NzBmMzY0Iiwicm9sZSI6IiJ9.ZctU7Yc6NdXocdPEBWwBU3GlFpM35upeAWrDyJvn1s0",
                "province": 23,
                "municipality": 900,
                "polygon": 35,
                "parcel": 23,
                "start_date": "2000-01-15",
                "end_date": "2024-12-01",
            },
        ],
        outputs=[
            {
                "path": "/api/shared/meteo_timeseries.csv",
            },
        ],
    ),
    Task(
        id="yieldpredict",
        name="yieldpredict",
        url="http://192.168.219.56:8004/ds-sarimax-predict",
        parameters=[
            {
                "method": "POST",
                "headers": {"Content-Type": "multipart/form-data"},
                "auth": ["a-", "a>+'|"],
                "target_col": "PesoNetoArticulo",
                "date_col": "fecha",
                "config": "Soft",
                "delimiter": ",",
            },
        ],
        inputs=[
            {
                "path": "/api/shared/meteo_timeseries.csv",
            },
        ],
        outputs=[
            {
                "path": "/api/shared/yield_predict.csv",
            },
        ],
        depends_on=["aemettimeseries"],
    ),
]

workflow = Workflow(
    tasks=tasks,
    metadata={"author": "anonymous", "other-key": "other-value"},
)

scheduler = Scheduler()
scheduler.run(workflow)


# aemet: http://192.168.219.56:8001/meteo_timeseries
# sigpac: http://192.168.219.56:8002/statistics_timeseries
# spei: http://192.168.219.56:8003/spei_timeseries
# yield:  http://192.168.219.56:8004/ds-sarimax-predict
