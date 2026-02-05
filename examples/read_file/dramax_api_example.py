import requests

json = {
    "id": "string",
    "label": "Mi_Workflow_Meteo_Prediccion",
    "tasks": [
        {
            "id": "aemettimeseries",
            "name": "aemettimeseries",
            "url": "http://192.168.219.56:8001/meteo_timeseries",
            "parameters": [
                {
                    "method": "POST",
                    "headers": {"Content-Type": "application/json"},
                    "auth": ["a-", "a>+|"],
                    "api_key": "s3cr3t",
                    "aemet_key": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmZ2FyY2lhY29ib0B1bWEuZXMiLCJqdGkiOiI2MjA2NDc1NC03MWYyLTRjMTQtOTdlMC03NjI3ZDY3MGYzNjQiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc1ODY5NjY3NywidXNlcklkIjoiNjIwNjQ3NTQtNzFmMi00YzE0LTk3ZTAtNzYyN2Q2NzBmMzY0Iiwicm9sZSI6IiJ9.ZctU7Yc6NdXocdPEBWwBU3GlFpM35upeAWrDyJvn1s0",
                    "province": 23,
                    "municipality": 900,
                    "polygon": 35,
                    "parcel": 23,
                    "start_date": "2000-01-15",
                    "end_date": "2024-12-01",
                }
            ],
            "outputs": [{"path": "/api/shared/meteo_timeseries.csv"}],
        },
        {
            "id": "yieldpredict",
            "name": "yieldpredict",
            "url": "http://192.168.219.56:8004/ds-sarimax-predict",
            "parameters": [
                {
                    "method": "POST",
                    "headers": {"Content-Type": "multipart/form-data"},
                    "auth": ["a-", "a>+|"],
                    "target_col": "PesoNetoArticulo",
                    "date_col": "fecha",
                    "config": "Soft",
                    "delimiter": ",",
                }
            ],
            "inputs": [{"path": "/api/shared/meteo_timeseries.csv"}],
            "outputs": [{"path": "/api/shared/yield_predict.csv"}],
            "depends_on": ["aemettimeseries"],
        },
    ],
    "metadata": {"author": "anonymous"},
}

response = requests.post(
    url="http://localhost:8005/api/v2/workflow/run",
    json=json,
)

print(response.text)

# aemet: http://192.168.219.56:8001/meteo_timeseries
# sigpac: http://192.168.219.56:8002/statistics_timeseries
# spei: http://192.168.219.56:8003/spei_timeseries
# yield:  http://192.168.219.56:8004/ds-sarimax-predict
