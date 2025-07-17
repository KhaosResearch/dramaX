from pathlib import Path

import requests
from structlog import get_logger

from dramax.models.dramatiq.task import Task, UnpackedParams


def unpack_parameters(param: dict) -> UnpackedParams:
    return UnpackedParams(
        method=param.get("method"),
        headers=param.get("headers"),
        timeout=param.get("timeout"),
        auth=param.get("auth"),
        body={
            k: v
            for k, v in param.items()
            if k not in {"method", "headers", "auth", "timeout"}
        },
    )


def api_execute(task: Task, workdir: str) -> str:
    unpacked_params = unpack_parameters(task.parameters)
    method = unpacked_params.method.upper()
    if method == "GET":
        result = get(unpacked_params, workdir)
    elif method == "POST":
        result = post(unpacked_params, workdir)
    return result


def get(task: Task, unpacked_params: UnpackedParams, workdir: str) -> str:
    log = get_logger("dramax.api_executor.get")
    log.bind(url=task.url, method="GET")
    try:
        if unpacked_params.auth:
            response = requests.get(
                task.url,
                headers=unpacked_params.headers,
                timeout=unpacked_params.timeout,
                auth=unpacked_params.auth,
            )
            response.raise_for_status()

            if not task.outputs:
                message = (
                    f"[WARNING] File downloaded with status {response.status_code} "
                    f"({response.reason}), but no output dir specified. File not saved."
                )
                log.warning(message)
                return message

            for artifact in task.outputs:
                artifact.base = workdir
                file_path = artifact.get_full_path()
                Path(file_path).parent.mkdir(parents=True, exist_ok=True)

                with Path.open(file_path, "wb") as f:
                    f.write(response.content)

                msg = (
                    f"[SUCCESS] File downloaded with status {response.status_code} "
                    f"({response.reason}) and saved to {file_path}"
                )
                log.info(msg)

            return (
                f"[SUCCESS] File downloaded and saved to {len(task.outputs)} locations."
            )
        message = "[ERROR] Authentication not provided."
        log.error(message)
        return message  # noqa: TRY300

    except requests.RequestException as e:
        message = f"[ERROR] Failed to download file from {task.url}: {e!s}"
        log.exception(message)
        return message


def post(task: Task, unpacked_params: UnpackedParams, workdir: str) -> str:
    log = get_logger("dramax.api_executor.post")
    log = log.bind(url=task.url, method="POST")

    try:
        if not unpacked_params.auth:
            message = f"[ERROR] Authentication not provided for {task.url}"
            log.error(message)
            return message

        headers = unpacked_params.headers or {}

        content_type = headers.get("Content-Type", "").lower()

        if "multipart/form-data" in content_type:
            for artifact in task.inputs:
                artifact.base = workdir
                file_path = Path(artifact.get_object_name())

                if not file_path.exists():
                    message = (
                        f"[ERROR] File to upload in POST method not found: {file_path}"
                    )
                    log.error(message)
                    return message

                files = {"file": Path.open(file_path, "rb")}
                data = dict(unpacked_params.body.items())

                response = requests.post(
                    task.url,
                    files=files,
                    data=data,
                    headers=unpacked_params.headers,
                    auth=unpacked_params.auth,
                    timeout=unpacked_params.timeout,
                )
        else:
            response = requests.post(
                task.url,
                headers=headers,
                auth=unpacked_params.auth,
                data=unpacked_params.body,
                timeout=unpacked_params.timeout,
            )
        response.raise_for_status()

        if files:
            files["file"].close()

        message = (
            f"[SUCCESS] POST request completed with status {response.status_code} "
            f"({response.reason})"
        )
        log.info(message)
        return message  # noqa: TRY300

    except requests.RequestException as e:
        message = f"[ERROR] Failed to POST to {task.url}: {e!s}"
        log.exception(message)
        return message
