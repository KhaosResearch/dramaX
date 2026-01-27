from pathlib import Path

import requests
from structlog import get_logger

from dramax.models.dramatiq.task import Task, UnpackedParams


def unpack_parameters(param: dict) -> UnpackedParams:
    log = get_logger()

    headers_key, headers_value = param.get("headers").split(": ")
    auth_param = tuple(param.get("auth").split(":"))

    return UnpackedParams(
        method=param.get("method").replace("'", ""),
        headers={headers_key.replace("'", ""): headers_value.replace("'", "")},
        timeout=param.get("timeout", 10),
        auth=auth_param,
        body={
            k: v.replace("'", "")
            for k, v in param.items()
            if k not in {"method", "headers", "auth", "timeout"}
        },
    )


def api_execute(task: Task, workdir: str) -> str:
    log = get_logger()
    raw_params = {
        p['name']: p['value']
        for p in task.parameters
    }
    unpacked_params = unpack_parameters(raw_params)
    method = unpacked_params.method

    if method == "GET":
        result = get(task, unpacked_params, workdir)
    elif method == "POST":
        result = post(task, unpacked_params, workdir)

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
                file_path = artifact.get_full_path(workdir)
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
        raise


def post(task: Task, unpacked_params: UnpackedParams, workdir: str) -> str:
    log = get_logger("dramax.api_executor.post")
    log = log.bind(url=task.url, method="POST")
    headers = unpacked_params.headers

    try:
        if not unpacked_params.auth:
            message = f"[ERROR] Authentication not provided for {task.url}"
            log.error(message)
            return message

        headers = unpacked_params.headers

        content_type = headers.get("Content-Type").lower()

        if "multipart/form-data" in content_type:
            for artifact in task.inputs:
                file_path = Path(artifact.get_full_path(workdir))

                if not file_path.exists():
                    message = f"[ERROR] File to upload in POST method not found: {file_path}"  #! RAISE EXCEPTION HERE  # noqa: E501, EXE001, EXE003, EXE005
                    log.error(message)
                    return message

                files = {"file": Path.open(file_path, "rb")}
                data = dict(unpacked_params.body.items())

                response = requests.post(
                    task.url,
                    files=files,
                    data=data,
                    auth=unpacked_params.auth,
                    timeout=unpacked_params.timeout,
                )
        else:
            response = requests.post(
                task.url,
                headers=headers,
                auth=unpacked_params.auth,
                json=unpacked_params.body,
                timeout=unpacked_params.timeout,
            )
        response.raise_for_status()

        # PARTE ACTUALIZADA DEL CÓDIGO SIN COMPROBAR
        if task.outputs:
            for artifact in task.outputs:
                file_path = artifact.get_full_path(workdir)
                Path(file_path).parent.mkdir(parents=True, exist_ok=True)

                with Path.open(file_path, "wb") as f:
                    f.write(response.content)

            msg = (
                f"[SUCCESS] POST response saved to {len(task.outputs)} locations "
                f"with status {response.status_code} ({response.reason})"
            )
            log.info(msg)
            return msg

        # PARTE ACTUALIZADA DEL CÓDIGO SIN COMPROBAR

    except requests.RequestException as e:
        message = f"[ERROR] Failed to POST to {task.url}: {e!s}"
        log.exception(message)
        raise
