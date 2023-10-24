from typing import Optional

import docker


def run_container(image: str, parameters: Optional[dict], local_dir: str) -> str:
    """
    Runs a docker container with the given parameters.

    :param image: The docker image to run.
    :param parameters: The parameters to pass to the docker image.
    :param local_dir: The local directory to mount in the container.
    :return: The logs of the container.
    """
    client = docker.from_env()

    def create_cmd_string() -> str:
        """
        Builds the command to run in the container.
        """
        pairs = []
        for parameter in parameters:
            pairs.append(f"{parameter['name']} {parameter['value']}")
        return " ".join(pairs)

    container = client.containers.run(
        image=image,
        volumes={local_dir: {"bind": "/mnt/shared", "mode": "rw"}},
        command=create_cmd_string(),
        detach=True,
        tty=True,
    )
    result = container.wait()
    logs = container.logs().decode("utf-8")

    container.stop()
    container.remove(v=True)

    if result["StatusCode"] != 0:
        raise Exception("Container failed to run: " + logs)

    return logs
