from typing import Optional

import docker


def run_container(
    image: str, parameters: Optional[dict], environment: Optional[dict], local_dir: str
) -> str:
    """
    Runs a docker container with the given parameters.

    :param image: The docker image to run.
    :param parameters: The parameters to pass to the container.
    :param environment: The environment variables to pass to the container.
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

    cmd_string = create_cmd_string()

    def create_volumes() -> dict:
        """
        Builds the volumes to mount in the container.
        By default, volumes are `/mnt/inputs/`, `/mnt/outputs/` and `/mnt/shared/`.
        """
        return {
            f"{local_dir}/mnt/inputs": {"bind": "/mnt/inputs/", "mode": "rw"},
            f"{local_dir}/mnt/outputs": {"bind": "/mnt/outputs/", "mode": "rw"},
            f"{local_dir}/mnt/shared": {"bind": "/mnt/shared/", "mode": "rw"},
        }

    container = client.containers.run(
        image=image,
        volumes=create_volumes(),
        command=cmd_string,
        environment=environment,
        detach=True,
        tty=True,
    )
    result = container.wait()
    logs = container.logs().decode("utf-8")

    # Prepend the command string to the logs.
    logs = f"{cmd_string}\n{logs}"

    container.stop()
    container.remove(v=True)

    if result["StatusCode"] != 0:
        error_message = f"Container failed:\n{logs}"
        raise Exception(error_message)

    return logs
