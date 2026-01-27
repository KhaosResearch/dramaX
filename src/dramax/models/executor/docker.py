import docker

from dramax.common.exceptions import DockerExecutionError
from dramax.models.dramatiq.task import Task

from dramax.common.settings import settings

def docker_execute(task: Task, workdir: str) -> str:
    """Runs a docker container with the given parameters.

    :param image: The docker image to run.
    :param parameters: The parameters to pass to the container.
    :param environment: The environment variables to pass to the container.
    :param local_dir: The local directory to mount in the container.
    :return: The logs of the container.
    """
    client = docker.from_env()
    client.login(
        registry = settings.docker_registry,
        username = settings.docker_username,
        password = settings.docker_password
    )

    client.images.pull(
        task.image
    )

    def create_cmd_string() -> str:
        """Builds the command to run in the container."""
        pairs = [
            f"{parameter['name']} {parameter['value']}" for parameter in task.parameters
        ]

        return " ".join(pairs)

    cmd_string = create_cmd_string()

    def create_volumes() -> dict:
        """Builds the volumes to mount in the container.

        By default, volumes are `/mnt/inputs/`, `/mnt/outputs/` and `/mnt/shared/`.
        """
        return {
            f"{workdir}/mnt/inputs": {"bind": "/mnt/inputs/", "mode": "rw"},
            f"{workdir}/mnt/outputs": {"bind": "/mnt/outputs/", "mode": "rw"},
            f"{workdir}/mnt/shared": {"bind": "/mnt/shared/", "mode": "rw"},
        }

    container = client.containers.run(
        image=task.image,
        volumes=create_volumes(),
        command=cmd_string,
        environment=task.environment,
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
        raise DockerExecutionError(error_message)

    return logs
