from typing import Any, Literal

import docker

from dramax.exceptions import DockerExecutionError

from .base import Executor


class DockerExecutor(Executor):
    type: Literal["docker"] = "docker"
    image: str
    label: str = "latest"
    environment: dict[str, Any] | None
    binding_dir: str | None
    command: list[dict[str, str]] | None

    def execute(
        self,
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
            pairs = [
                f"{parameter['name']} {parameter['value']}"
                for parameter in self.command
            ]
            return " ".join(pairs)

        cmd_string = create_cmd_string()

        def create_volumes() -> dict:
            """
            Builds the volumes to mount in the container.
            By default, volumes are `/mnt/inputs/`, `/mnt/outputs/` and `/mnt/shared/`.
            """
            return {
                f"{self.binding_dir}/mnt/inputs": {
                    "bind": "/mnt/inputs/",
                    "mode": "rw",
                },
                f"{self.binding_dir}/mnt/outputs": {
                    "bind": "/mnt/outputs/",
                    "mode": "rw",
                },
                f"{self.binding_dir}/mnt/shared": {
                    "bind": "/mnt/shared/",
                    "mode": "rw",
                },
            }

        container = client.containers.run(
            image=self.image,
            volumes=create_volumes(),
            command=cmd_string,
            environment=self.environment,
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
