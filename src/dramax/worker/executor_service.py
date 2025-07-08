from dramax.models.dramatiq.task import Task


def run_task(task: Task) -> str:
    """
    Executes a task using the task provided executor.

    Parameters:
    - task: A Task instance containing all the necessary data for execution.

    Returns:
    - A string containing the logs generated during the task execution.
    """
    return task.executor.execute()
