from .worker import set_failure, set_running, set_success, worker

__all__ = [
    "set_failure",
    "set_running",
    "set_success",
    "worker",
]

from dramax.logger.configure_logger import configure_logger

configure_logger()
