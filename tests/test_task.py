from dramax.models.task import Task
import pytest
from pydantic import ValidationError


def test_task_name_with_spaces():
    with pytest.raises(ValidationError):
        Task(name="test task", image="busybox")


def test_task_name_with_dots():
    with pytest.raises(ValidationError):
        Task(name="test.task", image="busybox")


def test_task_name_without_spaces_and_dots():
    try:
        Task(name="test", image="test_image.png")
    except ValidationError:
        pytest.fail("ValidationError was not expected")


def test_task_inputs_without_slash():
    with pytest.raises(ValidationError):
        Task(name="test", image="busybox", inputs=[{"path": "taskName.rest.of.path"}])


def test_task_inputs_with_slash():
    try:
        Task(name="test", image="busybox", inputs=[{"path": "taskName/rest/of/path"}])
    except ValidationError:
        pytest.fail("ValidationError was not expected")
