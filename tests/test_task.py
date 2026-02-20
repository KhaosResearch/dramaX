import pytest
from pydantic import ValidationError

from dramax.models.dramatiq.task import Task


def test_task_name_with_spaces():
    with pytest.raises(ValidationError):
        Task(id="test", name="test task", image="busybox")


def test_task_name_with_dots():
    with pytest.raises(ValidationError):
        Task(id="test", name="test.task", image="busybox")


def test_task_name_without_spaces_and_dots():
    try:
        Task(id="test", name="test", image="test_image.png")
    except ValidationError:
        pytest.fail("ValidationError was not expected")


def test_task_inputs_with_slash():
    try:
        Task(
            id="test",
            name="test",
            image="busybox",
            inputs=[{"path": "taskName/rest/of/path"}],
        )
    except ValidationError:
        pytest.fail("ValidationError was not expected")
