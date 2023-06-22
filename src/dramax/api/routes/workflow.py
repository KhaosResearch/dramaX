from fastapi import APIRouter, Depends, HTTPException
from pymongo.database import Database
from structlog import get_logger

from dramax.database import get_mongo
from dramax.manager import TaskManager, WorkflowManager
from dramax.models.workflow import Workflow, WorkflowInDatabase
from dramax.worker.scheduler import Scheduler

log = get_logger("dramax.api.routes.workflow")

router = APIRouter()


@router.post(
    "/run",
    name="Execute workflow",
    tags=["workflow"],
    response_model=str,
    response_model_exclude_unset=True,
)
async def run(workflow_request: Workflow) -> str:
    """
    Executes a collection of tasks.
    """
    log.debug("Got workflow request", workflow_request=workflow_request)
    with Scheduler() as scheduler:
        scheduler.run(workflow_request)
    return workflow_request.id


@router.get(
    "/status",
    name="Get workflow execution status",
    tags=["workflow"],
    response_model=WorkflowInDatabase,
)
async def status(
    id: str,
    db: Database = Depends(get_mongo),
) -> WorkflowInDatabase:
    """
    Returns execution status from execution id.
    """
    workflow_in_db = WorkflowManager(db).find_one(id=id)
    if not workflow_in_db:
        raise HTTPException(status_code=404, detail=f"Workflow {id} not found")
    workflow_in_db.tasks = TaskManager(db).find(parent=id)
    return workflow_in_db


@router.post(
    "/revoke",
    name="Cancel workflow execution",
    tags=["workflow"],
    response_model=WorkflowInDatabase,
    response_model_exclude_unset=True,
)
async def cancel_or_revoke(
    id: str,
    db=Depends(get_mongo),
) -> WorkflowInDatabase:
    """
    Revokes the execution of a workflow, i.e., cancel the execution of pending tasks.
    """
    workflow = WorkflowManager(db).find_one(id=id)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {id} not found")

    if not workflow.is_revoked:
        WorkflowManager(db).create_or_update_from_id(workflow.id, is_revoked=True)

    return workflow
