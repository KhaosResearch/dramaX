from fastapi import APIRouter, Depends, HTTPException
from pymongo.database import Database
from structlog import get_logger

from dramax.models.databases.mongo import get_mongo
from dramax.manager import TaskManager, WorkflowManager
from dramax.models.dramatiq.workflow import ExecutionId, Workflow, WorkflowInDatabase
from dramax.worker.scheduler import Scheduler

log = get_logger("dramax.api.routes.workflow")

router = APIRouter()


@router.post(
    "/run",
    name="Execute workflow",
    tags=["workflow"],
    response_model=ExecutionId,
    response_model_exclude_unset=True,
)
async def run(workflow_request: Workflow) -> ExecutionId:
    """
    Executes a collection of tasks.
    """
    log.debug("Getting workflow request", workflow_request=workflow_request)
    try:
        scheduler = Scheduler()
        scheduler.run(workflow_request)
    except Exception as e:
        log.error("Error executing workflow", error=e)
        raise HTTPException(status_code=500, detail="Error executing workflow") from e
    return ExecutionId(id=workflow_request.id)


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
    log.debug("Getting workflow status", id=id)
    try:
        workflow_in_db = WorkflowManager(db).find_one(id=id)
    except Exception as e:
        log.error("Error getting workflow status", id=id, error=e)
        raise HTTPException(
            status_code=500, detail=f"Error getting workflow {id}"
        ) from e

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
    # TODO: Doesn't work yet.
    workflow = WorkflowManager(db).find_one(id=id)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {id} not found")

    if not workflow.is_revoked:
        WorkflowManager(db).create_or_update_from_id(workflow.id, is_revoked=True)

    return workflow
