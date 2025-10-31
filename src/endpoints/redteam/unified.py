"""Unified red team evaluation endpoint."""

import logging
from typing import Dict

from fastapi import APIRouter, BackgroundTasks, HTTPException
from src.core.redteam.job_manager import get_job_manager
from src.core.redteam.schemas import (
    RedTeamJobSummary,
    UnifiedRedTeamJobRequest,
    UnifiedRedTeamJobResults,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/redteam/evaluate", summary="Comprehensive red team evaluation")
async def evaluate_model(
    request: UnifiedRedTeamJobRequest,
    background_tasks: BackgroundTasks
) -> Dict[str, str]:
    """
    Comprehensive red team evaluation with direct testing and optional automated attacks.
    
    This unified endpoint simplifies red teaming:
    - Always runs direct testing (dataset prompts → target model)
    - Optionally runs automated attacks if attacker_model is provided
    - Auto-selects appropriate judge based on dataset
    - Returns unified results with overall safety score
    
    **Direct Testing:**
    - Tests each prompt from the dataset directly against the target model
    - Fast, deterministic
    - Attack type: "{dataset}-direct"
    
    **Automated Attacks (if attacker_model provided):**
    - Uses each prompt as a goal for GOAT attacks
    - Multi-turn conversations
    - Adaptive adversarial prompts
    - Attack type: "goat"
    
    **Progress Tracking:**
    - Real-time progress across all attacks
    - Example: "45/60 attacks (75%)" includes both direct and automated
    
    Args:
        request: Unified job configuration
        background_tasks: FastAPI background tasks manager

    Returns:
        Dict containing the job_id for tracking

    Raises:
        HTTPException: If job creation fails
        
    Example:
        ```
        {
          "target_model": {"model_name": "gpt-3.5-turbo", ...},
          "dataset_source_config": {"source": "jailbreakbench", "limit": 20},
          "attacker_model": {"model_name": "gpt-4", ...},  // Optional
          "judge_model": {"model_name": "gpt-4", ...}      // Optional (auto-selected)
        }
        ```
    """
    try:
        job_manager = get_job_manager()
        job_id = job_manager.create_job(request)

        # Start execution in background
        background_tasks.add_task(job_manager.execute_job, job_id)

        logger.info(f"Started red team evaluation: {job_id}")
        return {
            "status": "success",
            "message": "Red team evaluation started",
            "job_id": job_id
        }

    except Exception as e:
        logger.error(f"Error starting unified evaluation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start evaluation: {str(e)}")


@router.get("/redteam/jobs/{job_id}", summary="Get job status and progress")
async def get_job_status(job_id: str) -> RedTeamJobSummary:
    """
    Get the status and progress of a red team evaluation job.
    
    Shows real-time progress across all attack types.

    Args:
        job_id: Job identifier

    Returns:
        RedTeamJobSummary with current status and progress

    Raises:
        HTTPException: If job not found
    """
    job_manager = get_job_manager()
    job = job_manager.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    logger.info(f"Job {job_id} status: {job.status.value}, progress={job.progress:.1f}%")
    
    summary = job_manager.get_job_summary(job_id)
    return summary


@router.get("/redteam/jobs/{job_id}/results", summary="Get complete results")
async def get_job_results(job_id: str) -> UnifiedRedTeamJobResults:
    """
    Get the complete results of a red team evaluation.
    
    Returns unified results including:
    - Attack results for each approach (direct, automated)
    - Comparison statistics
    - Overall safety score
    - Worst attack type

    Args:
        job_id: Job identifier

    Returns:
        UnifiedRedTeamJobResults with all evaluation results

    Raises:
        HTTPException: If job not found or not yet completed
    """
    job_manager = get_job_manager()
    results = job_manager.get_job_results(job_id)

    if not results:
        job = job_manager.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        raise HTTPException(
            status_code=400,
            detail=f"Job {job_id} has no results yet (status: {job.status.value})"
        )

    return results


@router.delete("/redteam/jobs/{job_id}", summary="Delete a job")
async def delete_job(job_id: str) -> Dict[str, str]:
    """
    Delete a red team evaluation job.
    
    If the job is running, it will be cancelled first.

    Args:
        job_id: Job identifier

    Returns:
        Success message

    Raises:
        HTTPException: If job not found
    """
    job_manager = get_job_manager()
    success = job_manager.delete_job(job_id)

    if not success:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return {
        "status": "success",
        "message": f"Job {job_id} deleted"
    }

