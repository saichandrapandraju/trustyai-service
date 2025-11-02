"""Unified red team evaluation endpoint."""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from src.core.redteam.job_manager_v2 import get_job_manager
from src.core.redteam.schemas import (
    RedTeamJobStatus,
    RedTeamJobSummary,
    UnifiedRedTeamJobRequest,
    UnifiedRedTeamJobResults,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/redteam/evaluate", summary="Comprehensive red team evaluation")
async def evaluate_model(
    request: UnifiedRedTeamJobRequest
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
        import asyncio
        
        job_manager = get_job_manager()
        job_id = job_manager.create_job(request)
        
        # Get the job to store task reference
        job = job_manager.get_job(job_id)
        if not job:
            raise HTTPException(status_code=500, detail="Failed to create job")

        # Create task (so we can cancel it later)
        task = asyncio.create_task(job_manager.execute_job(job_id))
        
        # Store task reference in job for cancellation
        with job._lock:
            job.task = task

        logger.info(f"Started red team evaluation: {job_id}")
        return {
            "status": "success",
            "message": "Red team evaluation started",
            "job_id": job_id
        }

    except Exception as e:
        logger.error(f"Error starting unified evaluation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start evaluation: {str(e)}")


@router.get("/redteam/jobs", summary="List all red team jobs")
async def list_jobs() -> Dict[str, List[RedTeamJobSummary] | int]:
    """
    List all red team evaluation jobs.
    
    Returns a list of all jobs with their current status and progress.
    
    Returns:
        Dict containing list of job summaries
    """
    job_manager = get_job_manager()
    jobs = job_manager.list_jobs()
    
    logger.info(f"Listed {len(jobs)} jobs")
    return {
        "jobs": jobs,
        "total": len(jobs)
    }


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


@router.get("/redteam/jobs/{job_id}/results", summary="Get aggregated results")
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


@router.get("/redteam/jobs/{job_id}/details", summary="Get detailed per-prompt results")
async def get_job_details(
    job_id: str,
    attack_type: Optional[str] = None,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    Get detailed per-prompt/conversation results for a job.
    
    Returns individual test results with prompts, responses, and evaluations.
    This is useful for:
    - Manual review of specific failures
    - Understanding attack patterns
    - Debugging evaluation logic
    - Showing examples to stakeholders
    
    Args:
        job_id: Job identifier
        attack_type: Optional filter by attack type (e.g., "jailbreak-direct")
        limit: Optional limit on number of results returned
    
    Returns:
        Dict with detailed results per test
        
    Raises:
        HTTPException: If job not found or not yet completed
    """
    job_manager = get_job_manager()
    job = job_manager.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    if job.status != RedTeamJobStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Job {job_id} not yet completed (status: {job.status.value})"
        )
    
    # Collect all detailed results
    detailed_results = []
    
    for vector_result in job.vector_results:
        for test_result in vector_result.detailed_results:
            # Filter by attack_type if specified
            full_attack_type = f"{vector_result.vector_id}-{test_result.method}"
            if attack_type and full_attack_type != attack_type:
                continue
            
            # Build detailed result entry
            result_entry = {
                "attack_type": full_attack_type,
                "vector": vector_result.vector_id,
                "method": test_result.method,
                "prompt": test_result.prompt,
                "original_prompt": test_result.original_prompt,
                "response": test_result.response,
                "is_safe": test_result.is_safe,
                "score": test_result.score,
                "reasoning": test_result.reasoning,
                "dataset_id": test_result.dataset_id
            }
            
            # Add conversation history for dynamic attacks
            if test_result.conversation:
                result_entry["conversation"] = [
                    {
                        "turn": idx + 1,
                        "attacker_prompt": turn.attacker_prompt,
                        "target_response": turn.target_response,
                        "judge_evaluation": {
                            "attack_success": turn.judge_evaluation.attack_success,
                            "confidence": turn.judge_evaluation.confidence,
                            "reasoning": turn.judge_evaluation.reasoning
                        }
                    }
                    for idx, turn in enumerate(test_result.conversation)
                ]
            
            detailed_results.append(result_entry)
    
    # Apply limit if specified
    if limit and limit > 0:
        detailed_results = detailed_results[:limit]
    
    # Group by attack type for easier navigation
    by_attack_type = {}
    for result in detailed_results:
        attack_type_key = result["attack_type"]
        if attack_type_key not in by_attack_type:
            by_attack_type[attack_type_key] = []
        by_attack_type[attack_type_key].append(result)
    
    return {
        "job_id": job_id,
        "total_results": len(detailed_results),
        "filtered": attack_type is not None,
        "filter": attack_type,
        "limit_applied": limit if limit else None,
        "results": detailed_results,
        "by_attack_type": by_attack_type
    }


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

