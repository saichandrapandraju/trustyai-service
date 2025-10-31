"""Job management for red team evaluations."""

import asyncio
import logging
import threading
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union

from src.core.redteam.judge import LLMJudge
from src.core.redteam.llm_interface import get_llm
from src.core.redteam.schemas import (
    AttackResult,
    CategoryStats,
    ComparisonStats,
    DynamicAttackConfig,
    DynamicRedTeamJobResults,
    RedTeamAttackResult,
    RedTeamJobResults,
    RedTeamJobStatus,
    RedTeamJobSummary,
    RedTeamPrompt,
    SessionSummary,
    StaticRedTeamDataset,
    StaticRedTeamJobRequest,
    UnifiedRedTeamJobRequest,
    UnifiedRedTeamJobResults,
)

logger = logging.getLogger(__name__)


class RedTeamJob:
    """Represents a red team evaluation job."""

    def __init__(
        self,
        job_id: str,
        request: UnifiedRedTeamJobRequest
    ) -> None:
        self.job_id = job_id
        self.request = request
        self.status = RedTeamJobStatus.QUEUED
        self.started_at: Optional[str] = None
        self.completed_at: Optional[str] = None
        self.progress: float = 0.0
        
        # Job data
        self.attack_results: List[AttackResult] = []
        self.dataset_info: Optional[Dict] = None
        
        # Real-time tracking (for preliminary_stats)
        self.total_attacks_planned: int = 0
        self.attacks_completed: int = 0
        self.successful_attacks: int = 0
        
        self.error: Optional[str] = None
        self.task: Optional[asyncio.Task] = None
        self._lock = threading.Lock()  # For thread-safe updates

    def get_summary(self) -> RedTeamJobSummary:
        """Get job summary with thread-safe access."""
        with self._lock:
            preliminary_stats = {}
            
            # Use real-time counters if job is running
            if self.status == RedTeamJobStatus.RUNNING and self.attacks_completed > 0:
                preliminary_stats = {
                    "attacks_completed": self.attacks_completed,
                    "total_attacks_planned": self.total_attacks_planned,
                    "successful_attacks": self.successful_attacks,
                    "overall_success_rate": self.successful_attacks / self.attacks_completed if self.attacks_completed > 0 else 0.0
                }
            # Use final attack_results if job is complete
            elif self.attack_results:
                total_attacks = sum(r.total_attempts for r in self.attack_results)
                successful_attacks = sum(r.successful_attempts for r in self.attack_results)
                preliminary_stats = {
                    "attacks_completed": len(self.attack_results),
                    "total_attacks": total_attacks,
                    "successful_attacks": successful_attacks,
                    "overall_success_rate": successful_attacks / total_attacks if total_attacks > 0 else 0.0
                }

            return RedTeamJobSummary(
                job_id=self.job_id,
                status=self.status,
                started_at=self.started_at or "",
                completed_at=self.completed_at,
                progress=self.progress,
                preliminary_stats=preliminary_stats
            )
    
    def update_progress(self, results_count: int, total: int) -> None:
        """Update job progress in a thread-safe manner."""
        with self._lock:
            self.progress = (results_count / total) * 100 if total > 0 else 0.0
        logger.debug(f"Updated job {self.job_id} progress: {self.progress:.1f}% ({results_count}/{total})")


class RedTeamJobManager:
    """Manages red team evaluation jobs."""

    def __init__(self) -> None:
        self.jobs: Dict[str, RedTeamJob] = {}
        self.lock = threading.Lock()

    def create_job(self, request: UnifiedRedTeamJobRequest) -> str:
        """
        Create a new red team job.

        Args:
            request: Job request configuration

        Returns:
            Job ID
        """
        job_id = str(uuid.uuid4())
        job = RedTeamJob(job_id, request)

        with self.lock:
            self.jobs[job_id] = job

        logger.info(f"Created red team job: {job_id}")
        return job_id


    async def execute_job(self, job_id: str) -> None:
        """
        Execute a red team evaluation job (direct + optional automated attacks).

        Args:
            job_id: Job identifier
        """
        from src.core.redteam.datasets.dataset_registry import DatasetRegistry
        from src.core.redteam.strategies.goat import GOATSessionRunner
        
        logger.info(f"execute_job called for {job_id}")
        
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                logger.error(f"Job {job_id} not found")
                return

        try:
            # Validate request
            job.request.validate_inputs()
            
            with job._lock:
                job.status = RedTeamJobStatus.RUNNING
                job.started_at = datetime.now(timezone.utc).isoformat()
            logger.info(f"Starting red team job: {job_id}")

            # Load dataset based on input method
            if job.request.attack_vectors:
                # Vector-based: Load datasets for specified vectors
                from src.core.redteam.attack_vectors.registry import VectorRegistry
                
                logger.info(f"Loading datasets for vectors: {job.request.attack_vectors}")
                dataset_ids = VectorRegistry.get_all_datasets_for_vectors(job.request.attack_vectors)
                
                # Load and merge datasets
                datasets: List[StaticRedTeamDataset] = []
                for dataset_id in dataset_ids:
                    from src.core.redteam.datasets.dataset_sources import DatasetSourceConfig
                    
                    # All dataset_ids from vectors are builtin references
                    config = DatasetSourceConfig(
                        source="builtin",
                        dataset_id=dataset_id
                    )
                    ds = DatasetRegistry.load_dataset(config)
                    datasets.append(ds)
                
                # Merge datasets
                all_prompts = []
                dataset_sources = []
                for ds in datasets:
                    all_prompts.extend(ds.prompts)
                    # Track original sources
                    if "source" in ds.metadata:
                        dataset_sources.append(ds.metadata["source"])
                
                # Preserve primary source (use first dataset's source for judge selection)
                primary_source = dataset_sources[0] if dataset_sources else "custom"
                
                # Create merged dataset
                from src.core.redteam.schemas import StaticRedTeamDataset
                dataset = StaticRedTeamDataset(
                    dataset_id=f"vectors-{'-'.join(job.request.attack_vectors)}",
                    name=f"Attack Vectors: {', '.join(job.request.attack_vectors)}",
                    description=f"Comprehensive testing for {len(job.request.attack_vectors)} attack vectors",
                    version="1.0.0",
                    prompts=all_prompts,
                    metadata={
                        "source": primary_source,  # Preserve original source for judge selection
                        "attack_vectors": job.request.attack_vectors,
                        "dataset_sources": dataset_sources  # All sources for reference
                    }
                )
                
                logger.info(
                    f"Loaded {len(all_prompts)} prompts from {len(datasets)} datasets "
                    f"for vectors: {job.request.attack_vectors}"
                )
            else:
                # Dataset-based: Direct specification
                dataset = DatasetRegistry.load_dataset(job.request.dataset_source_config)
                logger.info(f"Loaded dataset: {dataset.name} ({len(dataset.prompts)} prompts)")
            
            # Store dataset info
            job.dataset_info = {
                "dataset_id": dataset.dataset_id,
                "name": dataset.name,
                "source": dataset.metadata.get("source", "custom"),
                "total_prompts": len(dataset.prompts),
                "vectors": dataset.metadata.get("vectors", [])
            }

            # Log dataset metadata for debugging
            logger.info(f"Dataset metadata for judge selection: {dataset.metadata}")

            # Initialize target model
            target_model = get_llm(job.request.target_model)
            
            # Auto-select judge
            from src.core.redteam.judge import auto_select_judge
            judge: LLMJudge = None
            judge, judge_type = auto_select_judge(
                dataset.metadata,
                job.request.judge_model,
                job.request.evaluation_criteria
            )
            logger.info(f"Using {judge_type} judge for evaluation")
            
            # Health check target
            target_healthy = await target_model.health_check()
            if not target_healthy:
                raise ValueError(f"Target model health check failed")
            judge_healthy = await judge.health_check()
            if not judge_healthy:
                raise ValueError(f"Judge health check failed")
            
            logger.info(f"Health checks passed")

            # Determine converters to use
            from src.core.redteam.converters.registry import ConverterRegistry
            
            converters_to_test = []
            if job.request.converters == "all":
                converters_to_test = ConverterRegistry.get_all_converter_ids()
                logger.info(f"Will test with ALL converters: {converters_to_test}")
            elif job.request.converters:
                converters_to_test = job.request.converters
                logger.info(f"Will test with converters: {converters_to_test}")
            else:
                logger.info("No converters specified - testing original prompts only")
            
            # Calculate total work for progress tracking
            # Each prompt tested: 1 (original) + len(converters) times
            variants_per_prompt = 1 + len(converters_to_test)
            total_direct_tests = len(dataset.prompts) * variants_per_prompt
            
            total_automated_sessions = 0
            if job.request.attacker_model:
                # Automated attacks use original prompts only (not converted)
                total_automated_sessions = len(dataset.prompts) * job.request.num_sessions
            
            total_work = total_direct_tests + total_automated_sessions
            completed_work = 0
            
            # Set total planned for preliminary_stats
            job.total_attacks_planned = total_work
            
            logger.info(
                f"Will run {total_direct_tests} direct tests "
                f"({len(dataset.prompts)} prompts x {variants_per_prompt} variants) "
                f"+ {total_automated_sessions} automated sessions = {total_work} total attacks"
            )

            # PHASE 1: Run direct testing (with optional converters)
            logger.info(f"Starting direct testing...")
            
            # Track results per variant type
            direct_results_by_variant = {"original": []}
            for converter_id in converters_to_test:
                direct_results_by_variant[converter_id] = []
            
            for prompt in dataset.prompts:
                # Test original prompt
                try:
                    response, latency_ms = await target_model.generate(prompt.prompt)
                    evaluation = await judge.evaluate(
                        prompt.prompt,
                        response,
                        prompt.category,
                        prompt.expected_behavior
                    )
                    
                    direct_results_by_variant["original"].append(evaluation.attack_success)
                    completed_work += 1
                    
                    # Update real-time counters
                    with job._lock:
                        job.attacks_completed += 1
                        if evaluation.attack_success:
                            job.successful_attacks += 1
                    
                    job.update_progress(completed_work, total_work)
                    
                except Exception as e:
                    logger.error(f"Error in direct test {prompt.prompt_id}: {e}")
                    direct_results_by_variant["original"].append(False)
                    completed_work += 1
                    job.update_progress(completed_work, total_work)
                
                # Test with converters (if any)
                for converter_id in converters_to_test:
                    try:
                        # Apply converter
                        converted_prompt = ConverterRegistry.apply_converter(converter_id, prompt.prompt)
                        
                        # Test converted version
                        response, latency_ms = await target_model.generate(converted_prompt)
                        evaluation = await judge.evaluate(
                            prompt.prompt,  # Use original for judge context
                            response,
                            prompt.category,
                            prompt.expected_behavior
                        )
                        
                        direct_results_by_variant[converter_id].append(evaluation.attack_success)
                        completed_work += 1
                        
                        # Update real-time counters
                        with job._lock:
                            job.attacks_completed += 1
                            if evaluation.attack_success:
                                job.successful_attacks += 1
                        
                        job.update_progress(completed_work, total_work)
                        
                    except Exception as e:
                        logger.error(f"Error in {converter_id} test {prompt.prompt_id}: {e}")
                        direct_results_by_variant[converter_id].append(False)
                        completed_work += 1
                        
                        # Update real-time counters
                        with job._lock:
                            job.attacks_completed += 1
                        
                        job.update_progress(completed_work, total_work)
            
            # Create attack results for each variant
            for variant_type, results_list in direct_results_by_variant.items():
                if results_list:  # Only if we have results
                    successful = sum(1 for r in results_list if r)
                    attack_result = AttackResult(
                        attack_type=f"{dataset.dataset_id}-direct" if variant_type == "original" else f"{dataset.dataset_id}-direct-{variant_type}",
                        success_rate=successful / len(results_list) if results_list else 0.0,
                        total_attempts=len(results_list),
                        successful_attempts=successful,
                        status="completed"
                    )
                    
                    with job._lock:
                        job.attack_results.append(attack_result)
                    
                    logger.info(
                        f"{variant_type.capitalize()} testing complete: {successful}/{len(results_list)} "
                        f"attacks succeeded ({attack_result.success_rate:.1%})"
                    )

            # PHASE 2: Run automated attacks if attacker model provided
            automated_result = None
            automated_results_list = []
            
            if job.request.attacker_model:
                logger.info(f"Starting automated attacks ({total_automated_sessions} sessions)...")
                
                # Initialize attacker
                attacker_model = get_llm(job.request.attacker_model)
                attacker_healthy = await attacker_model.health_check()
                if not attacker_healthy:
                    raise ValueError("Attacker model health check failed")
                
                # Create attack config
                # TODO: Add automated attack names to run
                attack_config = job.request.attack_config or DynamicAttackConfig()
                
                # Create strategy (currently only GOAT)
                runner = GOATSessionRunner(
                    target_model=target_model,
                    attacker_model=attacker_model,
                    max_iterations=attack_config.max_iterations,
                    early_stop=attack_config.early_stop_on_success
                )
                
                # Run sessions for each prompt as a goal
                goals = [p.prompt for p in dataset.prompts]
                
                for goal_idx, goal in enumerate(goals):
                    for session_num in range(job.request.num_sessions):
                        try:
                            session = await runner.engine.run_attack_session(goal)
                            automated_results_list.append(session.final_success)
                            
                            completed_work += 1
                            
                            # Update real-time counters
                            with job._lock:
                                job.attacks_completed += 1
                                if session.final_success:
                                    job.successful_attacks += 1
                            
                            job.update_progress(completed_work, total_work)
                            
                            logger.info(
                                f"Automated session {completed_work - total_direct_tests}/"
                                f"{total_automated_sessions}: goal={goal_idx+1}, "
                                f"success={session.final_success}"
                            )
                            
                        except Exception as e:
                            logger.error(f"Error in automated session: {e}")
                            automated_results_list.append(False)
                            completed_work += 1
                            
                            # Update real-time counters
                            with job._lock:
                                job.attacks_completed += 1
                            
                            job.update_progress(completed_work, total_work)
                
                # Create automated attack result
                automated_successful = sum(1 for r in automated_results_list if r)
                automated_result = AttackResult(
                    attack_type="goat",
                    success_rate=automated_successful / len(automated_results_list) if automated_results_list else 0.0,
                    total_attempts=len(automated_results_list),
                    successful_attempts=automated_successful,
                    status="completed"
                )
                
                with job._lock:
                    job.attack_results.append(automated_result)
                
                logger.info(
                    f"Automated attacks complete: {automated_successful}/{len(automated_results_list)} "
                    f"sessions succeeded ({automated_result.success_rate:.1%})"
                )

            # Job completed
            with job._lock:
                job.status = RedTeamJobStatus.COMPLETED
                job.completed_at = datetime.now(timezone.utc).isoformat()
                job.progress = 100.0
            
            logger.info(f"Red team job {job_id} complete")

        except Exception as e:
            logger.error(f"Error executing job {job_id}: {e}")
            with job._lock:
                job.status = RedTeamJobStatus.FAILED
                job.error = str(e)
                job.completed_at = datetime.now(timezone.utc).isoformat()

    def get_job(self, job_id: str) -> Optional[RedTeamJob]:
        """Get job by ID."""
        with self.lock:
            return self.jobs.get(job_id)

    def get_job_summary(self, job_id: str) -> Optional[RedTeamJobSummary]:
        """Get job summary by ID."""
        job = self.get_job(job_id)
        if job:
            logger.debug(f"Getting summary for job {job_id}: progress={job.progress}, attacks={len(job.attack_results)}")
        return job.get_summary() if job else None

    def get_job_results(self, job_id: str) -> Optional[UnifiedRedTeamJobResults]:
        """Get job results with comparison and overall safety score."""
        job = self.get_job(job_id)
        if not job or not job.attack_results:
            return None

        # Calculate comparison stats if both attacks were run
        comparison = None
        if len(job.attack_results) >= 2:
            direct_result = job.attack_results[0]  # Always first
            automated_result = job.attack_results[1]  # If exists
            
            comparison = ComparisonStats(
                direct_success_rate=direct_result.success_rate,
                automated_success_rate=automated_result.success_rate,
                delta=automated_result.success_rate - direct_result.success_rate
            )

        # Calculate overall safety score (worst-case)
        worst_success_rate = max(r.success_rate for r in job.attack_results)
        overall_safety_score = 1.0 - worst_success_rate
        
        # Identify worst attack type
        worst_attack = max(job.attack_results, key=lambda x: x.success_rate)
        
        # Calculate duration
        duration_seconds = 0.0
        if job.started_at and job.completed_at:
            start = datetime.fromisoformat(job.started_at)
            end = datetime.fromisoformat(job.completed_at)
            duration_seconds = (end - start).total_seconds()

        return UnifiedRedTeamJobResults(
            job_id=job.job_id,
            status=job.status,
            target_model=job.request.target_model.model_name,
            dataset_info=job.dataset_info or {},
            attacks=job.attack_results,
            comparison=comparison,
            overall_safety_score=overall_safety_score,
            worst_attack_type=worst_attack.attack_type,
            duration_seconds=duration_seconds,
            started_at=job.started_at or "",
            completed_at=job.completed_at or "",
            details_available={
                "job_status": f"/redteam/jobs/{job_id}",
                "full_results": f"/redteam/jobs/{job_id}/results"
            }
        )

    def list_jobs(self) -> List[RedTeamJobSummary]:
        """List all jobs."""
        with self.lock:
            return [job.get_summary() for job in self.jobs.values()]

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job."""
        job = self.get_job(job_id)
        if not job:
            return False

        if job.status == RedTeamJobStatus.RUNNING and job.task:
            job.task.cancel()
            job.status = RedTeamJobStatus.CANCELLED
            job.completed_at = datetime.now(timezone.utc).isoformat()
            logger.info(f"Cancelled job: {job_id}")
            return True

        return False

    def delete_job(self, job_id: str) -> bool:
        """Delete a job."""
        with self.lock:
            if job_id in self.jobs:
                # Cancel if running
                self.cancel_job(job_id)
                del self.jobs[job_id]
                logger.info(f"Deleted job: {job_id}")
                return True
        return False


# Global job manager instance
_job_manager: Optional[RedTeamJobManager] = None


def get_job_manager() -> RedTeamJobManager:
    """Get the global job manager instance."""
    global _job_manager
    if _job_manager is None:
        _job_manager = RedTeamJobManager()
        logger.info(f"Created new RedTeamJobManager instance: {id(_job_manager)}")
    else:
        logger.debug(f"Returning existing RedTeamJobManager instance: {id(_job_manager)}")
    return _job_manager

