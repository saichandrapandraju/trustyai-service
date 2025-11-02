"""
Simplified Job Manager using RedTeamExecutor.

This version delegates all attack logic to the executor,
keeping job management clean and focused.
"""

import asyncio
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from src.core.redteam.attack_vectors.registry import VectorRegistry
from src.core.redteam.executor import RedTeamExecutor, VectorResults, AttackResult
from src.core.redteam.llm_interface import get_llm
from src.core.redteam.schemas import (
    RedTeamJobStatus,
    RedTeamJobSummary,
    UnifiedRedTeamJobRequest,
    UnifiedRedTeamJobResults,
    AttackResult as SchemaAttackResult
)

logger = logging.getLogger(__name__)


class RedTeamJob:
    """Simplified job representation."""
    
    def __init__(self, job_id: str, request: UnifiedRedTeamJobRequest):
        self.job_id = job_id
        self.request = request
        self.status = RedTeamJobStatus.QUEUED
        self.started_at: Optional[str] = None
        self.completed_at: Optional[str] = None
        self.progress: float = 0.0
        self.error: Optional[str] = None
        
        # Results storage
        self.vector_results: List[VectorResults] = []
        
        # Real-time tracking (vector level)
        self.current_vector: Optional[str] = None
        self.total_vectors: int = 0
        self.completed_vectors: int = 0
        
        # Real-time tracking (test level) - updated during execution
        self.tests_completed: int = 0
        self.tests_planned: int = 0
        self.successful_attacks_current: int = 0
        
        # Task control for cancellation
        self.task: Optional[asyncio.Task] = None
        
        self._lock = threading.Lock()
    
    def get_summary(self) -> RedTeamJobSummary:
        """Get job summary with real-time test tracking."""
        with self._lock:
            preliminary_stats = {}
            
            if self.status == RedTeamJobStatus.RUNNING:
                # Show real-time test counts during execution
                preliminary_stats = {
                    "current_vector": self.current_vector,
                    "completed_vectors": self.completed_vectors,
                    "total_vectors": self.total_vectors,
                    # Real-time test tracking
                    "tests_completed": self.tests_completed,
                    "tests_planned": self.tests_planned,
                    "successful_attacks": self.successful_attacks_current,
                    "current_safety_score": 1.0 - (self.successful_attacks_current / self.tests_completed) if self.tests_completed > 0 else 1.0
                }
            elif self.vector_results:
                # Final stats when complete
                total_tests = sum(vr.total_tests for vr in self.vector_results)
                successful_attacks = sum(vr.successful_attacks for vr in self.vector_results)
                preliminary_stats = {
                    "total_tests": total_tests,
                    "successful_attacks": successful_attacks,
                    "overall_safety_score": 1.0 - (successful_attacks / total_tests if total_tests > 0 else 0)
                }
            
            return RedTeamJobSummary(
                job_id=self.job_id,
                status=self.status,
                started_at=self.started_at or "",
                completed_at=self.completed_at,
                progress=self.progress,
                preliminary_stats=preliminary_stats
            )


class RedTeamJobManager:
    """
    Simplified job manager.
    
    Delegates all attack execution to RedTeamExecutor.
    Focuses on job lifecycle and result aggregation.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.jobs: Dict[str, RedTeamJob] = {}
            self.lock = threading.Lock()
            self.initialized = True
            logger.info("RedTeamJobManager initialized (v2 - with Executor)")
    
    def create_job(self, request: UnifiedRedTeamJobRequest) -> str:
        """
        Create a new red team job.
        
        Args:
            request: Job configuration
            
        Returns:
            Job ID
        """
        # Validate request
        request.validate_inputs()
        
        job_id = str(uuid.uuid4())
        job = RedTeamJob(job_id, request)
        
        with self.lock:
            self.jobs[job_id] = job
        
        logger.info(f"Created job {job_id}")
        return job_id
    
    async def execute_job(self, job_id: str) -> None:
        """
        Execute a red team job using the Executor.
        
        Handles task cancellation properly - if cancelled, stops gracefully.
        
        Args:
            job_id: Job identifier
        """
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                logger.error(f"Job {job_id} not found")
                return
        
        try:
            # Mark as running
            with job._lock:
                job.status = RedTeamJobStatus.RUNNING
                job.started_at = datetime.now(timezone.utc).isoformat()
            
            logger.info(f"Starting job {job_id}")
            
            # Initialize models
            target_model = get_llm(job.request.target_model)
            judge_model = get_llm(job.request.judge_model) if job.request.judge_model else None
            attacker_model = get_llm(job.request.attacker_model) if job.request.attacker_model else None
            
            # Create progress callback for real-time tracking
            def update_progress(is_safe: bool):
                """Update job progress counters in real-time."""
                with job._lock:
                    job.tests_completed += 1
                    if not is_safe:  # Attack succeeded
                        job.successful_attacks_current += 1
            
            # Create executor with progress callback
            executor = RedTeamExecutor(
                target_model=target_model,
                judge_model=judge_model,
                attacker_model=attacker_model,
                progress_callback=update_progress
            )
            
            # Choose testing approach based on request type
            if job.request.attack_vectors:
                # VECTOR-BASED TESTING: Test specific attack vectors
                await self._execute_vector_based(job, executor)
            elif job.request.dataset_source_config:
                # DATASET-BASED TESTING: Test specific dataset directly
                await self._execute_dataset_based(job, executor)
            else:
                raise ValueError("Must provide either attack_vectors or dataset_source_config")
            
            # Mark as complete
            with job._lock:
                job.status = RedTeamJobStatus.COMPLETED
                job.completed_at = datetime.now(timezone.utc).isoformat()
                job.progress = 100.0
            
            logger.info(f"Job {job_id} completed successfully")
        
        except asyncio.CancelledError:
            # Task was cancelled by user
            logger.info(f"Job {job_id} was cancelled")
            with job._lock:
                job.status = RedTeamJobStatus.FAILED
                job.error = "Job cancelled by user"
                job.completed_at = datetime.now(timezone.utc).isoformat()
            raise  # Re-raise to properly cancel the task
        
        except Exception as e:
            logger.error(f"Error executing job {job_id}: {e}", exc_info=True)
            with job._lock:
                job.status = RedTeamJobStatus.FAILED
                job.error = str(e)
                job.completed_at = datetime.now(timezone.utc).isoformat()
    
    async def _execute_vector_based(self, job: RedTeamJob, executor: RedTeamExecutor) -> None:
        """
        Execute vector-based testing (when attack_vectors is provided).
        
        Tests specific attack vectors with their configured datasets, converters, and dynamic attacks.
        """
        from src.core.redteam.attack_vectors.registry import VectorRegistry
        from src.core.redteam.datasets.dataset_registry import DatasetRegistry
        from src.core.redteam.datasets.dataset_sources import DatasetSourceConfig
        
        # Get vectors to test
        vectors = [
            VectorRegistry.get_vector(v_id)
            for v_id in job.request.attack_vectors
            if VectorRegistry.get_vector(v_id) is not None
        ]
        
        job.total_vectors = len(vectors)
        logger.info(f"Testing {len(vectors)} attack vectors")
        
        # Calculate total tests planned for progress tracking
        total_tests_planned = 0
        
        # Determine how many converters will be used (user's request, not vector's)
        num_converters = 0
        if job.request.converters and len(job.request.converters) > 0:
            if job.request.converters == ["all"] or job.request.converters == "all":
                from src.core.redteam.converters.registry import ConverterRegistry
                num_converters = len(ConverterRegistry.get_all_converter_ids())
            else:
                num_converters = len(job.request.converters)  # Use user's count!
        
        for vector in vectors:
            # Load datasets to count prompts
            for dataset_id in vector.datasets:
                try:
                    config = DatasetSourceConfig(source="builtin", dataset_id=dataset_id)
                    dataset = DatasetRegistry.load_dataset(config)
                    num_prompts = len(dataset.prompts)
                    
                    # Count tests: direct + converters + dynamic
                    total_tests_planned += num_prompts  # Direct
                    total_tests_planned += num_prompts * num_converters  # User's converters
                    
                    # Dynamic attacks
                    if job.request.attacker_model:
                        total_tests_planned += num_prompts * job.request.num_sessions
                        
                except Exception as e:
                    logger.warning(f"Failed to count prompts for {dataset_id}: {e}")
        
        with job._lock:
            job.tests_planned = total_tests_planned
        
        logger.info(f"Planned {total_tests_planned} total tests")
        
        # Execute each vector
        for idx, vector in enumerate(vectors):
            with job._lock:
                job.current_vector = vector.name
                job.progress = (idx / len(vectors)) * 100
            
            logger.info(f"Executing vector: {vector.vector_id}")
            
            # Prepare converter list (user's request overrides vector defaults)
            converter_ids_to_use = None
            if job.request.converters and len(job.request.converters) > 0:
                if job.request.converters == ["all"] or job.request.converters == "all":
                    from src.core.redteam.converters.registry import ConverterRegistry
                    converter_ids_to_use = ConverterRegistry.get_all_converter_ids()
                else:
                    converter_ids_to_use = job.request.converters  # Use user's list!
            
            # Execute vector
            vector_results = await executor.execute_vector(
                vector=vector,
                enable_converters=job.request.converters is not None and len(job.request.converters) > 0,
                enable_dynamic=job.request.attacker_model is not None,
                num_sessions_per_prompt=job.request.num_sessions,
                attack_config=job.request.attack_config if job.request.attack_config else None,
                converter_ids_override=converter_ids_to_use  # Pass user's converter list!
            )
            
            # Store results
            with job._lock:
                job.vector_results.append(vector_results)
                job.completed_vectors += 1
                job.progress = ((idx + 1) / len(vectors)) * 100
            
            logger.info(
                f"Completed vector {vector.vector_id}: "
                f"{vector_results.total_tests} tests, "
                f"{vector_results.successful_attacks} successful attacks "
                f"({vector_results.success_rate:.2%})"
            )
    
    async def _execute_dataset_based(self, job: RedTeamJob, executor: RedTeamExecutor) -> None:
        """
        Execute dataset-based testing (when dataset_source_config is provided).
        
        Tests the specific dataset directly without vector abstraction.
        Applies converters and dynamic attacks if configured.
        """
        from src.core.redteam.datasets.dataset_registry import DatasetRegistry
        
        # Load dataset
        dataset = DatasetRegistry.load_dataset(job.request.dataset_source_config)
        
        with job._lock:
            job.current_vector = f"Dataset: {dataset.name}"
            job.total_vectors = 1
            job.progress = 0.0
        
        logger.info(f"Testing dataset: {dataset.name} ({len(dataset.prompts)} prompts)")
        
        # Calculate total tests planned
        num_prompts = len(dataset.prompts)
        total_tests_planned = num_prompts  # Direct
        
        if job.request.converters and len(job.request.converters) > 0:
            num_converters = len(job.request.converters)
            if job.request.converters == ["all"] or job.request.converters == "all":
                from src.core.redteam.converters.registry import ConverterRegistry
                num_converters = len(ConverterRegistry.get_all_converter_ids())
            total_tests_planned += num_prompts * num_converters
        
        if job.request.attacker_model:
            total_tests_planned += num_prompts * job.request.num_sessions
        
        with job._lock:
            job.tests_planned = total_tests_planned
        
        logger.info(f"Planned {total_tests_planned} total tests")
        
        all_results: List[AttackResult] = []
        
        # 1. Direct testing
        logger.info("Running direct tests...")
        direct_results = await executor._test_direct(dataset)
        all_results.extend(direct_results)
        logger.info(f"Direct tests: {len(direct_results)} completed")
        
        # Update progress
        total_work = 1  # direct
        if job.request.converters and len(job.request.converters) > 0:
            total_work += 1  # converters
        if job.request.attacker_model:
            total_work += 1  # dynamic
        
        with job._lock:
            job.progress = (1 / total_work) * 100
        
        # 2. Converter testing (if enabled)
        if job.request.converters and len(job.request.converters) > 0:
            logger.info(f"Running converter tests with: {job.request.converters}")
            
            # Use converters from request, not from vector
            if job.request.converters == ["all"] or job.request.converters == "all":
                from src.core.redteam.converters.registry import ConverterRegistry
                converter_ids = ConverterRegistry.list_converters()
            else:
                converter_ids = job.request.converters
            
            converter_results = await executor._test_with_converters(dataset, converter_ids)
            all_results.extend(converter_results)
            logger.info(f"Converter tests: {len(converter_results)} completed")
            
            with job._lock:
                job.progress = (2 / total_work) * 100
        
        # 3. Dynamic attacks (if enabled)
        if job.request.attacker_model:
            logger.info("Running dynamic attacks...")
            
            # Use configured dynamic attacks or default to GOAT
            dynamic_attack_ids = ["goat"]  # Default
            
            dynamic_results = await executor._test_with_dynamic(
                dataset,
                dynamic_attack_ids,
                num_sessions=job.request.num_sessions
            )
            all_results.extend(dynamic_results)
            logger.info(f"Dynamic attacks: {len(dynamic_results)} completed")
        
        # Create a VectorResults-like structure for consistency
        from src.core.redteam.executor import VectorResults
        
        # Aggregate results
        successful_attacks = sum(1 for r in all_results if not r.is_safe)
        
        # Group by method
        by_method = {}
        for result in all_results:
            if result.method not in by_method:
                by_method[result.method] = {"total": 0, "success": 0}
            by_method[result.method]["total"] += 1
            if not result.is_safe:
                by_method[result.method]["success"] += 1
        
        vector_results = VectorResults(
            vector_id=dataset.dataset_id,
            vector_name=dataset.name,
            total_tests=len(all_results),
            successful_attacks=successful_attacks,
            results_by_method=by_method,
            detailed_results=all_results
        )
        
        # Store results
        with job._lock:
            job.vector_results.append(vector_results)
            job.completed_vectors = 1
            job.progress = 100.0
        
        logger.info(
            f"Completed dataset testing: "
            f"{vector_results.total_tests} tests, "
            f"{vector_results.successful_attacks} successful attacks "
            f"({vector_results.success_rate:.2%})"
        )
    
    def get_job_summary(self, job_id: str) -> Optional[RedTeamJobSummary]:
        """Get job summary."""
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return None
            return job.get_summary()
    
    def get_job_results(self, job_id: str) -> Optional[UnifiedRedTeamJobResults]:
        """
        Get complete job results.
        
        Converts internal VectorResults to UnifiedRedTeamJobResults with proper aggregation.
        
        Args:
            job_id: Job identifier
            
        Returns:
            UnifiedRedTeamJobResults or None
        """
        from collections import defaultdict
        from datetime import datetime
        
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return None
            
            if job.status != RedTeamJobStatus.COMPLETED:
                return None
            
            # STEP 1: Aggregate results by (attack_type, variant)
            # Group all individual test results into aggregated attack results
            aggregated = defaultdict(lambda: {
                "total": 0,
                "successes": 0,
                "attack_type": None,
                "variant": None
            })
            
            for vector_result in job.vector_results:
                for ar in vector_result.detailed_results:
                    # Key: (attack_type, variant) - e.g., ("jailbreak", "direct")
                    key = (vector_result.vector_id, ar.method)
                    
                    if aggregated[key]["attack_type"] is None:
                        aggregated[key]["attack_type"] = vector_result.vector_id
                        aggregated[key]["variant"] = ar.method
                    
                    aggregated[key]["total"] += 1
                    if not ar.is_safe:  # Attack succeeded
                        aggregated[key]["successes"] += 1
            
            # Convert to AttackResult objects
            attacks = []
            for key, data in aggregated.items():
                attacks.append(SchemaAttackResult(
                    attack_type=f"{data['attack_type']}-{data['variant']}",  # e.g., "jailbreak-direct"
                    total_attempts=data["total"],
                    successful_attempts=data["successes"],
                    success_rate=data["successes"] / data["total"] if data["total"] > 0 else 0.0,
                    status="completed"
                ))
            
            # STEP 2: Calculate overall stats
            total_tests = sum(vr.total_tests for vr in job.vector_results)
            successful_attacks = sum(vr.successful_attacks for vr in job.vector_results)
            overall_safety_score = 1.0 - (successful_attacks / total_tests if total_tests > 0 else 0)
            
            # STEP 3: Find worst attack type
            worst_attack = min(attacks, key=lambda a: 1.0 - a.success_rate) if attacks else None
            worst_attack_type = worst_attack.attack_type if worst_attack else "none"
            
            # STEP 4: Calculate duration
            if job.started_at and job.completed_at:
                start = datetime.fromisoformat(job.started_at)
                end = datetime.fromisoformat(job.completed_at)
                duration = (end - start).total_seconds()
            else:
                duration = 0.0
            
            # STEP 5: Build dataset info from vector results
            dataset_info = {
                "vectors_tested": [vr.vector_id for vr in job.vector_results],
                "total_tests": sum(vr.total_tests for vr in job.vector_results),
                "vector_names": [vr.vector_name for vr in job.vector_results]
            }
            
            # STEP 6: Return properly formatted UnifiedRedTeamJobResults
            return UnifiedRedTeamJobResults(
                job_id=job_id,
                status=job.status,
                target_model=job.request.target_model.model_name,
                dataset_info=dataset_info,
                attacks=attacks,
                comparison=None,  # Removed - not needed
                overall_safety_score=overall_safety_score,
                worst_attack_type=worst_attack_type,
                duration_seconds=duration,
                started_at=job.started_at or "",
                completed_at=job.completed_at or ""
            )
    
    def get_job(self, job_id: str) -> Optional[RedTeamJob]:
        """
        Get a job by ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            RedTeamJob or None if not found
        """
        with self.lock:
            return self.jobs.get(job_id)
    
    def list_jobs(self) -> List[RedTeamJobSummary]:
        """List all jobs."""
        with self.lock:
            return [job.get_summary() for job in self.jobs.values()]
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job and cancel its task if running.
        
        If the job is running, this will:
        1. Cancel the asyncio task (stops LLM calls immediately)
        2. Mark job as cancelled
        3. Remove from jobs dict
        
        This prevents wasting resources on cancelled jobs.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if job was deleted, False if not found
        """
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return False
            
            # If running, cancel the task
            if job.status == RedTeamJobStatus.RUNNING and job.task:
                logger.info(f"Cancelling running job {job_id}")
                job.task.cancel()
                
                # Mark as cancelled
                with job._lock:
                    job.status = RedTeamJobStatus.FAILED
                    job.error = "Job cancelled by user"
                    job.completed_at = datetime.now(timezone.utc).isoformat()
                
                logger.info(f"Task cancelled for job {job_id}")
            
            # Remove from jobs dict
            del self.jobs[job_id]
            logger.info(f"Deleted job {job_id}")
            return True


# Singleton accessor
def get_job_manager() -> RedTeamJobManager:
    """Get the singleton job manager instance."""
    return RedTeamJobManager()

