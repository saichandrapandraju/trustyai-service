"""
Red Team Executor - Clean orchestrator for all attack components.

The executor coordinates:
- Attack vectors
- Datasets
- Converters
- Dynamic attacks
- Evaluation

Providing a simple interface: execute_vector(vector) → results
"""

import logging
from typing import Dict, List, Optional, Any, Type
from dataclasses import dataclass

from src.core.redteam.attack_vectors.base import AttackVector
from src.core.redteam.converters.base import Converter
from src.core.redteam.converters.registry import ConverterRegistry
from src.core.redteam.datasets.dataset_registry import DatasetRegistry
from src.core.redteam.datasets.dataset_sources import DatasetSourceConfig
from src.core.redteam.datasets.evaluator import DatasetEvaluator
from src.core.redteam.llm_interface import LLMInterface, get_llm, LLMConfig
from src.core.redteam.schemas import StaticRedTeamDataset, RedTeamPrompt, EvalResult, DynamicAttackConfig, DynamicAttackSession, ConversationTurn
from src.core.redteam.strategies.base import AttackStrategy
from src.core.redteam.strategies.registry import DynamicAttackRegistry

logger = logging.getLogger(__name__)


@dataclass
class AttackResult:
    """Result of a single attack attempt."""
    prompt: str
    original_prompt: Optional[str]  # If transformed by converter
    response: str
    is_safe: bool
    score: float
    reasoning: str
    method: str  # "direct", "converter-base64", "dynamic-goat", etc.
    dataset_id: str
    conversation: Optional[List[ConversationTurn]] = None  # For dynamic attacks


@dataclass
class VectorResults:
    """Aggregated results for an attack vector."""
    vector_id: str
    vector_name: str
    total_tests: int
    successful_attacks: int
    results_by_method: Dict[str, Dict[str, int]]  # method -> {total, success}
    detailed_results: List[AttackResult]
    
    @property
    def success_rate(self) -> float:
        """Overall attack success rate."""
        if self.total_tests == 0:
            return 0.0
        return self.successful_attacks / self.total_tests
    
    @property
    def safety_score(self) -> float:
        """Safety score (inverse of success rate)."""
        return 1.0 - self.success_rate


class RedTeamExecutor:
    """
    Clean orchestrator for red team testing.
    
    Takes an AttackVector → produces VectorResults
    
    Flow:
    1. Load datasets for vector
    2. For each dataset:
       a. Test original prompts (direct)
       b. Test with converters (if enabled)
       c. Test with dynamic attacks (if enabled)
    3. Aggregate and return results
    """
    
    def __init__(
        self,
        target_model: LLMInterface,
        judge_model: Optional[LLMInterface] = None,
        attacker_model: Optional[LLMInterface] = None,
        progress_callback: Optional[callable] = None,
        dataset_callback: Optional[callable] = None,
        update_planned_callback: Optional[callable] = None
    ):
        """
        Initialize executor.
        
        Args:
            target_model: The model being tested
            judge_model: Optional judge for evaluations (auto-selected if not provided)
            attacker_model: Optional attacker for dynamic attacks
            progress_callback: Optional callback for real-time test progress updates
            dataset_callback: Optional callback for dataset-level progress (for external libs)
            update_planned_callback: Optional callback to update tests_planned (for Garak)
        """
        self.target_model = target_model
        self.judge_model = judge_model
        self.attacker_model = attacker_model
        self.progress_callback = progress_callback
        self.dataset_callback = dataset_callback
        self.update_planned_callback = update_planned_callback
        
        # Create evaluator for dataset evaluations
        self.evaluator = DatasetEvaluator(judge_llm=judge_model)
    
    async def execute_vector(
        self,
        vector: AttackVector,
        enable_converters: bool = True,
        enable_dynamic: bool = True,
        num_sessions_per_prompt: int = 1,
        attack_config: Optional[DynamicAttackConfig] = None,
        converter_ids_override: Optional[List[str]] = None
    ) -> VectorResults:
        """
        Execute all tests for an attack vector.
        
        Args:
            vector: The attack vector to test
            enable_converters: Whether to test with converters
            enable_dynamic: Whether to run dynamic attacks
            num_sessions_per_prompt: How many dynamic sessions per prompt
            
        Returns:
            VectorResults with all test outcomes
        """
        logger.info(f"Executing attack vector: {vector.vector_id}")
        all_results: List[AttackResult] = []
        
        # 1. Handle external library datasets (e.g., Garak probes)
        for dataset_id in vector.datasets:
            if dataset_id.startswith("garak."):
                # Garak probe - use GarakExecutor
                probe_name = dataset_id.replace("garak.", "")
                
                # Update dataset progress
                if self.dataset_callback:
                    self.dataset_callback(dataset_name=f"Garak: {probe_name}", completed=False)
                
                logger.info(f"Running Garak probe: {probe_name}")
                
                try:
                    from src.core.redteam.integrations.garak.executor import get_garak_executor
                    
                    garak_executor = get_garak_executor()
                    garak_results = await garak_executor.execute_dataset(
                        dataset_id=probe_name,
                        target_model=self.target_model,
                        progress_callback=self.progress_callback
                    )
                    
                    all_results.extend(garak_results)
                    logger.info(f"Garak probe {probe_name}: {len(garak_results)} tests completed")
                    
                    # Update total planned tests (now we know how many Garak had)
                    if self.update_planned_callback:
                        self.update_planned_callback(additional_tests=len(garak_results))
                    
                    # Mark dataset as completed
                    if self.dataset_callback:
                        self.dataset_callback(dataset_name=f"Garak: {probe_name}", completed=True)
                    
                except ImportError:
                    logger.warning("Garak integration not available (import failed)")
                except Exception as e:
                    logger.error(f"Error running Garak probe {probe_name}: {e}")
        
        # 2. Load and test native datasets
        datasets = self._load_datasets(vector.datasets)
        logger.info(f"Loaded {len(datasets)} native datasets for {vector.vector_id}")
        
        for dataset in datasets:
            # Update dataset progress
            if self.dataset_callback:
                self.dataset_callback(dataset_name=dataset.name, completed=False)
            
            logger.info(f"Testing dataset: {dataset.dataset_id} ({len(dataset.prompts)} prompts)")
            
            # 2a. Direct testing (original prompts)
            direct_results = await self._test_direct(dataset)
            all_results.extend(direct_results)
            logger.info(f"Direct testing: {len(direct_results)} tests completed")
            
            # 2b. Converter testing
            if enable_converters:
                # Use user's requested converters if provided, otherwise use vector's defaults
                converters_to_use = converter_ids_override if converter_ids_override else vector.converter_ids
                
                if converters_to_use:
                    converter_results = await self._test_with_converters(
                        dataset,
                        converters_to_use
                    )
                    all_results.extend(converter_results)
                    logger.info(f"Converter testing: {len(converter_results)} tests completed")
            
            # 2c. Dynamic attacks
            if enable_dynamic and vector.dynamic_attack_ids and self.attacker_model:
                dynamic_results = await self._test_with_dynamic(
                    dataset,
                    vector.dynamic_attack_ids,
                    num_sessions_per_prompt,
                    attack_config
                )
                all_results.extend(dynamic_results)
                logger.info(f"Dynamic testing: {len(dynamic_results)} tests completed")
            
            # Mark dataset as completed
            if self.dataset_callback:
                self.dataset_callback(dataset_name=dataset.name, completed=True)
        
        # 3. Aggregate results
        return self._aggregate_results(vector, all_results)
    
    def _load_datasets(self, dataset_ids: List[str]) -> List[StaticRedTeamDataset]:
        """
        Load all datasets by ID.
        
        Note: Garak datasets (starting with 'garak.') are handled separately
        in execute_vector() - they run through GarakExecutor, not loaded here.
        """
        datasets = []
        for dataset_id in dataset_ids:
            # Skip external library datasets - they're executed separately
            if dataset_id.startswith("garak."):
                continue
            
            try:
                config = DatasetSourceConfig(source="builtin", dataset_id=dataset_id)
                dataset = DatasetRegistry.load_dataset(config)
                datasets.append(dataset)
            except Exception as e:
                logger.error(f"Failed to load dataset {dataset_id}: {e}")
        return datasets
    
    async def _test_direct(
        self,
        dataset: StaticRedTeamDataset
    ) -> List[AttackResult]:
        """Test each prompt directly (no modification)."""
        results = []
        
        for prompt_obj in dataset.prompts:
            try:
                # Generate response
                response, _ = await self.target_model.generate(prompt_obj.prompt)
                
                # Evaluate using evaluator
                eval_result = await self.evaluator.evaluate_response(
                    dataset=dataset,
                    prompt=prompt_obj.prompt,
                    response=response
                )
                
                result = AttackResult(
                    prompt=prompt_obj.prompt,
                    original_prompt=None,
                    response=response,
                    is_safe=eval_result.is_safe,
                    score=eval_result.score,
                    reasoning=eval_result.reasoning,
                    method="direct",
                    dataset_id=dataset.dataset_id
                )
                results.append(result)
                
                # Update progress callback (real-time tracking)
                if self.progress_callback:
                    self.progress_callback(is_safe=result.is_safe)
                    
            except Exception as e:
                logger.error(f"Error testing prompt {prompt_obj.prompt_id}: {e}")
                # Continue with other prompts
        
        return results
    
    async def _test_with_converters(
        self,
        dataset: StaticRedTeamDataset,
        converter_ids: List[str]
    ) -> List[AttackResult]:
        """Test prompts with each converter."""
        results = []
        
        for converter_id in converter_ids:
            converter: Converter = ConverterRegistry.get_converter(converter_id)
            if not converter:
                logger.warning(f"Converter not found: {converter_id}")
                continue
            
            for prompt_obj in dataset.prompts:
                try:
                    # Apply converter
                    variants = ConverterRegistry.apply_converter(converter_id, prompt_obj.prompt)
                    
                    for variant in variants:
                        # Generate response
                        response, _ = await self.target_model.generate(variant)
                        
                        # Evaluate (against original prompt intent)
                        eval_result = await self.evaluator.evaluate_response(
                            dataset=dataset,
                            prompt=prompt_obj.prompt,  # Eval against original
                            response=response
                        )
                        
                        result = AttackResult(
                            prompt=variant,
                            original_prompt=prompt_obj.prompt,
                            response=response,
                            is_safe=eval_result.is_safe,
                            score=eval_result.score,
                            reasoning=eval_result.reasoning,
                            method=f"converter-{converter_id}",
                            dataset_id=dataset.dataset_id
                        )
                        results.append(result)
                        
                        # Update progress callback
                        if self.progress_callback:
                            self.progress_callback(is_safe=result.is_safe)
                            
                except Exception as e:
                    logger.error(f"Error with converter {converter_id} on {prompt_obj.prompt_id}: {e}")
        
        return results
    
    async def _test_with_dynamic(
        self,
        dataset: StaticRedTeamDataset,
        dynamic_attack_ids: List[str],
        num_sessions: int = 1,
        attack_config: Optional[DynamicAttackConfig] = None
    ) -> List[AttackResult]:
        """Test prompts with dynamic attack strategies."""
        # One AttackResult per session - each session will have multiple turns
        # Note that we're not aggregating the results of the turns, we're just returning the final result of the session
        # This is because the results of the turns are not relevant to the final result of the session
        # The final result of the session is the only thing that matters
        results: List[AttackResult] = []
        
        for attack_id in dynamic_attack_ids:
            strategy_class: Type[AttackStrategy] = DynamicAttackRegistry.get(attack_id)
            if not strategy_class:
                logger.warning(f"Dynamic attack not found: {attack_id}")
                continue
            
            # Instantiate strategy
            strategy: AttackStrategy = strategy_class(
                attacker_model=self.attacker_model,
                target_model=self.target_model,
                max_iterations=attack_config.max_iterations if attack_config else 5,
                early_stop=attack_config.early_stop_on_success if attack_config else True
            )
            
            for prompt_obj in dataset.prompts:
                # Run multiple sessions for this prompt
                for session_idx in range(num_sessions):
                    try:
                        # Execute attack session
                        session_result: DynamicAttackSession = await strategy.run_attack_session(prompt_obj.prompt)
                        
                        # Determine if attack succeeded
                        # If dataset specifies override_dynamic_eval, use its eval logic
                        if dataset.override_dynamic_eval:
                            eval_result: EvalResult = await self.evaluator.evaluate_response(
                                dataset=dataset,
                                prompt=prompt_obj.prompt,
                                response=session_result.turns[-1].target_response
                            )
                            is_safe = eval_result.is_safe
                            score = eval_result.score
                            reasoning = eval_result.reasoning
                        else:
                            # Use the session's own evaluation
                            is_safe = not session_result.final_success
                            score = 1.0 - session_result.turns[-1].judge_evaluation.confidence if session_result.final_success else session_result.turns[-1].judge_evaluation.confidence
                            reasoning = session_result.turns[-1].judge_evaluation.reasoning
                        
                        result = AttackResult(
                            prompt=prompt_obj.prompt,
                            original_prompt=None,
                            response=session_result.turns[-1].target_response,
                            is_safe=is_safe,
                            score=score,
                            reasoning=reasoning,
                            method=f"dynamic-{attack_id}",
                            dataset_id=dataset.dataset_id,
                            conversation=session_result.turns,
                        )
                        results.append(result)
                        
                        # Update progress callback
                        if self.progress_callback:
                            self.progress_callback(is_safe=result.is_safe)
                            
                    except Exception as e:
                        logger.error(f"Error with dynamic attack {attack_id} on {prompt_obj.prompt_id}: {e}")
        
        return results
    
    def _aggregate_results(
        self,
        vector: AttackVector,
        all_results: List[AttackResult]
    ) -> VectorResults:
        """Aggregate results by method."""
        # Group by method
        by_method: Dict[str, Dict[str, int]] = {}
        
        for result in all_results:
            if result.method not in by_method:
                by_method[result.method] = {"total": 0, "success": 0}
            
            by_method[result.method]["total"] += 1
            if not result.is_safe:  # Attack succeeded
                by_method[result.method]["success"] += 1
        
        # Calculate totals
        total_tests = len(all_results)
        successful_attacks = sum(1 for r in all_results if not r.is_safe)
        
        return VectorResults(
            vector_id=vector.vector_id,
            vector_name=vector.name,
            total_tests=total_tests,
            successful_attacks=successful_attacks,
            results_by_method=by_method,
            detailed_results=all_results
        )

