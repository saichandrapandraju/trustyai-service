from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field

# Import unified LLM configuration
from src.core.redteam.llm_interface import LLMConfig
from src.core.redteam.datasets.dataset_sources import DatasetSourceConfig


class RedTeamPrompt(BaseModel):
    """Single prompt in a red team dataset."""

    prompt_id: str = Field(..., description="Unique identifier for the prompt")
    prompt: str = Field(..., description="The actual prompt text to test")
    category: Optional[str] = Field(None, description="Category of attack (e.g., jailbreak, prompt_injection, exfiltration)")
    expected_behavior: Optional[str] = Field(None, description="What a safe response should look like")
    tags: Optional[List[str]] = Field(None, description="Additional categorization tags")


class StaticRedTeamDataset(BaseModel):
    """Collection of red team prompts."""

    dataset_id: str = Field(..., description="Unique identifier for the dataset")
    name: str = Field(..., description="Human-readable name")
    description: str = Field(..., description="Description of the dataset")
    version: str = Field(..., description="Dataset version")
    prompts: List[RedTeamPrompt] = Field(..., description="List of red team prompts")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class JudgeEvaluation(BaseModel):
    """Result of judge evaluation for a single prompt-response pair."""

    attack_success: bool = Field(..., description="Whether the attack was successful")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Judge confidence score (0-1)")
    reasoning: Optional[str] = Field(None, description="Judge's detailed reasoning")


class RedTeamAttackResult(BaseModel):
    """Result of a single prompt attack."""

    prompt_id: str
    prompt: str
    response: str
    attack_success: bool
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: Optional[str]
    category: Optional[str]
    timestamp: str
    latency_ms: Optional[float]


# Legacy aliases for backward compatibility
TargetModelConfig = LLMConfig
JudgeModelConfig = LLMConfig


class RedTeamJobStatus(str, Enum):
    """Status of a red team job."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StaticRedTeamJobRequest(BaseModel):
    """Request to start a static red team evaluation."""

    target_model: LLMConfig
    
    # Dataset specification (choose one method)
    dataset_id: Optional[str] = Field(None, description="ID of built-in dataset (legacy)")
    dataset_source: Optional[str] = Field(None, description="File path to custom dataset (legacy)")
    dataset_source_config: Optional[DatasetSourceConfig] = Field(
        None,
        description="Structured dataset source configuration (recommended)"
    )
    custom_dataset: Optional[StaticRedTeamDataset] = Field(None, description="Inline dataset")
    
    judge_model: LLMConfig
    evaluation_criteria: Optional[Dict[str, str]] = Field(
        None,
        description="Custom evaluation criteria for the judge (e.g., {'bias': 'Check for bias', 'privacy': 'Check for PII'})"
    )
    batch_size: int = Field(10, ge=1, le=100, description="Number of prompts to process concurrently")
    max_concurrent: int = Field(5, ge=1, le=20, description="Maximum concurrent evaluations")


class CategoryStats(BaseModel):
    """Statistics for a specific category."""

    category: Optional[str]
    total_prompts: int
    successful_attacks: int
    attack_success_rate: float
    avg_confidence: float


class RedTeamJobSummary(BaseModel):
    """Summary of a red team job."""

    job_id: str
    status: RedTeamJobStatus
    started_at: str
    completed_at: Optional[str] = None
    progress: float = Field(ge=0.0, le=100.0, description="Progress percentage")
    preliminary_stats: Dict[str, Any] = Field(default_factory=dict)


class RedTeamJobResults(BaseModel):
    """Complete results of a red team evaluation job."""

    job_id: str
    target_model: str
    status: RedTeamJobStatus

    # Overall metrics
    total_prompts: int
    successful_attacks: int
    attack_success_rate: float
    avg_confidence: float
    avg_latency_ms: Optional[float]

    # Breakdown by category
    category_breakdown: Dict[str, CategoryStats]

    # Individual results
    results: List[RedTeamAttackResult]

    # Metadata
    duration_seconds: float
    started_at: str
    completed_at: str


# AttackerModelConfig is now just an alias to LLMConfig
AttackerModelConfig = LLMConfig


class DynamicAttackConfig(BaseModel):
    """Configuration for dynamic red teaming attack."""

    max_iterations: int = Field(5, ge=1, le=20, description="Maximum conversation turns per session")
    early_stop_on_success: bool = Field(True, description="Stop session when attack succeeds")


class ConversationTurn(BaseModel):
    """Single turn in a dynamic attack conversation."""

    turn_number: int
    attacker_prompt: str
    target_response: str
    attacker_reasoning: Optional[str] = Field(None, description="Why attacker chose this prompt")
    attack_success: bool
    judge_evaluation: JudgeEvaluation


class DynamicAttackSession(BaseModel):
    """Complete dynamic attack session."""

    session_id: str
    config: Dict[str, Any] = Field(..., description="Session configuration including goal")
    turns: List[ConversationTurn]
    final_success: bool
    success_turns: Optional[List[int]] = Field(None, description="Turn numbers where attack succeeded")
    total_turns: int
    duration_seconds: float
    started_at: str
    completed_at: str


class DynamicRedTeamJobRequest(BaseModel):
    """Request to start a dynamic red team evaluation (GOAT)."""

    target_model: LLMConfig
    attacker_model: LLMConfig
    
    # Input method (choose ONE)
    dataset_source_config: Optional[DatasetSourceConfig] = Field(
        None,
        description="Load goals from dataset (each prompt becomes a goal)"
    )
    attack_goals: Optional[List[str]] = Field(
        None,
        description="Manually specified attack goals (uses defaults if neither dataset nor goals provided)"
    )
    
    attack_config: DynamicAttackConfig
    num_sessions: int = Field(1, ge=1, le=10, description="Number of trials per goal/prompt")
    
    # Judge config (optional - auto-selected based on dataset)
    judge_model: Optional[LLMConfig] = Field(
        None,
        description="Judge model config (auto-selected for JBB if not provided)"
    )
    
    def validate_inputs(self) -> None:
        """Validate that only one input method is used."""
        has_dataset = self.dataset_source_config is not None
        has_goals = self.attack_goals is not None and len(self.attack_goals) > 0
        
        if has_dataset and has_goals:
            raise ValueError(
                "Cannot specify both dataset_source_config and attack_goals. "
                "Choose one input method."
            )


class SessionSummary(BaseModel):
    """Lightweight summary of a dynamic attack session."""
    
    session_id: str
    goal: str
    final_success: bool
    total_turns: int
    success_turns: Optional[List[int]] = None
    duration_seconds: float


class DynamicRedTeamJobResults(BaseModel):
    """Complete results of a dynamic red team evaluation job."""

    job_id: str
    target_model: str
    attacker_model: str
    status: RedTeamJobStatus

    # Overall metrics
    total_goals: int
    total_sessions: int
    successful_sessions: int
    session_success_rate: float
    avg_turns_per_session: float
    avg_turns_to_success: Optional[float] = Field(None, description="Average turns to first success")

    # Per-goal breakdown
    success_rate_by_goal: Dict[str, float]

    # Lightweight session list (full details available via GET /sessions/{session_id})
    session_summaries: List[SessionSummary] = Field(
        description="Summary of each session - use GET /jobs/{job_id}/sessions/{session_id} for full conversation"
    )

    # Metadata
    duration_seconds: float
    started_at: str
    completed_at: str


# ============================================================================
# UNIFIED RED TEAMING (Phase C)
# ============================================================================

class AttackResult(BaseModel):
    """Result of a single attack approach (direct testing or automated attack)."""
    
    attack_type: str = Field(description="Type of attack (e.g., 'jailbreakbench-direct', 'goat')")
    success_rate: float = Field(ge=0.0, le=1.0, description="Attack success rate (0-1)")
    total_attempts: int = Field(description="Total number of attempts (prompts or sessions)")
    successful_attempts: int = Field(description="Number of successful attacks")
    status: str = Field(default="completed", description="Status: completed, failed, partial")
    error: Optional[str] = Field(None, description="Error message if failed")


class ComparisonStats(BaseModel):
    """Comparison statistics between different attack approaches."""
    
    direct_success_rate: float
    automated_success_rate: Optional[float] = None
    delta: Optional[float] = Field(None, description="Difference (automated - direct)")
    
    # Per-prompt comparison (if both direct and automated were run)
    prompts_failed_both: int = 0
    prompts_only_direct: int = 0
    prompts_only_automated: int = 0
    prompts_both_succeeded: int = 0


class UnifiedRedTeamJobRequest(BaseModel):
    """Unified request for comprehensive red team evaluation."""
    
    # Required
    target_model: LLMConfig
    
    # Input method (choose ONE)
    attack_vectors: Optional[List[str]] = Field(
        None,
        description="Attack vectors to test (e.g., ['prompt_injection', 'jailbreak'])"
    )
    dataset_source_config: Optional[DatasetSourceConfig] = Field(
        None,
        description="Manual dataset specification (alternative to attack_vectors)"
    )
    
    # Optional - enables automated attacks
    attacker_model: Optional[LLMConfig] = Field(
        None,
        description="If provided, runs automated attacks in addition to direct testing"
    )
    
    # Attack configuration (for automated attacks)
    attack_config: Optional[DynamicAttackConfig] = Field(
        None,
        description="Configuration for automated attacks (uses defaults if not provided)"
    )
    num_sessions: int = Field(1, ge=1, le=10, description="Trials per prompt for automated attacks")
    
    # Converters (opt-in)
    converters: Optional[Union[List[str], Literal["all"]]] = Field(
        default=[],
        description="Encodings to test (e.g., ['base64', 'rot13'], 'all', or [] for none)"
    )
    
    # Judge (auto-selected based on dataset/vector if not provided)
    judge_model: Optional[LLMConfig] = Field(None, description="Judge model (auto-selected if not provided)")
    evaluation_criteria: Optional[Dict[str, str]] = Field(None, description="Custom evaluation criteria")
    
    # Processing options
    batch_size: int = Field(10, ge=1, le=100, description="Batch size for direct testing")
    
    def validate_inputs(self) -> None:
        """Validate that exactly one input method is specified."""
        has_vectors = self.attack_vectors is not None and len(self.attack_vectors) > 0
        has_dataset = self.dataset_source_config is not None
        
        if not has_vectors and not has_dataset:
            raise ValueError(
                "Must specify either attack_vectors or dataset_source_config"
            )
        
        if has_vectors and has_dataset:
            raise ValueError(
                "Cannot specify both attack_vectors and dataset_source_config. Choose one."
            )


class UnifiedRedTeamJobResults(BaseModel):
    """Unified results for red team evaluation."""
    
    job_id: str
    status: RedTeamJobStatus
    target_model: str
    
    # Dataset information
    dataset_info: Dict[str, Any] = Field(description="Dataset metadata (source, size, etc.)")
    
    # Attack results (one entry per attack type)
    attacks: List[AttackResult] = Field(
        description="Results for each attack approach (direct, goat, etc.)"
    )
    
    # Comparison (if multiple attacks were run)
    comparison: Optional[ComparisonStats] = None
    
    # Overall safety assessment
    overall_safety_score: float = Field(
        ge=0.0,
        le=1.0,
        description="Overall safety score (0=completely unsafe, 1=completely safe)"
    )
    worst_attack_type: str = Field(description="Most successful attack approach")
    
    # Metadata
    duration_seconds: float
    started_at: str
    completed_at: str
    
    # Access to detailed results (don't include full data, just how to get it)
    details_available: Dict[str, str] = Field(
        description="Endpoints to get detailed results for each attack type"
    )
