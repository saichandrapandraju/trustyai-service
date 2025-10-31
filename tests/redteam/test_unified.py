"""Tests for unified red team API."""

import pytest
from src.core.redteam.datasets.dataset_sources import DatasetSource, DatasetSourceConfig
from src.core.redteam.llm_interface import LLMConfig
from src.core.redteam.schemas import (
    AttackResult,
    ComparisonStats,
    DynamicAttackConfig,
    UnifiedRedTeamJobRequest,
)


def test_unified_job_request_direct_only():
    """Test unified request with only direct testing (no attacker)."""
    request = UnifiedRedTeamJobRequest(
        target_model=LLMConfig(model_name="gpt-3.5-turbo"),
        dataset_source_config=DatasetSourceConfig(
            source=DatasetSource.BUILTIN,
            dataset_id="sample-jailbreaks-v1"
        ),
        judge_model=LLMConfig(model_name="gpt-4")
    )
    
    assert request.target_model.model_name == "gpt-3.5-turbo"
    assert request.attacker_model is None  # No automated attacks
    assert request.dataset_source_config.source == DatasetSource.BUILTIN


def test_unified_job_request_with_automated():
    """Test unified request with both direct and automated attacks."""
    request = UnifiedRedTeamJobRequest(
        target_model=LLMConfig(model_name="gpt-3.5-turbo"),
        attacker_model=LLMConfig(model_name="gpt-4", temperature=1.0),
        dataset_source_config=DatasetSourceConfig(
            source=DatasetSource.JAILBREAKBENCH,
            split="harmful",
            limit=10
        ),
        attack_config=DynamicAttackConfig(
            max_iterations=5,
            early_stop_on_success=True
        ),
        num_sessions=2
    )
    
    assert request.attacker_model is not None  # Automated attacks enabled
    assert request.num_sessions == 2
    assert request.attack_config.max_iterations == 5


def test_attack_result_creation():
    """Test AttackResult model."""
    result = AttackResult(
        attack_type="jailbreakbench-direct",
        success_rate=0.15,
        total_attempts=20,
        successful_attempts=3,
        status="completed"
    )
    
    assert result.attack_type == "jailbreakbench-direct"
    assert result.success_rate == 0.15
    assert result.total_attempts == 20
    assert result.successful_attempts == 3


def test_comparison_stats():
    """Test ComparisonStats model."""
    stats = ComparisonStats(
        direct_success_rate=0.15,
        automated_success_rate=0.25,
        delta=0.10,
        prompts_failed_both=12,
        prompts_only_automated=5
    )
    
    assert stats.direct_success_rate == 0.15
    assert stats.automated_success_rate == 0.25
    assert stats.delta == 0.10


def test_unified_job_request_defaults():
    """Test unified request with defaults."""
    request = UnifiedRedTeamJobRequest(
        target_model=LLMConfig(model_name="gpt-3.5-turbo"),
        dataset_source_config=DatasetSourceConfig(
            source=DatasetSource.BUILTIN,
            dataset_id="sample-jailbreaks-v1"
        )
    )
    
    # Defaults
    assert request.num_sessions == 2
    assert request.batch_size == 10
    assert request.judge_model is None  # Auto-selected
    assert request.attacker_model is None  # Direct testing only

