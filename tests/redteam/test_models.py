"""Tests for red teaming models."""

import pytest
from src.core.redteam.llm_interface import LLMConfig
from src.core.redteam.schemas import (
    CategoryStats,
    JudgeEvaluation,
    RedTeamAttackResult,
    RedTeamJobStatus,
    RedTeamPrompt,
    StaticRedTeamDataset,
    StaticRedTeamJobRequest,
)


def test_redteam_prompt_creation():
    """Test RedTeamPrompt model creation."""
    prompt = RedTeamPrompt(
        prompt_id="test_001",
        prompt="Test prompt",
        category="jailbreak",
        expected_behavior="Refuse the request",
        tags=["test"]
    )

    assert prompt.prompt_id == "test_001"
    assert prompt.category == "jailbreak"
    assert len(prompt.tags) == 1


def test_static_dataset_creation():
    """Test StaticRedTeamDataset model creation."""
    prompts = [
        RedTeamPrompt(
            prompt_id="p1",
            prompt="Prompt 1",
            category="jailbreak",
            expected_behavior="Refuse",
            tags=[]
        ),
        RedTeamPrompt(
            prompt_id="p2",
            prompt="Prompt 2",
            category="injection",
            expected_behavior="Refuse",
            tags=[]
        )
    ]

    dataset = StaticRedTeamDataset(
        dataset_id="test_dataset",
        name="Test Dataset",
        description="A test dataset",
        version="1.0",
        prompts=prompts,
        metadata={"source": "test"}
    )

    assert dataset.dataset_id == "test_dataset"
    assert len(dataset.prompts) == 2
    assert dataset.metadata["source"] == "test"


def test_llm_config_for_target():
    """Test LLMConfig creation for target model."""
    config = LLMConfig(
        model_type="openai",
        model_name="gpt-4",
        api_key="test_key",
        temperature=0.7,
        max_tokens=1000
    )

    assert config.model_type == "openai"
    assert config.model_name == "gpt-4"
    assert config.temperature == 0.7


def test_llm_config_for_judge():
    """Test LLMConfig creation for judge model."""
    config = LLMConfig(
        model_type="openai",
        model_name="gpt-4",
        api_key="test_key",
        temperature=0.0
    )

    assert config.model_type == "openai"
    assert config.temperature == 0.0


def test_judge_evaluation():
    """Test JudgeEvaluation model creation."""
    evaluation = JudgeEvaluation(
        attack_success=True,
        confidence=0.95,
        reasoning="The model provided harmful information"
    )

    assert evaluation.attack_success is True
    assert evaluation.confidence == 0.95
    assert evaluation.reasoning == "The model provided harmful information"


def test_attack_result():
    """Test RedTeamAttackResult model creation."""
    result = RedTeamAttackResult(
        prompt_id="test_001",
        prompt="Test prompt",
        response="Test response",
        attack_success=False,
        confidence=0.9,
        reasoning="Safe response",
        category="jailbreak",
        timestamp="2025-01-01T00:00:00",
        latency_ms=150.5
    )

    assert result.prompt_id == "test_001"
    assert result.attack_success is False
    assert result.latency_ms == 150.5
    assert result.category == "jailbreak"


def test_job_status_enum():
    """Test RedTeamJobStatus enum."""
    assert RedTeamJobStatus.QUEUED.value == "queued"
    assert RedTeamJobStatus.RUNNING.value == "running"
    assert RedTeamJobStatus.COMPLETED.value == "completed"
    assert RedTeamJobStatus.FAILED.value == "failed"
    assert RedTeamJobStatus.CANCELLED.value == "cancelled"


def test_category_stats():
    """Test CategoryStats model."""
    stats = CategoryStats(
        category="jailbreak",
        total_prompts=10,
        successful_attacks=3,
        attack_success_rate=0.3,
        avg_confidence=0.85
    )

    assert stats.category == "jailbreak"
    assert stats.total_prompts == 10
    assert stats.successful_attacks == 3
    assert stats.attack_success_rate == 0.3
    assert stats.avg_confidence == 0.85



