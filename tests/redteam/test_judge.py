"""Tests for LLM judge."""

import pytest
from src.core.redteam.judge import create_judge_prompt, LLMJudge
from src.core.redteam.llm_interface import LLMConfig
from src.core.redteam.schemas import JudgeEvaluation


def test_create_judge_prompt_with_all_fields():
    """Test creating judge prompt with all fields."""
    prompt = create_judge_prompt(
        prompt="Test adversarial prompt",
        response="Test response",
        category="jailbreak",
        expected_behavior="Refuse the request"
    )
    
    assert "Test adversarial prompt" in prompt
    assert "Test response" in prompt
    assert "jailbreak" in prompt
    assert "Refuse the request" in prompt


def test_create_judge_prompt_with_optional_fields():
    """Test creating judge prompt with optional fields as None."""
    prompt = create_judge_prompt(
        prompt="Test adversarial prompt",
        response="Test response",
        category=None,
        expected_behavior=None
    )
    
    assert "Test adversarial prompt" in prompt
    assert "Test response" in prompt
    # Should not crash with None values


def test_create_judge_prompt_partial_optional():
    """Test creating judge prompt with some optional fields."""
    prompt = create_judge_prompt(
        prompt="Test adversarial prompt",
        response="Test response",
        category="jailbreak",
        expected_behavior=None
    )
    
    assert "Test adversarial prompt" in prompt
    assert "jailbreak" in prompt


def test_judge_evaluation_minimal():
    """Test JudgeEvaluation with minimal required fields."""
    eval = JudgeEvaluation(
        attack_success=True,
        confidence=0.9
    )
    
    assert eval.attack_success is True
    assert eval.confidence == 0.9
    assert eval.reasoning is None


def test_judge_evaluation_with_reasoning():
    """Test JudgeEvaluation with optional reasoning."""
    eval = JudgeEvaluation(
        attack_success=False,
        confidence=0.95,
        reasoning="Model safely refused the request"
    )
    
    assert eval.attack_success is False
    assert eval.confidence == 0.95
    assert eval.reasoning == "Model safely refused the request"

