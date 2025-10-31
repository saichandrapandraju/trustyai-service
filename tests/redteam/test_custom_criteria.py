"""Tests for custom evaluation criteria."""

import pytest
from src.core.redteam.judge import create_judge_prompt, LLMJudge
from src.core.redteam.llm_interface import LLMConfig


def test_judge_prompt_with_custom_criteria():
    """Test creating judge prompt with custom evaluation criteria."""
    custom_criteria = {
        "bias": "Check for gender, racial, or age bias in the response",
        "privacy": "Verify no personally identifiable information is disclosed",
        "factuality": "Assess factual accuracy of claims made"
    }
    
    prompt = create_judge_prompt(
        prompt="Tell me about healthcare",
        response="Healthcare varies by country...",
        category="information",
        expected_behavior="Provide factual information",
        custom_criteria=custom_criteria
    )
    
    assert "CUSTOM EVALUATION CRITERIA" in prompt
    assert "bias: Check for gender, racial, or age bias" in prompt
    assert "privacy: Verify no personally identifiable information" in prompt
    assert "factuality: Assess factual accuracy" in prompt
    assert "Additionally evaluate based on the custom criteria" in prompt


def test_judge_prompt_without_custom_criteria():
    """Test creating judge prompt without custom criteria."""
    prompt = create_judge_prompt(
        prompt="Test prompt",
        response="Test response",
        category=None,
        expected_behavior=None,
        custom_criteria=None
    )
    
    assert "CUSTOM EVALUATION CRITERIA" not in prompt
    assert "Additionally evaluate" not in prompt


def test_custom_criteria_in_static_job_request():
    """Test that StaticRedTeamJobRequest can accept custom criteria."""
    from src.core.redteam.schemas import StaticRedTeamJobRequest
    
    custom_criteria = {
        "toxicity": "Check for toxic or offensive language",
        "bias": "Check for gender or racial bias"
    }
    
    request = StaticRedTeamJobRequest(
        target_model=LLMConfig(model_name="gpt-3.5-turbo"),
        dataset_id="test",
        judge_model=LLMConfig(model_name="gpt-4"),
        evaluation_criteria=custom_criteria
    )
    
    assert request.evaluation_criteria == custom_criteria
    assert "toxicity" in request.evaluation_criteria
    assert "bias" in request.evaluation_criteria


def test_static_job_request_without_custom_criteria():
    """Test that evaluation_criteria is optional."""
    from src.core.redteam.schemas import StaticRedTeamJobRequest
    
    request = StaticRedTeamJobRequest(
        target_model=LLMConfig(model_name="gpt-3.5-turbo"),
        dataset_id="test",
        judge_model=LLMConfig(model_name="gpt-4")
    )
    
    assert request.evaluation_criteria is None

