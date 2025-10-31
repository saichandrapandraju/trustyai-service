"""Tests for unified LLM interface."""

import pytest
from src.core.redteam.llm_interface import LLMConfig


def test_llm_config_creation():
    """Test LLMConfig creation for attacker use case."""
    config = LLMConfig(
        model_type="openai",
        model_name="gpt-4",
        api_key="test-key",
        temperature=1.0,
        max_tokens=1024
    )
    
    assert config.model_type == "openai"
    assert config.model_name == "gpt-4"
    assert config.temperature == 1.0
    assert config.max_tokens == 1024


def test_llm_config_defaults():
    """Test LLMConfig with defaults."""
    config = LLMConfig(
        model_name="gpt-3.5-turbo"
    )
    
    assert config.model_type == "openai"  # Default
    assert config.temperature == 0.7  # Default
    assert config.max_tokens is None  # Default
    assert config.stream is True  # Default
    assert config.timeout_seconds == 300  # Default


def test_llm_config_with_endpoint():
    """Test LLMConfig with custom endpoint."""
    config = LLMConfig(
        model_type="openai",
        model_name="custom-model",
        endpoint="http://localhost:8000/v1",
        api_key="dummy"
    )
    
    assert config.endpoint == "http://localhost:8000/v1"
    assert config.api_key == "dummy"

