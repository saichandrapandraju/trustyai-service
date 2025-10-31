"""Tests for attack vectors."""

import pytest
from src.core.redteam.attack_vectors.base import AttackVector
from src.core.redteam.attack_vectors.registry import (
    HARMFUL_CONTENT,
    JAILBREAK,
    PROMPT_INJECTION,
    VectorRegistry,
)


def test_attack_vector_creation():
    """Test AttackVector model."""
    vector = AttackVector(
        vector_id="test",
        name="Test Vector",
        description="A test vector",
        datasets=["dataset1", "dataset2"]
    )
    
    assert vector.vector_id == "test"
    assert len(vector.datasets) == 2


def test_prompt_injection_vector():
    """Test prompt injection vector definition."""
    assert PROMPT_INJECTION.vector_id == "prompt_injection"
    assert "injection" in PROMPT_INJECTION.description.lower()
    assert len(PROMPT_INJECTION.datasets) > 0


def test_jailbreak_vector():
    """Test jailbreak vector definition."""
    assert JAILBREAK.vector_id == "jailbreak"
    assert "jailbreakbench" in JAILBREAK.datasets


def test_harmful_content_vector():
    """Test harmful content vector definition."""
    assert HARMFUL_CONTENT.vector_id == "harmful_content"
    assert len(HARMFUL_CONTENT.datasets) > 0


def test_vector_registry_get():
    """Test getting vectors from registry."""
    vector = VectorRegistry.get_vector("prompt_injection")
    assert vector is not None
    assert vector.vector_id == "prompt_injection"
    
    missing = VectorRegistry.get_vector("nonexistent")
    assert missing is None


def test_vector_registry_list():
    """Test listing all vectors."""
    vectors = VectorRegistry.list_vectors()
    assert len(vectors) == 3
    vector_ids = [v.vector_id for v in vectors]
    assert "prompt_injection" in vector_ids
    assert "jailbreak" in vector_ids
    assert "harmful_content" in vector_ids


def test_get_datasets_for_vectors():
    """Test getting all datasets for vectors."""
    datasets = VectorRegistry.get_all_datasets_for_vectors(["jailbreak", "harmful_content"])
    assert len(datasets) > 0
    assert "jailbreakbench" in datasets


def test_get_datasets_for_single_vector():
    """Test getting datasets for a single vector."""
    datasets = VectorRegistry.get_all_datasets_for_vectors(["prompt_injection"])
    assert len(datasets) > 0
