"""Tests for dataset registry and loaders."""

import pytest
from src.core.redteam.datasets.dataset_registry import DatasetRegistry
from src.core.redteam.datasets.dataset_sources import DatasetSourceConfig


def test_builtin_sample_jailbreaks():
    """Test loading built-in sample-jailbreaks dataset."""
    config = DatasetSourceConfig(
        source="builtin",
        dataset_id="sample-jailbreaks-v1"
    )
    
    dataset = DatasetRegistry.load_dataset(config)
    
    assert dataset is not None
    assert dataset.dataset_id == "sample-jailbreaks-v1"
    assert len(dataset.prompts) > 0


@pytest.mark.skip(reason="Requires HuggingFace datasets and network access")
def test_builtin_jailbreakbench():
    """Test loading builtin JailbreakBench (actually loads from HF)."""
    config = DatasetSourceConfig(
        source="builtin",
        dataset_id="jailbreakbench",
        limit=5
    )
    
    dataset = DatasetRegistry.load_dataset(config)
    
    assert dataset is not None
    assert len(dataset.prompts) == 5


@pytest.mark.skip(reason="Requires HuggingFace datasets and network access")
def test_hf_dataset_direct():
    """Test loading directly from HuggingFace."""
    config = DatasetSourceConfig(
        source="hf",
        hf_dataset="JailbreakBench/JBB-Behaviors",
        hf_config="behaviors",
        split="harmful",
        prompt_column="Goal",
        limit=3
    )
    
    dataset = DatasetRegistry.load_dataset(config)
    
    assert dataset is not None
    assert len(dataset.prompts) == 3


def test_builtin_with_limit():
    """Test builtin dataset with limit override."""
    config = DatasetSourceConfig(
        source="builtin",
        dataset_id="sample-jailbreaks-v1",
        limit=5
    )
    
    dataset = DatasetRegistry.load_dataset(config)
    
    assert dataset is not None
    assert len(dataset.prompts) == 5


def test_unknown_builtin_error():
    """Test error when builtin dataset not found."""
    config = DatasetSourceConfig(
        source="builtin",
        dataset_id="nonexistent-dataset"
    )
    
    with pytest.raises(ValueError, match="Unknown builtin"):
        DatasetRegistry.load_dataset(config)
