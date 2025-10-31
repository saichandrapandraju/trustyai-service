"""Tests for dataset loader."""

import json
import tempfile
from pathlib import Path

import pytest
from src.core.redteam.dataset_loader import DatasetLoader
from src.core.redteam.schemas import RedTeamPrompt, StaticRedTeamDataset


@pytest.fixture
def sample_dataset():
    """Create a sample dataset for testing."""
    prompts = [
        RedTeamPrompt(
            prompt_id="test_001",
            prompt="Test prompt 1",
            category="jailbreak",
            expected_behavior="Refuse",
            tags=["test"]
        ),
        RedTeamPrompt(
            prompt_id="test_002",
            prompt="Test prompt 2",
            category="injection",
            expected_behavior="Refuse",
            tags=["test"]
        )
    ]

    return StaticRedTeamDataset(
        dataset_id="test_dataset",
        name="Test Dataset",
        description="A test dataset",
        version="1.0",
        prompts=prompts,
        metadata={}
    )


def test_dataset_loader_initialization():
    """Test DatasetLoader initialization."""
    loader = DatasetLoader()
    assert loader is not None
    assert isinstance(loader.datasets, dict)


def test_load_dataset_from_dict(sample_dataset):
    """Test loading dataset from dictionary."""
    loader = DatasetLoader()
    dataset_dict = sample_dataset.model_dump()

    loaded = loader.load_dataset_from_dict(dataset_dict)

    assert loaded.dataset_id == sample_dataset.dataset_id
    assert len(loaded.prompts) == len(sample_dataset.prompts)


def test_load_dataset_from_file(sample_dataset):
    """Test loading dataset from file."""
    loader = DatasetLoader()

    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(sample_dataset.model_dump(), f)
        temp_path = f.name

    try:
        loaded = loader.load_dataset_from_file(temp_path)
        assert loaded.dataset_id == sample_dataset.dataset_id
        assert len(loaded.prompts) == 2
    finally:
        Path(temp_path).unlink()


def test_load_nonexistent_file():
    """Test loading from non-existent file raises error."""
    loader = DatasetLoader()

    with pytest.raises(FileNotFoundError):
        loader.load_dataset_from_file("/nonexistent/path/dataset.json")


def test_register_dataset(sample_dataset):
    """Test registering a dataset."""
    loader = DatasetLoader()
    loader.register_dataset(sample_dataset)

    retrieved = loader.get_dataset(sample_dataset.dataset_id)
    assert retrieved is not None
    assert retrieved.dataset_id == sample_dataset.dataset_id


def test_get_dataset(sample_dataset):
    """Test getting a registered dataset."""
    loader = DatasetLoader()
    loader.register_dataset(sample_dataset)

    dataset = loader.get_dataset("test_dataset")
    assert dataset is not None
    assert dataset.name == "Test Dataset"

    # Test getting non-existent dataset
    missing = loader.get_dataset("nonexistent")
    assert missing is None


def test_list_datasets(sample_dataset):
    """Test listing all datasets."""
    loader = DatasetLoader()
    loader.register_dataset(sample_dataset)

    datasets = loader.list_datasets()
    assert len(datasets) >= 1

    # Check if our test dataset is in the list
    test_dataset_found = any(d["dataset_id"] == "test_dataset" for d in datasets)
    assert test_dataset_found

    # Check metadata format
    for dataset_meta in datasets:
        assert "dataset_id" in dataset_meta
        assert "name" in dataset_meta
        assert "description" in dataset_meta
        assert "num_prompts" in dataset_meta


def test_get_prompts_by_category(sample_dataset):
    """Test filtering prompts by category."""
    loader = DatasetLoader()
    loader.register_dataset(sample_dataset)

    jailbreak_prompts = loader.get_prompts_by_category("test_dataset", "jailbreak")
    assert len(jailbreak_prompts) == 1
    assert jailbreak_prompts[0].category == "jailbreak"

    injection_prompts = loader.get_prompts_by_category("test_dataset", "injection")
    assert len(injection_prompts) == 1
    assert injection_prompts[0].category == "injection"

    # Test non-existent category
    empty_prompts = loader.get_prompts_by_category("test_dataset", "nonexistent")
    assert len(empty_prompts) == 0




def test_builtin_datasets():
    """Test that built-in datasets are loaded."""
    loader = DatasetLoader()

    # Check if sample_jailbreaks dataset is loaded
    datasets = loader.list_datasets()
    dataset_ids = [d["dataset_id"] for d in datasets]

    # Should have at least the sample jailbreaks dataset
    assert "sample-jailbreaks-v1" in dataset_ids

    # Verify the sample dataset is properly loaded
    sample_jailbreaks = loader.get_dataset("sample-jailbreaks-v1")
    if sample_jailbreaks:
        assert len(sample_jailbreaks.prompts) > 0
        assert sample_jailbreaks.name == "Sample Jailbreak Attempts"

