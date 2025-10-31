"""Built-in dataset loader - loads from local JSON files on first request."""

import logging
from pathlib import Path
from typing import Dict, Optional

from src.core.redteam.datasets.loaders.file import load_from_file
from src.core.redteam.schemas import StaticRedTeamDataset

logger = logging.getLogger(__name__)

# Directory where built-in datasets are stored
# __file__ is in: .../datasets/loaders/builtin.py
# We want: .../datasets/ (parent.parent gives us that)
DATASETS_DIR = Path(__file__).parent.parent

# Cache for loaded datasets (lazy loading)
_dataset_cache: Dict[str, StaticRedTeamDataset] = {}


def get_builtin_dataset(dataset_id: str) -> Optional[StaticRedTeamDataset]:
    """
    Get a built-in dataset by ID (loads and caches on first request).
    
    Args:
        dataset_id: Dataset identifier
        
    Returns:
        StaticRedTeamDataset if found, None otherwise
    """
    # Check cache first
    if dataset_id in _dataset_cache:
        logger.debug(f"Returning cached dataset: {dataset_id}")
        return _dataset_cache[dataset_id]
    
    # Try to load from disk
    dataset_file = DATASETS_DIR / f"{dataset_id}.json"
    logger.debug(f"Trying: {dataset_file}")
    
    if not dataset_file.exists():
        # Try without version suffix (e.g., sample-jailbreaks-v1 → sample_jailbreaks)
        base_name = dataset_id.split("-v")[0].replace("-", "_")
        dataset_file = DATASETS_DIR / f"{base_name}.json"
        logger.debug(f"File not found, trying: {dataset_file}")
    
    if dataset_file.exists():
        try:
            dataset = load_from_file(str(dataset_file))
            _dataset_cache[dataset_id] = dataset
            logger.info(f"Loaded and cached built-in dataset: {dataset_id}")
            return dataset
        except Exception as e:
            logger.error(f"Error loading built-in dataset {dataset_id}: {e}")
            return None
    
    logger.warning(f"Built-in dataset not found: {dataset_id}")
    return None


def list_builtin_datasets() -> list:
    """
    List all available built-in datasets.
    
    Returns:
        List of dataset metadata dicts
    """
    datasets = []
    
    if not DATASETS_DIR.exists():
        return datasets
    
    for dataset_file in DATASETS_DIR.glob("*.json"):
        try:
            dataset = load_from_file(str(dataset_file))
            datasets.append({
                "dataset_id": dataset.dataset_id,
                "name": dataset.name,
                "description": dataset.description,
                "version": dataset.version,
                "num_prompts": len(dataset.prompts)
            })
            # Cache it while we're at it
            _dataset_cache[dataset.dataset_id] = dataset
        except Exception as e:
            logger.error(f"Error loading {dataset_file}: {e}")
    
    return datasets

