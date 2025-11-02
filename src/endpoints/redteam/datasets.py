"""Dataset management endpoints for red teaming."""

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException
from src.core.redteam.datasets.dataset_registry import DatasetRegistry
from src.core.redteam.datasets.dataset_sources import DatasetSourceConfig
from src.core.redteam.datasets.loaders.builtin import get_builtin_dataset, list_builtin_datasets
from src.core.redteam.schemas import StaticRedTeamDataset

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/redteam/datasets", summary="List available datasets")
async def list_datasets() -> Dict[str, List[Dict[str, str]]]:
    """
    List all available builtin datasets.
    
    Returns minimal info: id, name/path, source type.
    """
    from src.core.redteam.datasets.dataset_sources import BUILTIN_DATASETS
    
    datasets = []
    
    # Auto-discover all builtin datasets
    for builtin_id, builtin_config in BUILTIN_DATASETS.items():
        dataset_entry = {
            "id": builtin_id,
            "source": "builtin"
        }
        
        # Add name (HF path or dataset id)
        if builtin_config.get("use_preloaded"):
            dataset_entry["name"] = builtin_id
            dataset_entry["type"] = "preloaded"
        elif builtin_config.get("hf_dataset"):
            dataset_entry["name"] = builtin_config["hf_dataset"]
            dataset_entry["type"] = "huggingface"
        else:
            dataset_entry["name"] = builtin_id
            dataset_entry["type"] = "unknown"
        
        datasets.append(dataset_entry)
    
    return {"datasets": datasets}


@router.get("/redteam/datasets/{dataset_id}", summary="Get dataset details")
async def get_dataset(dataset_id: str) -> StaticRedTeamDataset:
    """
    Get details of a specific built-in dataset.

    Args:
        dataset_id: Dataset identifier (e.g., 'jailbreakbench', 'sample-jailbreaks-v1')

    Returns:
        Complete dataset with all prompts

    Raises:
        HTTPException: If dataset not found
    """
    try:
        # Load via registry (handles both file-based and HF-based builtins)
        config = DatasetSourceConfig(
            source="builtin",
            dataset_id=dataset_id
        )
        dataset = DatasetRegistry.load_dataset(config)
        return dataset
    
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error loading dataset {dataset_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load dataset: {str(e)}")


@router.post("/redteam/datasets/load", summary="Preview dataset configuration")
async def preview_dataset(config: DatasetSourceConfig) -> StaticRedTeamDataset:
    """
    Preview/test a dataset configuration before using it in an evaluation.
    
    Useful for:
    - Testing HuggingFace dataset configs
    - Verifying custom file paths
    - Checking prompt counts
    - Inspecting dataset structure

    Args:
        config: Dataset source configuration to test

    Returns:
        Loaded dataset with prompts (for preview)

    Raises:
        HTTPException: If dataset cannot be loaded
        
    Example:
        ```json
        {
          "source": "hf",
          "hf_dataset": "JailbreakBench/JBB-Behaviors",
          "hf_config": "behaviors",
          "split": "harmful",
          "prompt_column": "Goal",
          "limit": 5
        }
        ```
    """
    try:
        dataset = DatasetRegistry.load_dataset(config)
        logger.info(f"Previewed dataset: {config.source} → {len(dataset.prompts)} prompts")
        return dataset

    except Exception as e:
        logger.error(f"Error loading dataset: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load dataset: {str(e)}")
