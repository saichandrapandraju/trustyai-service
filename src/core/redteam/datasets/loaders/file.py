"""File-based dataset loader for JSON files."""

import json
import logging
from pathlib import Path

from src.core.redteam.schemas import StaticRedTeamDataset

logger = logging.getLogger(__name__)


def load_from_file(file_path: str) -> StaticRedTeamDataset:
    """
    Load a dataset from a JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Loaded StaticRedTeamDataset
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format is invalid
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset file not found: {file_path}")
    
    try:
        with open(path, "r") as f:
            data = json.load(f)
        
        return StaticRedTeamDataset(**data)
    
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in dataset file: {e}")
    except Exception as e:
        raise ValueError(f"Error parsing dataset: {e}")

