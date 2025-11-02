"""Generic HuggingFace dataset loader."""

import logging
from typing import List, Optional, Literal

from src.core.redteam.schemas import RedTeamPrompt, StaticRedTeamDataset

logger = logging.getLogger(__name__)


def load_hf_dataset(
    hf_dataset: str,
    hf_config: Optional[str] = None,
    split: str = "train",
    prompt_column: str = "prompt",
    category_column: Optional[str] = None,
    limit: Optional[int] = None,
    goal: Optional[str] = None,
    evals: Optional[List[str]] = None,
    evals_op: Literal["AND", "OR"] = "OR",
    override_dynamic_eval: bool = False
) -> StaticRedTeamDataset:
    """
    Generic loader for any HuggingFace dataset.
    
    Args:
        hf_dataset: HF dataset identifier (e.g., "JailbreakBench/JBB-Behaviors")
        hf_config: Config name if dataset has multiple configs
        split: Dataset split to load
        prompt_column: Column containing prompts
        category_column: Optional column for categories
        limit: Maximum prompts to load
        
    Returns:
        StaticRedTeamDataset with loaded prompts
    """
    try:
        from datasets import load_dataset
    except ImportError:
        raise ImportError(
            "HuggingFace datasets not installed. "
            "Install with: pip install 'trustyai-service[redteam]'"
        )
    
    try:
        logger.info(f"Loading HF dataset: {hf_dataset} (config={hf_config}, split={split})")
        
        # Load from HuggingFace
        if hf_config:
            dataset = load_dataset(hf_dataset, hf_config, split=split)
        else:
            dataset = load_dataset(hf_dataset, split=split)
        
        # Apply limit
        if limit and limit < len(dataset):
            dataset = dataset.select(range(limit))
        
        # Convert to RedTeamPrompt objects
        prompts = []
        for i, row in enumerate(dataset):
            # Get prompt text from specified column
            prompt_text = row.get(prompt_column, "")
            if not prompt_text:
                logger.warning(f"Row {i} missing prompt column '{prompt_column}', skipping")
                continue
            
            # Get category if column specified
            category = None
            if category_column and category_column in row:
                category = str(row[category_column]).lower().replace(" ", "_")
            
            prompt = RedTeamPrompt(
                prompt_id=f"{hf_dataset.split('/')[-1]}_{i}",
                prompt=prompt_text,
                category=category,
                expected_behavior="Model should refuse harmful requests",
                tags=[hf_dataset, split]
            )
            prompts.append(prompt)
        
        logger.info(f"Loaded {len(prompts)} prompts from {hf_dataset}")
        
        return StaticRedTeamDataset(
            dataset_id=f"{hf_dataset.replace('/', '-')}-{split}-{len(prompts)}",
            name=f"{hf_dataset} ({split})",
            description=f"Loaded from HuggingFace: {hf_dataset}",
            version="1.0.0",
            prompts=prompts,
            goal=goal,
            evals=evals,
            evals_op=evals_op,
            override_dynamic_eval=override_dynamic_eval,
            metadata={
                "source": hf_dataset,
                "split": split,
                "total_loaded": len(prompts)
            }
        )
        
    except Exception as e:
        logger.error(f"Error loading HF dataset {hf_dataset}: {e}")
        raise ValueError(f"Failed to load HF dataset: {e}")

