"""Simple dataset registry."""

import logging

from src.core.redteam.datasets.dataset_sources import BUILTIN_DATASETS, DatasetSourceConfig
from src.core.redteam.schemas import StaticRedTeamDataset

logger = logging.getLogger(__name__)


class DatasetRegistry:
    """Simple registry for loading datasets from different sources."""

    @staticmethod
    def load_dataset(config: DatasetSourceConfig) -> StaticRedTeamDataset:
        """
        Load a dataset based on source configuration.
        
        Args:
            config: Dataset source configuration
            
        Returns:
            Loaded StaticRedTeamDataset
        """
        # BUILTIN: Resolve to actual config
        if config.source == "builtin":
            if not config.dataset_id:
                raise ValueError("dataset_id required for builtin source")
            
            if config.dataset_id not in BUILTIN_DATASETS:
                raise ValueError(f"Unknown builtin dataset: {config.dataset_id}")
            
            # Get builtin mapping
            builtin_config = BUILTIN_DATASETS[config.dataset_id]
            
            # Check if it's a pre-loaded dataset
            if builtin_config.get("use_preloaded"):
                # Load from built-in loaders
                from src.core.redteam.datasets.loaders.builtin import get_builtin_dataset
                dataset = get_builtin_dataset(config.dataset_id)
                if not dataset:
                    raise ValueError(f"Built-in dataset not found: {config.dataset_id}")
                
                # Apply limit if specified
                if config.limit and config.limit < len(dataset.prompts):
                    # Create a copy with limited prompts
                    limited_dataset = StaticRedTeamDataset(
                        dataset_id=f"{dataset.dataset_id}-limited-{config.limit}",
                        name=dataset.name,
                        description=dataset.description,
                        version=dataset.version,
                        prompts=dataset.prompts[:config.limit],
                        metadata=dataset.metadata
                    )
                    return limited_dataset
                
                return dataset
            
            # Otherwise, recurse with the mapped config
            if config.limit:
                builtin_config = {**builtin_config, "limit": config.limit}
            
            actual_config = DatasetSourceConfig(**builtin_config)
            return DatasetRegistry.load_dataset(actual_config)
        
        # HF: Load from HuggingFace
        elif config.source == "hf":
            from src.core.redteam.datasets.loaders.huggingface import load_hf_dataset
            
            return load_hf_dataset(
                hf_dataset=config.hf_dataset,
                hf_config=config.hf_config,
                split=config.split,
                prompt_column=config.prompt_column,
                category_column=config.category_column,
                limit=config.limit
            )
        
        # CUSTOM_FILE: Load from JSON file
        elif config.source == "custom_file":
            from src.core.redteam.datasets.loaders.file import load_from_file
            
            if not config.file_path:
                raise ValueError("file_path required for custom_file source")
            
            dataset = load_from_file(config.file_path)
            
            # Apply limit if specified
            if config.limit and config.limit < len(dataset.prompts):
                limited_dataset = StaticRedTeamDataset(
                    dataset_id=f"{dataset.dataset_id}-limited-{config.limit}",
                    name=dataset.name,
                    description=dataset.description,
                    version=dataset.version,
                    prompts=dataset.prompts[:config.limit],
                    metadata=dataset.metadata
                )
                return limited_dataset
            
            return dataset
        
        else:
            raise ValueError(f"Unknown source: {config.source}")
