from typing import Literal, Optional, List

from pydantic import BaseModel, Field


class DatasetSourceConfig(BaseModel):
    """Simple configuration for loading datasets."""
    
    # Source type (where to get data from)
    source: Literal["hf", "builtin", "custom_file"] = Field(
        description="Dataset source: 'hf' (HuggingFace), 'builtin' (pre-configured), 'custom_file' (local JSON)"
    )
    
    # For HuggingFace (generic)
    hf_dataset: Optional[str] = Field(None, description="HF dataset path (e.g., 'JailbreakBench/JBB-Behaviors')")
    hf_config: Optional[str] = Field(None, description="HF dataset config name (e.g., 'behaviors')")
    split: Optional[str] = Field("harmful", description="Dataset split (e.g., 'harmful', 'train')")
    prompt_column: Optional[str] = Field("Goal", description="Column name containing prompts")
    category_column: Optional[str] = Field("Category", description="Column name for categories (optional)")
    
    # For builtin (convenience aliases)
    dataset_id: Optional[str] = Field(None, description="Builtin dataset ID (e.g., 'jailbreakbench', 'sample-jailbreaks')")
    
    # For custom files
    file_path: Optional[str] = Field(None, description="Path to custom JSON dataset file")
    
    # Universal options
    limit: Optional[int] = Field(None, description="Maximum number of prompts to load")

    # For dataset evaluation
    goal: Optional[str] = Field(None, description="Goal of the dataset (used for judge prompting)")
    evals: Optional[List[str]] = Field(None, description="Evaluation methods to use (used for judge prompting)")
    evals_op: Literal["AND", "OR"] = Field(default="OR", description="How to combine multiple eval results (AND=all must pass, OR=any must pass)")
    override_dynamic_eval: bool = Field(default=False, description="If True, use these evals even for dynamic attacks (instead of GOAT judge)")

# Builtin dataset mappings
BUILTIN_DATASETS = {
    # HuggingFace datasets (loaded on-demand)
    "jailbreakbench": {
        "source": "hf",
        "hf_dataset": "JailbreakBench/JBB-Behaviors",
        "hf_config": "behaviors",
        "split": "harmful",
        "prompt_column": "Goal",
        "category_column": "Category",
        "goal": "Elicit harmful behavior from the model and test its resistance to jailbreak attempts",
        "evals": ["contains_refusal"]
    },
    
    # Local datasets (pre-loaded by DatasetLoader)
    # These use a special marker that tells registry to use the pre-loaded cache
    "sample-jailbreaks-v1": {
        "use_preloaded": True  # Flag to use DatasetLoader cache
    }
}
