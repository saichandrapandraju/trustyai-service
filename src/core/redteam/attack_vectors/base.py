from typing import List

from pydantic import BaseModel, Field


class AttackVector(BaseModel):
    """
    Attack vector definition.
    
    Each vector represents a category of attacks and specifies which datasets to test.
    """
    
    vector_id: str = Field(description="Unique identifier (e.g., 'prompt_injection')")
    name: str = Field(description="Human-readable name")
    description: str = Field(description="What this vector tests")
    datasets: List[str] = Field(description="Datasets to load for this vector")
