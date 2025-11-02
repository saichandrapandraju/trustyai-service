from typing import List

from pydantic import BaseModel, Field


class AttackVector(BaseModel):
    """
    Complete definition of an attack vector.
    
    Each vector represents a category of attacks and includes:
    - Which datasets to test
    - Which converters to apply (encoding/obfuscation)
    - Which dynamic attacks to run (multi-turn strategies)
    - Taxonomy mappings (OWASP, NIST, etc.)
    """
    
    # Identity
    vector_id: str = Field(description="Unique identifier (e.g., 'prompt_injection')")
    name: str = Field(description="Human-readable name")
    description: str = Field(description="What this vector tests")
    
    # Test resources
    datasets: List[str] = Field(description="Dataset IDs to load for this vector")
    
    # Taxonomy mappings
    taxonomy_ids: List[str] = Field(
        default_factory=list,
        description="Standards this vector maps to (e.g., ['OWASP-LLM-01', 'NIST-AI-100'])"
    )
    
    # Attack methods
    converter_ids: List[str] = Field(
        default_factory=list,
        description="Converters to apply (e.g., ['base64', 'rot13', 'unicode'])"
    )
    dynamic_attack_ids: List[str] = Field(
        default_factory=list,
        description="Dynamic attack strategies to use (e.g., ['goat', 'pair'])"
    )
    
    # Configuration
    severity: str = Field( # TODO: I don't think this is necessary, come back to this
        default="medium",
        description="Severity level for prioritization (low, medium, high, critical)"
    )
    recommended_batch_size: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Recommended batch size for testing"
    )
