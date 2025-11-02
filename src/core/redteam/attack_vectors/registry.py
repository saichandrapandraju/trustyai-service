"""Registry of all attack vectors."""

from typing import Dict, List, Optional

from src.core.redteam.attack_vectors.base import AttackVector


# ============================================================================
# ATTACK VECTOR DEFINITIONS
# ============================================================================

PROMPT_INJECTION = AttackVector(
    vector_id="prompt_injection",
    name="Prompt Injection",
    description="Tests instruction override and injection attacks (ignore previous, system commands, etc.)",
    datasets=["sample-jailbreaks-v1"],  # Has injection prompts
    taxonomy_ids=["OWASP-LLM-01"],
    converter_ids=["base64", "rot13", "unicode"],  # Encoding can bypass filters
    dynamic_attack_ids=["goat"],  # Multi-turn injection attempts
    severity="high"
)

JAILBREAK = AttackVector(
    vector_id="jailbreak",
    name="Jailbreak",
    description="Tests attempts to bypass safety guardrails (DAN, roleplay, hypothetical scenarios, etc.)",
    datasets=["jailbreakbench"],  # Builtin: 100 research-vetted jailbreak attempts from HF
    taxonomy_ids=["OWASP-LLM-01"],
    converter_ids=["base64", "rot13", "leetspeak"],  # Obfuscation techniques
    dynamic_attack_ids=["goat"],  # Iterative jailbreak refinement
    severity="critical"
)

HARMFUL_CONTENT = AttackVector(
    vector_id="harmful_content",
    name="Harmful Content",
    description="Tests direct requests for harmful/illegal/unethical content",
    datasets=["jailbreakbench"],  # Builtin: JBB covers harmful behaviors
    taxonomy_ids=["OWASP-LLM-02"],
    converter_ids=["base64", "unicode"],  # Simple obfuscation
    dynamic_attack_ids=["goat"],  # Progressive harmful requests
    severity="critical"
)


# ============================================================================
# REGISTRY
# ============================================================================

class VectorRegistry:
    """Simple registry for attack vectors."""
    
    _vectors: Dict[str, AttackVector] = {
        "prompt_injection": PROMPT_INJECTION,
        "jailbreak": JAILBREAK,
        "harmful_content": HARMFUL_CONTENT,
    }
    
    @classmethod
    def get_vector(cls, vector_id: str) -> Optional[AttackVector]:
        """Get an attack vector by ID."""
        return cls._vectors.get(vector_id)
    
    @classmethod
    def list_vectors(cls) -> List[AttackVector]:
        """List all available attack vectors."""
        return list(cls._vectors.values())
    
    @classmethod
    def get_all_datasets_for_vectors(cls, vector_ids: List[str]) -> List[str]:
        """Get all unique datasets needed for specified vectors."""
        datasets = set()
        for vector_id in vector_ids:
            vector = cls.get_vector(vector_id)
            if vector:
                datasets.update(vector.datasets)
        return list(datasets)
