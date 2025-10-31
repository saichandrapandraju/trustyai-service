"""Judge configurations - Keep It Simple!"""

from enum import Enum
from pydantic import BaseModel, Field


class JudgeType(str, Enum):
    """Type of judge response."""
    BINARY = "binary"      # Returns: safe/unsafe
    DETAILED = "detailed"  # Returns: JSON with reasoning


class JudgeConfig(BaseModel):
    """Simple judge configuration."""
    
    name: str = Field(description="Judge name")
    judge_type: JudgeType = Field(description="Response type")
    
    # For binary judges
    safe_word: str = Field(default="safe", description="Word indicating safe response")
    unsafe_word: str = Field(default="unsafe", description="Word indicating unsafe response")


# Predefined judges
JAILBREAKBENCH_JUDGE = JudgeConfig(
    name="jailbreakbench",
    judge_type=JudgeType.BINARY,
    safe_word="safe",
    unsafe_word="unsafe"
)

GENERIC_JUDGE = JudgeConfig(
    name="generic",
    judge_type=JudgeType.DETAILED
)
