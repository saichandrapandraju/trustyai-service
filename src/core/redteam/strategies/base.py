"""Base class for attack strategies."""

from abc import ABC, abstractmethod

from src.core.redteam.llm_interface import LLMInterface
from src.core.redteam.schemas import DynamicAttackSession


class AttackStrategy(ABC):
    """
    Abstract base class for dynamic red teaming attack strategies.
    
    All attack strategies (GOAT, PAIR, Simple, etc.) should inherit from this class.
    """

    def __init__(
        self,
        attacker_model: LLMInterface,
        target_model: LLMInterface,
        max_iterations: int = 5,
        early_stop: bool = True
    ) -> None:
        """
        Initialize the attack strategy.
        
        Args:
            attacker_model: The attacker LLM
            target_model: The target LLM being tested
            max_iterations: Maximum conversation turns per session
            early_stop: Stop when attack succeeds
        """
        self.attacker_model = attacker_model
        self.target_model = target_model
        self.max_iterations = max_iterations
        self.early_stop = early_stop

    @abstractmethod
    async def run_attack_session(
        self,
        goal: str
    ) -> DynamicAttackSession:
        """
        Execute one attack session for a specific goal.
        
        Args:
            goal: The attack objective to achieve
            
        Returns:
            DynamicAttackSession with complete conversation and results
        """
        pass

    @property
    @abstractmethod
    def strategy_name(self) -> str:
        """Return the name of this strategy (e.g., 'goat', 'pair', 'simple')."""
        pass

