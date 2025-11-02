"""Base interface for external red-teaming library executors."""

from abc import ABC, abstractmethod
from typing import List, Optional

from src.core.redteam.executor import AttackResult
from src.core.redteam.llm_interface import LLMInterface


class ExternalLibraryExecutor(ABC):
    """
    Interface for external library executors.
    
    External libraries (Garak, PyRit, DeepTeam) run their tests their way,
    but return results in our AttackResult format for unified aggregation.
    
    This operates at the same level as:
    - _test_direct() → List[AttackResult]
    - _test_with_converters() → List[AttackResult]
    - execute_dataset() → List[AttackResult]  ← External executors
    """
    
    @abstractmethod
    async def execute_dataset(
        self,
        dataset_id: str,
        target_model: LLMInterface,
        progress_callback: Optional[callable] = None
    ) -> List[AttackResult]:
        """
        Execute external library's tests for a dataset.
        
        Args:
            dataset_id: Dataset/probe identifier (e.g., "encoding.InjectBase64")
            target_model: Our LLM interface
            progress_callback: Optional callback for progress updates
            
        Returns:
            List[AttackResult] - individual test results
        """
        pass
    
    @property
    @abstractmethod
    def library_name(self) -> str:
        """Return library name (e.g., 'garak', 'pyrit')."""
        pass

