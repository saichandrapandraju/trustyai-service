"""Registry for dynamic attack strategies."""

from typing import Dict, Optional, Type

from src.core.redteam.strategies.base import AttackStrategy


class DynamicAttackRegistry:
    """
    Simple registry for dynamic attack strategies.
    
    Maps strategy IDs (e.g., "goat", "pair") to their implementation classes.
    """
    
    _strategies: Dict[str, Type[AttackStrategy]] = {}
    
    @classmethod
    def register(cls, strategy_id: str, strategy_class: Type[AttackStrategy]) -> None:
        """
        Register a new attack strategy.
        
        Args:
            strategy_id: Unique identifier (e.g., "goat")
            strategy_class: The strategy class (must inherit from AttackStrategy)
        """
        cls._strategies[strategy_id] = strategy_class
    
    @classmethod
    def get(cls, strategy_id: str) -> Optional[Type[AttackStrategy]]:
        """
        Get a strategy class by ID.
        
        Args:
            strategy_id: Strategy identifier
            
        Returns:
            Strategy class or None if not found
        """
        return cls._strategies.get(strategy_id)
    
    @classmethod
    def list_strategies(cls) -> list[str]:
        """List all registered strategy IDs."""
        return list(cls._strategies.keys())
    
    @classmethod
    def is_registered(cls, strategy_id: str) -> bool:
        """Check if a strategy is registered."""
        return strategy_id in cls._strategies


# ============================================================================
# AUTO-REGISTER BUILT-IN STRATEGIES
# ============================================================================

def _auto_register_builtin_strategies():
    """Auto-register all built-in strategies on module load."""
    try:
        from src.core.redteam.strategies.goat import GOATStrategy
        DynamicAttackRegistry.register("goat", GOATStrategy)
    except ImportError:
        pass  # GOAT strategy not available
    
    # Future strategies can be added here:
    # from src.core.redteam.strategies.pair_strategy import PAIRStrategy
    # DynamicAttackRegistry.register("pair", PAIRStrategy)


# Auto-register on module import
_auto_register_builtin_strategies()

