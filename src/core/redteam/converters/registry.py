"""Registry of all available converters."""

from typing import Dict, List, Optional

from src.core.redteam.converters.base import Converter
from src.core.redteam.converters.encoding import Base64Converter, ROT13Converter
from src.core.redteam.converters.obfuscation import LeetspeakConverter, UnicodeConverter


class ConverterRegistry:
    """Central registry for all prompt converters."""
    
    _converters: Dict[str, Converter] = {
        "base64": Base64Converter(),
        "rot13": ROT13Converter(),
        "unicode": UnicodeConverter(),
        "leetspeak": LeetspeakConverter(),
    }
    
    @classmethod
    def get_converter(cls, converter_id: str) -> Optional[Converter]:
        """
        Get a converter by ID.
        
        Args:
            converter_id: Converter identifier
            
        Returns:
            Converter instance or None if not found
        """
        return cls._converters.get(converter_id)
    
    @classmethod
    def list_converters(cls) -> List[Converter]:
        """List all available converters."""
        return list(cls._converters.values())
    
    @classmethod
    def get_all_converter_ids(cls) -> List[str]:
        """Get all converter IDs."""
        return list(cls._converters.keys())
    
    @classmethod
    def apply_converter(cls, converter_id: str, text: str) -> str:
        """
        Apply a converter to text.
        
        Args:
            converter_id: Converter to use
            text: Original text
            
        Returns:
            Converted text with instruction
            
        Raises:
            ValueError: If converter not found
        """
        converter = cls.get_converter(converter_id)
        if not converter:
            raise ValueError(f"Converter '{converter_id}' not found")
        
        return converter.apply(text)

