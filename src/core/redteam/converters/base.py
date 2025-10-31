"""Base class for prompt converters."""

from abc import ABC, abstractmethod


class Converter(ABC):
    """
    Abstract base class for prompt converters.
    
    Converters transform prompts to test if encoding/obfuscation can bypass safety measures.
    """

    @abstractmethod
    def convert(self, text: str) -> str:
        """
        Convert/encode the input text.
        
        Args:
            text: Original text to convert
            
        Returns:
            Converted/encoded text
        """
        pass

    @property
    @abstractmethod
    def converter_id(self) -> str:
        """Return unique identifier for this converter (e.g., 'base64')."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Return human-readable name (e.g., 'Base64 Encoding')."""
        pass

    def apply(self, prompt: str) -> str:
        """
        Apply the converter with instruction.
        
        Args:
            prompt: Original prompt
            
        Returns:
            Full prompt with instruction + converted text
        """
        return self.convert(prompt)

