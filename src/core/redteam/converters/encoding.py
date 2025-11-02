"""Encoding-based converters (Base64, ROT13)."""

import base64
import codecs

from src.core.redteam.converters.base import Converter
from typing import Set

class Base64Converter(Converter):
    """
    Base64 encoding converter.
    
    Tests if the model will decode and process base64-encoded harmful requests.
    """

    @property
    def converter_id(self) -> str:
        return "base64"

    @property
    def name(self) -> str:
        return "Base64 Encoding"

    def convert(self, text: str) -> Set[str]:
        """Encode text in base64."""
        return {base64.b64encode(text.encode('utf-8')).decode('utf-8')}


class ROT13Converter(Converter):
    """
    ROT13 cipher converter.
    
    Tests if the model will decode and process ROT13-encoded requests.
    """

    @property
    def converter_id(self) -> str:
        return "rot13"

    @property
    def name(self) -> str:
        return "ROT13 Cipher"

    def convert(self, text: str) -> Set[str]:
        """Apply ROT13 cipher."""
        return {codecs.encode(text, 'rot_13')}
