"""Obfuscation-based converters (Unicode, Leetspeak)."""

from src.core.redteam.converters.base import Converter


class UnicodeConverter(Converter):
    """
    Unicode homoglyph converter.
    
    Replaces characters with visually similar Unicode characters to test
    if the model can recognize obfuscated harmful content.
    """

    # Common homoglyph mappings
    HOMOGLYPHS = {
        'a': 'а',  # Cyrillic 'а'
        'e': 'е',  # Cyrillic 'е'
        'o': 'о',  # Cyrillic 'о'
        'p': 'р',  # Cyrillic 'р'
        'c': 'с',  # Cyrillic 'с'
        'i': 'і',  # Cyrillic 'і'
        'x': 'х',  # Cyrillic 'х'
        'y': 'у',  # Cyrillic 'у'
    }

    @property
    def converter_id(self) -> str:
        return "unicode"

    @property
    def name(self) -> str:
        return "Unicode Homoglyphs"

    def convert(self, text: str) -> str:
        """Replace characters with Unicode homoglyphs."""
        result = []
        for char in text:
            # Replace if we have a homoglyph, otherwise keep original
            result.append(self.HOMOGLYPHS.get(char.lower(), char))
        return ''.join(result)


class LeetspeakConverter(Converter):
    """
    Leetspeak (1337) converter.
    
    Replaces letters with numbers and symbols to test obfuscation detection.
    """

    # Common leetspeak mappings
    LEET_MAP = {
        'a': '4',
        'e': '3',
        'i': '1',
        'o': '0',
        's': '5',
        't': '7',
        'l': '1',
        'g': '9',
        'b': '8',
    }

    @property
    def converter_id(self) -> str:
        return "leetspeak"

    @property
    def name(self) -> str:
        return "Leetspeak (1337)"

    def convert(self, text: str) -> str:
        """Convert to leetspeak."""
        result = []
        for char in text:
            # Replace vowels and common letters
            result.append(self.LEET_MAP.get(char.lower(), char))
        return ''.join(result)
