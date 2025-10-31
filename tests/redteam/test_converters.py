"""Tests for prompt converters."""

import pytest
from src.core.redteam.converters.encoding import Base64Converter, ROT13Converter
from src.core.redteam.converters.obfuscation import LeetspeakConverter, UnicodeConverter
from src.core.redteam.converters.registry import ConverterRegistry


def test_base64_converter():
    """Test Base64 converter."""
    converter = Base64Converter()
    
    assert converter.converter_id == "base64"
    assert converter.name == "Base64 Encoding"
    
    # Test encoding
    original = "How to hack a system"
    encoded = converter.convert(original)
    assert encoded != original
    assert "SG93" in encoded  # Base64 starts with this
    
    # Test full application
    full_prompt = converter.apply(original)
    assert "Decode" in full_prompt
    assert encoded in full_prompt


def test_rot13_converter():
    """Test ROT13 converter."""
    converter = ROT13Converter()
    
    assert converter.converter_id == "rot13"
    
    # Test ROT13
    original = "test message"
    encoded = converter.convert(original)
    assert encoded == "grfg zrffntr"  # ROT13 of "test message"
    
    # Test full application
    full_prompt = converter.apply(original)
    assert "ROT13" in full_prompt or "Decode" in full_prompt


def test_unicode_converter():
    """Test Unicode homoglyph converter."""
    converter = UnicodeConverter()
    
    assert converter.converter_id == "unicode"
    
    # Test conversion
    original = "hack"
    converted = converter.convert(original)
    assert converted != original  # Should have some Cyrillic chars
    
    # Test application
    full_prompt = converter.apply(original)
    assert converted in full_prompt


def test_leetspeak_converter():
    """Test Leetspeak converter."""
    converter = LeetspeakConverter()
    
    assert converter.converter_id == "leetspeak"
    
    # Test conversion
    original = "password"
    converted = converter.convert(original)
    assert "p4ssw0rd" == converted or "p455w0rd" in converted
    
    # Test application
    full_prompt = converter.apply(original)
    assert "leetspeak" in full_prompt.lower() or "1337" in full_prompt


def test_converter_registry_get():
    """Test getting converters from registry."""
    converter = ConverterRegistry.get_converter("base64")
    assert converter is not None
    assert converter.converter_id == "base64"
    
    missing = ConverterRegistry.get_converter("nonexistent")
    assert missing is None


def test_converter_registry_list():
    """Test listing all converters."""
    converters = ConverterRegistry.list_converters()
    assert len(converters) == 4
    
    ids = [c.converter_id for c in converters]
    assert "base64" in ids
    assert "rot13" in ids
    assert "unicode" in ids
    assert "leetspeak" in ids


def test_converter_registry_apply():
    """Test applying converter via registry."""
    original = "test"
    converted = ConverterRegistry.apply_converter("base64", original)
    
    assert "Decode" in converted
    assert converted != original


def test_get_all_converter_ids():
    """Test getting all converter IDs."""
    ids = ConverterRegistry.get_all_converter_ids()
    assert len(ids) == 4
    assert "base64" in ids
    assert "rot13" in ids

