"""Unified LLM interface for all red teaming models (target, attacker, judge)."""

import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class LLMConfig(BaseModel):
    """Unified configuration for all LLMs used in red teaming."""

    model_type: str = Field(
        default="openai",
        description="Model type - use 'openai' for OpenAI-compatible endpoints (includes vLLM)"
    )
    model_name: str = Field(..., description="Model name or identifier")
    endpoint: Optional[str] = Field(None, description="Custom endpoint URL (e.g., vLLM server)")
    api_key: Optional[str] = Field(None, description="API key for authentication")
    headers: Optional[Dict[str, str]] = Field(None, description="Additional headers")
    temperature: float = Field(0.7, ge=0.0, le=2.0, description="Sampling temperature")
    max_tokens: Optional[int] = Field(None, description="Maximum tokens to generate")
    timeout_seconds: int = Field(300, ge=1, description="Request timeout")
    stream: bool = Field(False, description="Disable streaming to avoid gateway timeouts")


class LLMInterface(ABC):
    """Unified abstract interface for all LLM interactions."""

    def __init__(self, config: LLMConfig) -> None:
        self.config = config

    @abstractmethod
    async def generate(self, prompt: str, **kwargs: str) -> Tuple[str, float]:
        """
        Single-turn generation.

        Args:
            prompt: The input prompt
            **kwargs: Additional generation parameters

        Returns:
            Tuple of (response text, latency in milliseconds)
        """
        pass

    @abstractmethod
    async def generate_with_history(
        self,
        messages: List[Dict[str, str]]
    ) -> str:
        """
        Multi-turn generation with conversation history.

        Args:
            messages: List of message dicts with 'role' and 'content'

        Returns:
            Generated text response
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Perform health check by making a test API call."""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Quick synchronous check if basic configuration is present."""
        pass


class OpenAIModel(LLMInterface):
    """OpenAI-compatible model implementation for all red teaming roles.
    
    Supports OpenAI API and any OpenAI-compatible endpoint (e.g., vLLM).
    Works for target models, attacker models, and judge models.
    """

    def __init__(self, config: LLMConfig) -> None:
        super().__init__(config)
        try:
            from openai import AsyncOpenAI
        except ImportError:
            raise ImportError(
                "OpenAI is not installed. Install with: pip install 'trustyai-service[redteam]'"
            )

        # Support custom endpoints (e.g., vLLM servers)
        client_kwargs = {"api_key": config.api_key or "dummy-key"}
        if config.endpoint:
            client_kwargs["base_url"] = config.endpoint
        
        self.client = AsyncOpenAI(**client_kwargs)

    async def _call_api_with_streaming(
        self,
        api_kwargs: Dict,
        start_time: float
    ) -> Tuple[str, float]:
        """
        Unified API call handler with streaming support.
        
        Args:
            api_kwargs: Arguments for the API call
            start_time: Start time for latency calculation
            
        Returns:
            Tuple of (response_text, latency_ms)
        """
        if self.config.stream:
            api_kwargs["stream"] = True
            logger.debug(f"Starting streaming generation for {self.config.model_name}...")
            stream = await self.client.chat.completions.create(**api_kwargs)

            # Concatenate streamed chunks
            response_text = ""
            chunk_count = 0
            async for chunk in stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    response_text += chunk.choices[0].delta.content
                    chunk_count += 1

            latency_ms = (time.time() - start_time) * 1000
            logger.debug(f"Generated streaming response in {latency_ms:.2f}ms ({chunk_count} chunks)")
        else:
            # Non-streaming mode
            response = await self.client.chat.completions.create(**api_kwargs)
            response_text = response.choices[0].message.content or ""
            latency_ms = (time.time() - start_time) * 1000
            logger.debug(f"Generated response in {latency_ms:.2f}ms")
        
        return response_text, latency_ms

    async def generate(self, prompt: str, **kwargs: str) -> Tuple[str, float]:
        """Single-turn generation with optional streaming."""
        start_time = time.time()

        try:
            # Prepare kwargs
            api_kwargs = {
                "model": self.config.model_name,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": kwargs.get("temperature", self.config.temperature),
                "timeout": self.config.timeout_seconds,
            }
            
            # Add max_tokens if specified
            max_tokens = kwargs.get("max_tokens", self.config.max_tokens)
            if max_tokens:
                api_kwargs["max_tokens"] = max_tokens
            
            # Use unified API call with streaming support
            return await self._call_api_with_streaming(api_kwargs, start_time)

        except Exception as e:
            logger.error(f"Error generating response: {e}")
            raise

    async def generate_with_history(
        self,
        messages: List[Dict[str, str]]
    ) -> str:
        """Multi-turn generation with conversation history."""
        start_time = time.time()

        try:
            # Prepare kwargs
            api_kwargs = {
                "model": self.config.model_name,
                "messages": messages,
                "temperature": self.config.temperature,
                "timeout": self.config.timeout_seconds,
            }
            
            # Add max_tokens if specified
            if self.config.max_tokens:
                api_kwargs["max_tokens"] = self.config.max_tokens
            
            # Use unified API call with streaming support
            response_text, _ = await self._call_api_with_streaming(api_kwargs, start_time)
            return response_text

        except Exception as e:
            logger.error(f"Error generating response with history: {e}")
            raise

    async def health_check(self) -> bool:
        """Perform health check by making a test API call."""
        try:
            # Make a simple test call to verify the API is working
            test_response = await self.client.chat.completions.create(
                model=self.config.model_name,
                messages=[{"role": "user", "content": "Hello"}],
                max_tokens=5,
                timeout=10  # Short timeout for health check
            )
            
            # If we got a response, the API is working
            is_healthy = test_response.choices[0].message.content is not None
            logger.info(f"Health check for {self.config.model_name}: {'✓' if is_healthy else '✗'}")
            return is_healthy
            
        except Exception as e:
            logger.error(f"Health check failed for {self.config.model_name}: {e}")
            return False
    
    def is_available(self) -> bool:
        """Quick synchronous check if basic configuration is present."""
        try:
            return self.config.api_key is not None and len(self.config.api_key) > 0
        except Exception as e:
            logger.error(f"Error checking availability: {e}")
            return False


def get_llm(config: LLMConfig) -> LLMInterface:
    """
    Factory function to create appropriate LLM instance.

    Args:
        config: LLM configuration

    Returns:
        LLM instance

    Raises:
        ValueError: If model_type is not supported
    """
    model_type = config.model_type.lower()

    if model_type == "openai":
        return OpenAIModel(config)
    else:
        raise ValueError(
            f"Unsupported model type: {model_type}. "
            f"Only 'openai' is supported (includes vLLM and other OpenAI-compatible endpoints)"
        )

