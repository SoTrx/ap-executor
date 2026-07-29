"""Builds the configured LLM provider. The `litellm` import is deferred into
the branch that actually needs it, so constructing the app with the default
`mock` provider never requires `litellm` to be installed."""
from ..config import MagicOperatorConfig
from .base import LLMProvider
from .mock import MockLLMProvider


def build_llm_provider(config: MagicOperatorConfig) -> LLMProvider:
    if config.llm_provider == "mock":
        return MockLLMProvider()

    from .litellm_provider import LiteLLMProvider

    return LiteLLMProvider(
        model=config.llm_model,
        api_base=config.llm_api_base,
        api_key=config.llm_api_key,
        timeout=config.llm_timeout_seconds,
        ssl_verify=config.llm_ssl_verify,
    )
