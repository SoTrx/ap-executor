"""The provider abstraction magic_operator/app.py calls through -- swappable
so `MAGIC_OPERATOR_LLM_PROVIDER=mock` (the CI default) never needs a real
LLM SDK installed or a network call made."""
from typing import Protocol


class LLMProvider(Protocol):
    async def respond(self, prompt: str) -> str: ...
