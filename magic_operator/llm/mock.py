"""Deterministic, zero-network LLM stand-in used by CI and local dev by
default (`MAGIC_OPERATOR_LLM_PROVIDER=mock`).

Echoing the *rendered* prompt (not a fixed constant) is deliberate: it lets
the e2e test recompute the same string client-side from the actual
wired-through inputs, so a passing assertion proves the whole pipeline --
input resolution, cross-operator dataflow, prompt templating -- worked, not
just "some string came back".
"""

MOCK_PREFIX = "MOCK_RESPONSE::"


class MockLLMProvider:
    async def respond(self, prompt: str) -> str:
        return f"{MOCK_PREFIX}{prompt}"
