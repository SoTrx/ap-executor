"""Real LLM provider, mirroring ap-management's `LLM` class
(ap_management/internal/llm.py): a thin `litellm.acompletion(...)` wrapper.
Only imported when `MAGIC_OPERATOR_LLM_PROVIDER=litellm` is actually
selected (see `llm/factory.py`) -- `litellm` is an optional extra
(`pyproject.toml`'s `[project.optional-dependencies] llm`), not a plain
dependency, so the default mock-provider path never needs it installed.
"""
from typing import Optional

import litellm


class LiteLLMProvider:
    def __init__(
        self,
        model: str,
        api_base: Optional[str],
        api_key: Optional[str],
        timeout: float,
        *,
        ssl_verify: bool = True,
    ):
        self._model = model
        self._api_base = api_base
        self._timeout = timeout
        # Some LLM APIs use self-signed certificates (mirrors ap-management's LLM.__init__).
        litellm.ssl_verify = ssl_verify
        # Omitted entirely when unset -- litellm complains if api_key=None is passed explicitly.
        self._extra_kwargs = {"api_key": api_key} if api_key else {}

    async def respond(self, prompt: str) -> str:
        response = await litellm.acompletion(
            model=self._model,
            api_base=self._api_base,
            messages=[{"role": "user", "content": prompt}],
            timeout=self._timeout,
            **self._extra_kwargs,
        )
        return response.choices[0].message.content
