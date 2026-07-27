"""Prompt templating: plain `str.format`, deliberately not Jinja2 (no new
dependency) or an f-string/eval scheme (arbitrary code execution risk).
Placeholder names are exactly the operator's declared `MAGIC_OPERATOR_INPUTS`
names, so the template is self-documenting from the manifest alone.
"""
from typing import Any, Dict


def render_prompt(template: str, inputs: Dict[str, Any]) -> str:
    try:
        return template.format(**inputs)
    except (KeyError, IndexError) as exc:
        raise ValueError(f"prompt template references an unresolved input: {exc}") from exc
