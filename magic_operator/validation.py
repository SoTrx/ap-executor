"""Input validation against the operator's own declared `InputSpec`s -- the
last line of defense if the executor ever forwarded something malformed."""
from typing import Any, Dict, List

from .config import InputSpec
from .errors import InputValidationFailed


def validate_inputs(payload: Dict[str, Any], specs: List[InputSpec]) -> Dict[str, Any]:
    """Resolve `payload` against declared `specs`, filling in defaults;
    raises `InputValidationFailed` listing every required-but-missing name."""
    resolved: Dict[str, Any] = {}
    missing: List[str] = []
    for spec in specs:
        if spec.name in payload:
            resolved[spec.name] = payload[spec.name]
        elif not spec.required:
            resolved[spec.name] = spec.default
        else:
            missing.append(spec.name)
    if missing:
        raise InputValidationFailed(missing)
    return resolved
