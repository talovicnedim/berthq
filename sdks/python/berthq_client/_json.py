"""Between the API's camelCase JSON and the snake_case dataclasses in ``types``.

Unknown response fields are ignored, so a newer broker that adds fields doesn't break an older
client.
"""

import dataclasses
import functools
import json
from typing import Any, Dict, Tuple, Type, TypeVar, Union, get_args, get_origin, get_type_hints

from .errors import BerthqError

T = TypeVar("T")


def camel(name: str) -> str:
    """``visibility_timeout_ms`` -> ``visibilityTimeoutMs``."""
    first, *rest = name.split("_")
    return first + "".join(word.title() for word in rest)


def dumps(value: Any) -> bytes:
    """A request body as compact UTF-8. The broker counts a message body's size as it was sent,
    so escaping ``é`` as ``\\u00e9`` or adding spaces would only make it bigger."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def to_json(values: Dict[str, Any]) -> Dict[str, Any]:
    """snake_case kwargs to a camelCase request body, leaving out ``None`` so the broker's
    defaults apply."""
    return {camel(key): value for key, value in values.items() if value is not None}


def from_json(cls: Type[T], data: Dict[str, Any]) -> T:
    """Builds the dataclass ``cls`` from a camelCase JSON object, recursing into nested
    dataclasses and lists of them."""
    values = {}
    missing = []
    for name, key, hint, required in _plan(cls):
        if key in data:
            values[name] = _convert(hint, data[key])
        elif required:
            missing.append(key)
    if missing:
        # Most likely a broker older than this client.
        raise BerthqError(
            "http", None, f"unexpected response: {cls.__name__} without {', '.join(missing)}"
        )
    return cls(**values)


@functools.lru_cache(maxsize=None)
def _plan(cls: type) -> Tuple[Tuple[str, str, Any, bool], ...]:
    """Per field of the dataclass ``cls``: its name, its camelCase key, its type and whether
    it is required. Worked out once per class: type hints are slow to read."""
    hints = get_type_hints(cls)
    return tuple(
        (
            f.name,
            camel(f.name),
            hints[f.name],
            f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING,
        )
        for f in dataclasses.fields(cls)  # type: ignore[arg-type]
    )


def _convert(hint: Any, value: Any) -> Any:
    if value is None:
        return None
    origin = get_origin(hint)
    if origin is Union:  # Optional[X]
        inner = [arg for arg in get_args(hint) if arg is not type(None)]
        return _convert(inner[0], value)
    if origin is list:
        (item,) = get_args(hint)
        return [_convert(item, entry) for entry in value]
    if dataclasses.is_dataclass(hint):
        return from_json(hint, value)
    return value
