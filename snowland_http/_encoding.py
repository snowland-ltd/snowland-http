"""Small helpers for resolving a response's text encoding.

The encoding must follow the HTTP rules, not a hard-coded UTF-8 assumption:
the ``charset`` parameter of the ``Content-Type`` header wins, and when it is
absent the default is ISO-8859-1 (latin-1) per RFC 7231.
"""

from typing import Dict, Optional


def parse_charset(headers: Dict[str, str]) -> Optional[str]:
    """Return the ``charset`` declared in the ``Content-Type`` header, or None.

    Header names are case-insensitive; values may carry parameters like
    ``text/html; charset=utf-8``. Returns the charset lower-cased, or None when
    no charset is present.
    """
    content_type = headers.get("Content-Type") or headers.get("content-type")
    if not content_type:
        return None
    # Split off parameters after the first ';', then look for charset=...
    parts = content_type.split(";")
    for part in parts[1:]:
        pair = part.split("=", 1)
        if len(pair) != 2:
            continue
        name, value = pair[0].strip().lower(), pair[1].strip()
        if name != "charset":
            continue
        # Strip surrounding quotes if present.
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            value = value[1:-1]
        return value.lower() if value else None
    return None
