"""Helpers for configuring PostgreSQL tablespaces in generated DDL."""

import re


_UNQUOTED_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def get_tablespace(config, object_type):
    """Return the configured tablespace for a table or index, if any.

    ``tablespaces`` is intentionally optional so existing configuration files
    continue to use PostgreSQL's default tablespace.
    """
    tablespaces = config.get("tablespaces")
    if tablespaces is None:
        return None
    if not isinstance(tablespaces, dict):
        raise ValueError("'tablespaces' must be a mapping with 'table' and/or 'index' keys")

    tablespace = tablespaces.get(object_type)
    if tablespace is not None and not isinstance(tablespace, str):
        raise ValueError(f"tablespaces.{object_type} must be a string or null")
    return tablespace


def tablespace_clause(tablespace):
    """Build a safe SQL ``TABLESPACE`` clause, or an empty string."""
    if tablespace is None:
        return ""
    if not tablespace:
        raise ValueError("tablespace names cannot be empty")

    # PostgreSQL identifiers cannot be query parameters. Preserve ordinary
    # names in the readable form users expect, and quote all other valid names.
    if _UNQUOTED_IDENTIFIER.fullmatch(tablespace):
        identifier = tablespace
    else:
        identifier = '"' + tablespace.replace('"', '""') + '"'
    return f" TABLESPACE {identifier}"
