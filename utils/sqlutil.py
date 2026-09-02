"""Small helpers for building SQL text safely and consistently."""


def sql_literal(value: str) -> str:
    """Quote a string as a SQL literal (single quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


def ilike_pattern(user_text: str) -> str:
    """Turn user filter text into an ILIKE pattern.

    `*` is accepted as a user-friendly wildcard alias for `%`, and the
    pattern matches anywhere in the value.
    """
    return f"%{user_text.replace('*', '%')}%"
