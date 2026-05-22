"""Configuration for LangGraph checkpointer."""

from typing import Literal

from pydantic import BaseModel, Field

CheckpointerType = Literal["memory", "sqlite", "postgres", "oceanbase"]


class CheckpointerConfig(BaseModel):
    """Configuration for LangGraph state persistence checkpointer."""

    type: CheckpointerType = Field(
        description="Checkpointer backend type. "
        "'memory' is in-process only (lost on restart). "
        "'sqlite' persists to a local file (requires langgraph-checkpoint-sqlite). "
        "'postgres' persists to PostgreSQL (install with deerflow-harness[postgres]). "
        "'oceanbase' persists to OceanBase via MySQL wire protocol "
        "(install with deerflow-harness[oceanbase]; async contexts only)."
    )
    connection_string: str | None = Field(
        default=None,
        description="Connection string for sqlite (file path) or postgres/oceanbase (DSN). "
        "Optional for sqlite and defaults to 'store.db' when omitted. "
        "Required for postgres and oceanbase. "
        "For sqlite, use a file path like '.deer-flow/checkpoints.db' or ':memory:' for in-memory. "
        "For postgres, use a DSN like 'postgresql://user:pass@localhost:5432/db'. "
        "For oceanbase, use 'mysql://user:pass@host:2881/db' (URL-encode the @ in user@tenant as %40).",
    )


# Global configuration instance — None means no checkpointer is configured.
_checkpointer_config: CheckpointerConfig | None = None


def get_checkpointer_config() -> CheckpointerConfig | None:
    """Get the current checkpointer configuration, or None if not configured."""
    return _checkpointer_config


def set_checkpointer_config(config: CheckpointerConfig | None) -> None:
    """Set the checkpointer configuration."""
    global _checkpointer_config
    _checkpointer_config = config


def load_checkpointer_config_from_dict(config_dict: dict | None) -> None:
    """Load checkpointer configuration from a dictionary."""
    global _checkpointer_config
    if config_dict is None:
        _checkpointer_config = None
        return
    _checkpointer_config = CheckpointerConfig(**config_dict)
