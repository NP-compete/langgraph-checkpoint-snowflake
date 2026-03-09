"""Base class for Snowflake checkpoint savers with shared SQL and utilities."""

from __future__ import annotations

import json
import random
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    get_checkpoint_id,
)
from langgraph.checkpoint.serde.types import TASKS

# Snowflake-specific SQL migrations
# Each migration is run in order, tracked by checkpoint_migrations table
MIGRATIONS: list[str] = [
    # Migration 0: Create migrations tracking table
    """
    CREATE TABLE IF NOT EXISTS checkpoint_migrations (
        v INTEGER NOT NULL,
        PRIMARY KEY (v)
    )
    """,
    # Migration 1: Create checkpoints table
    """
    CREATE TABLE IF NOT EXISTS checkpoints (
        thread_id VARCHAR(256) NOT NULL,
        checkpoint_ns VARCHAR(256) NOT NULL DEFAULT '',
        checkpoint_id VARCHAR(256) NOT NULL,
        parent_checkpoint_id VARCHAR(256),
        type VARCHAR(64),
        checkpoint VARIANT NOT NULL,
        metadata VARIANT NOT NULL DEFAULT PARSE_JSON('{}'),
        PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
    )
    """,
    # Migration 2: Create checkpoint_blobs table for large binary data
    """
    CREATE TABLE IF NOT EXISTS checkpoint_blobs (
        thread_id VARCHAR(256) NOT NULL,
        checkpoint_ns VARCHAR(256) NOT NULL DEFAULT '',
        channel VARCHAR(256) NOT NULL,
        version VARCHAR(64) NOT NULL,
        type VARCHAR(64) NOT NULL,
        blob BINARY,
        PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
    )
    """,
    # Migration 3: Create checkpoint_writes table for pending writes
    """
    CREATE TABLE IF NOT EXISTS checkpoint_writes (
        thread_id VARCHAR(256) NOT NULL,
        checkpoint_ns VARCHAR(256) NOT NULL DEFAULT '',
        checkpoint_id VARCHAR(256) NOT NULL,
        task_id VARCHAR(256) NOT NULL,
        idx INTEGER NOT NULL,
        channel VARCHAR(256) NOT NULL,
        type VARCHAR(64),
        blob BINARY NOT NULL,
        task_path VARCHAR(1024) NOT NULL DEFAULT '',
        PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
    )
    """,
    # Migration 4: Allow NULL blob in checkpoint_blobs (for empty channels)
    """
    ALTER TABLE checkpoint_blobs ALTER COLUMN blob DROP NOT NULL
    """,
]

# SQL for selecting checkpoints with their blobs and pending writes
# Snowflake uses ARRAY_AGG and OBJECT_CONSTRUCT instead of PostgreSQL's array_agg
SELECT_SQL = """
SELECT
    c.thread_id,
    c.checkpoint,
    c.checkpoint_ns,
    c.checkpoint_id,
    c.parent_checkpoint_id,
    c.metadata,
    (
        SELECT ARRAY_AGG(ARRAY_CONSTRUCT(bl.channel, bl.type, bl.blob))
        FROM checkpoint_blobs bl
        WHERE bl.thread_id = c.thread_id
            AND bl.checkpoint_ns = c.checkpoint_ns
            AND bl.channel IN (SELECT key FROM TABLE(FLATTEN(input => c.checkpoint:channel_versions)))
            AND bl.version = c.checkpoint:channel_versions[bl.channel]::VARCHAR
    ) AS channel_values,
    (
        SELECT ARRAY_AGG(ARRAY_CONSTRUCT(cw.task_id, cw.channel, cw.type, cw.blob))
        FROM checkpoint_writes cw
        WHERE cw.thread_id = c.thread_id
            AND cw.checkpoint_ns = c.checkpoint_ns
            AND cw.checkpoint_id = c.checkpoint_id
        ORDER BY cw.task_id, cw.idx
    ) AS pending_writes
FROM checkpoints c
"""

# SQL for selecting pending sends for migration
SELECT_PENDING_SENDS_SQL = f"""
SELECT
    checkpoint_id,
    ARRAY_AGG(ARRAY_CONSTRUCT(type, blob) ORDER BY task_path, task_id, idx) AS sends
FROM checkpoint_writes
WHERE thread_id = %s
    AND checkpoint_id IN (SELECT VALUE FROM TABLE(FLATTEN(input => PARSE_JSON(%s))))
    AND channel = '{TASKS}'
GROUP BY checkpoint_id
"""

# Snowflake MERGE for upsert on checkpoint_blobs
UPSERT_CHECKPOINT_BLOBS_SQL = """
MERGE INTO checkpoint_blobs AS target
USING (SELECT
    %s AS thread_id,
    %s AS checkpoint_ns,
    %s AS channel,
    %s AS version,
    %s AS type,
    %s AS blob
) AS source
ON target.thread_id = source.thread_id
    AND target.checkpoint_ns = source.checkpoint_ns
    AND target.channel = source.channel
    AND target.version = source.version
WHEN NOT MATCHED THEN INSERT (thread_id, checkpoint_ns, channel, version, type, blob)
VALUES (source.thread_id, source.checkpoint_ns, source.channel, source.version, source.type, source.blob)
"""

# Snowflake MERGE for upsert on checkpoints
UPSERT_CHECKPOINTS_SQL = """
MERGE INTO checkpoints AS target
USING (SELECT
    %s AS thread_id,
    %s AS checkpoint_ns,
    %s AS checkpoint_id,
    %s AS parent_checkpoint_id,
    PARSE_JSON(%s) AS checkpoint,
    PARSE_JSON(%s) AS metadata
) AS source
ON target.thread_id = source.thread_id
    AND target.checkpoint_ns = source.checkpoint_ns
    AND target.checkpoint_id = source.checkpoint_id
WHEN MATCHED THEN UPDATE SET
    checkpoint = source.checkpoint,
    metadata = source.metadata
WHEN NOT MATCHED THEN INSERT (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
VALUES (source.thread_id, source.checkpoint_ns, source.checkpoint_id, source.parent_checkpoint_id, source.checkpoint, source.metadata)
"""

# Snowflake MERGE for upsert on checkpoint_writes (for special writes like errors)
UPSERT_CHECKPOINT_WRITES_SQL = """
MERGE INTO checkpoint_writes AS target
USING (SELECT
    %s AS thread_id,
    %s AS checkpoint_ns,
    %s AS checkpoint_id,
    %s AS task_id,
    %s AS task_path,
    %s AS idx,
    %s AS channel,
    %s AS type,
    %s AS blob
) AS source
ON target.thread_id = source.thread_id
    AND target.checkpoint_ns = source.checkpoint_ns
    AND target.checkpoint_id = source.checkpoint_id
    AND target.task_id = source.task_id
    AND target.idx = source.idx
WHEN MATCHED THEN UPDATE SET
    channel = source.channel,
    type = source.type,
    blob = source.blob
WHEN NOT MATCHED THEN INSERT (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
VALUES (source.thread_id, source.checkpoint_ns, source.checkpoint_id, source.task_id, source.task_path, source.idx, source.channel, source.type, source.blob)
"""

# Snowflake INSERT with ignore duplicate (for regular writes)
INSERT_CHECKPOINT_WRITES_SQL = """
INSERT INTO checkpoint_writes (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
WHERE NOT EXISTS (
    SELECT 1 FROM checkpoint_writes
    WHERE thread_id = %s
        AND checkpoint_ns = %s
        AND checkpoint_id = %s
        AND task_id = %s
        AND idx = %s
)
"""

# SQL for deleting old checkpoints (TTL/expiration)
DELETE_OLD_CHECKPOINTS_SQL = """
DELETE FROM checkpoints
WHERE thread_id IN (
    SELECT DISTINCT thread_id FROM checkpoints
    WHERE checkpoint_id < %s
)
AND checkpoint_id < %s
"""

DELETE_OLD_BLOBS_SQL = """
DELETE FROM checkpoint_blobs
WHERE thread_id IN (
    SELECT DISTINCT thread_id FROM checkpoints
    WHERE checkpoint_id < %s
)
AND (thread_id, checkpoint_ns, channel, version) NOT IN (
    SELECT thread_id, checkpoint_ns, key, checkpoint:channel_versions[key]::VARCHAR
    FROM checkpoints, TABLE(FLATTEN(input => checkpoint:channel_versions))
)
"""

DELETE_OLD_WRITES_SQL = """
DELETE FROM checkpoint_writes
WHERE (thread_id, checkpoint_ns, checkpoint_id) NOT IN (
    SELECT thread_id, checkpoint_ns, checkpoint_id FROM checkpoints
)
"""

# SQL for counting checkpoints (for metrics)
COUNT_CHECKPOINTS_SQL = """
SELECT COUNT(*) FROM checkpoints
"""

COUNT_CHECKPOINTS_BY_THREAD_SQL = """
SELECT thread_id, COUNT(*) as count
FROM checkpoints
GROUP BY thread_id
ORDER BY count DESC
LIMIT %s
"""


class BaseSnowflakeSaver(BaseCheckpointSaver[str]):
    """Base class for Snowflake checkpoint savers.

    Provides shared SQL constants, migrations, and helper methods for
    both sync and async implementations.
    """

    SELECT_SQL = SELECT_SQL
    SELECT_PENDING_SENDS_SQL = SELECT_PENDING_SENDS_SQL
    MIGRATIONS = MIGRATIONS
    UPSERT_CHECKPOINT_BLOBS_SQL = UPSERT_CHECKPOINT_BLOBS_SQL
    UPSERT_CHECKPOINTS_SQL = UPSERT_CHECKPOINTS_SQL
    UPSERT_CHECKPOINT_WRITES_SQL = UPSERT_CHECKPOINT_WRITES_SQL
    INSERT_CHECKPOINT_WRITES_SQL = INSERT_CHECKPOINT_WRITES_SQL
    DELETE_OLD_CHECKPOINTS_SQL = DELETE_OLD_CHECKPOINTS_SQL
    DELETE_OLD_BLOBS_SQL = DELETE_OLD_BLOBS_SQL
    DELETE_OLD_WRITES_SQL = DELETE_OLD_WRITES_SQL
    COUNT_CHECKPOINTS_SQL = COUNT_CHECKPOINTS_SQL
    COUNT_CHECKPOINTS_BY_THREAD_SQL = COUNT_CHECKPOINTS_BY_THREAD_SQL

    def _migrate_pending_sends(
        self,
        pending_sends: list[tuple[str, bytes]] | None,
        checkpoint: dict[str, Any],
        channel_values: list[tuple[str, str, bytes]],
    ) -> None:
        """Migrate pending sends from old checkpoint format."""
        if not pending_sends:
            return
        # Add to values
        enc, blob = self.serde.dumps_typed(
            [self.serde.loads_typed((c, b)) for c, b in pending_sends],
        )
        channel_values.append((TASKS, enc, blob))
        # Add to versions
        checkpoint["channel_versions"][TASKS] = (
            max(checkpoint["channel_versions"].values())
            if checkpoint["channel_versions"]
            else self.get_next_version(None, None)
        )

    def _load_blobs(
        self, blob_values: list[tuple[str, str, bytes]] | None
    ) -> dict[str, Any]:
        """Deserialize blob values from database format."""
        if not blob_values:
            return {}
        result = {}
        for item in blob_values:
            if item is None:
                continue
            k, t, v = item[0], item[1], item[2]
            if t != "empty":
                result[k] = self.serde.loads_typed((t, v))
        return result

    def _dump_blobs(
        self,
        thread_id: str,
        checkpoint_ns: str,
        values: dict[str, Any],
        versions: ChannelVersions,
    ) -> list[tuple[str, str, str, str, str, bytes | None]]:
        """Serialize blob values for database storage."""
        if not versions:
            return []

        result = []
        for k, ver in versions.items():
            if k in values:
                type_str, blob = self.serde.dumps_typed(values[k])
            else:
                type_str, blob = "empty", None
            result.append(
                (
                    thread_id,
                    checkpoint_ns,
                    k,
                    cast("str", ver),
                    type_str,
                    blob,
                )
            )
        return result

    def _load_writes(
        self, writes: list[tuple[str, str, str, bytes]] | None
    ) -> list[tuple[str, str, Any]]:
        """Deserialize pending writes from database format."""
        if not writes:
            return []
        result = []
        for item in writes:
            if item is None:
                continue
            tid, channel, t, v = item[0], item[1], item[2], item[3]
            result.append((tid, channel, self.serde.loads_typed((t, v))))
        return result

    def _dump_writes(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        task_path: str,
        writes: Sequence[tuple[str, Any]],
    ) -> list[tuple[str, str, str, str, str, int, str, str, bytes]]:
        """Serialize writes for database storage."""
        result = []
        for idx, (channel, value) in enumerate(writes):
            type_str, blob = self.serde.dumps_typed(value)
            result.append(
                (
                    thread_id,
                    checkpoint_ns,
                    checkpoint_id,
                    task_id,
                    task_path,
                    WRITES_IDX_MAP.get(channel, idx),
                    channel,
                    type_str,
                    blob,
                )
            )
        return result

    def get_next_version(self, current: str | None, channel: None) -> str:
        """Generate the next version ID for a channel.

        Uses a format of "{version_number}.{random_hash}" to ensure
        monotonically increasing versions with collision resistance.
        """
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"

    def _search_where(
        self,
        config: RunnableConfig | None,
        filter: dict[str, Any] | None,
        before: RunnableConfig | None = None,
    ) -> tuple[str, list[Any]]:
        """Build WHERE clause for checkpoint queries.

        Returns a tuple of (where_clause, parameters) for use in SQL queries.
        """
        wheres = []
        param_values: list[Any] = []

        # Filter by config
        if config:
            wheres.append("thread_id = %s")
            param_values.append(config["configurable"]["thread_id"])
            checkpoint_ns = config["configurable"].get("checkpoint_ns")
            if checkpoint_ns is not None:
                wheres.append("checkpoint_ns = %s")
                param_values.append(checkpoint_ns)

            if checkpoint_id := get_checkpoint_id(config):
                wheres.append("checkpoint_id = %s")
                param_values.append(checkpoint_id)

        # Filter by metadata
        if filter:
            # Snowflake uses OBJECT_CONTAINS or path notation for JSON filtering
            for key, value in filter.items():
                wheres.append(f"metadata:{key} = %s")
                param_values.append(
                    json.dumps(value) if not isinstance(value, str) else value
                )

        # Filter by before checkpoint
        if before is not None:
            wheres.append("checkpoint_id < %s")
            param_values.append(get_checkpoint_id(before))

        where_clause = "WHERE " + " AND ".join(wheres) if wheres else ""
        return where_clause, param_values

    @staticmethod
    def _get_cutoff_checkpoint_id(max_age: timedelta) -> str:
        """Generate a checkpoint ID cutoff based on max_age.

        Checkpoint IDs are UUIDs that are time-ordered (UUIDv7-like),
        so we can generate a cutoff based on timestamp.

        Args:
            max_age (timedelta): Maximum age of checkpoints to keep.

        Returns:
            A checkpoint ID string representing the cutoff point.
        """
        cutoff_time = datetime.now(timezone.utc) - max_age
        # Generate a UUID-like string based on timestamp
        # Format: timestamp_hex-random-random-random-random
        timestamp_ms = int(cutoff_time.timestamp() * 1000)
        timestamp_hex = f"{timestamp_ms:012x}"
        return f"{timestamp_hex[:8]}-{timestamp_hex[8:12]}-0000-0000-000000000000"


__all__ = ["MIGRATIONS", "BaseSnowflakeSaver"]
