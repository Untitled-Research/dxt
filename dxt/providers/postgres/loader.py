"""PostgreSQL loader implementation.

This module provides data loading into PostgreSQL databases.
"""

from __future__ import annotations

from typing import Any, Optional

from dxt.providers.base.relational import RelationalConnector, RelationalLoader


class PostgresLoader(RelationalLoader):
    """PostgreSQL-specific loader.

    Extends RelationalLoader with PostgreSQL-specific features:
    - Native ON CONFLICT upsert syntax
    - TRUNCATE support for replace mode

    For bulk loading with COPY command, use PostgresCopyLoader.

    Example:
        >>> from dxt.providers.postgres import PostgresConnector, PostgresLoader
        >>>
        >>> with PostgresConnector(config) as conn:
        ...     loader = PostgresLoader(conn)
        ...     result = loader.load(stream, buffer)
    """

    def _build_upsert_sql(
        self, table_name: str, fields: list, upsert_keys: list[str]
    ) -> str:
        """Build PostgreSQL upsert SQL using ON CONFLICT.

        Args:
            table_name: Target table name
            fields: Field definitions
            upsert_keys: Keys for conflict detection

        Returns:
            PostgreSQL INSERT ... ON CONFLICT statement
        """
        field_names = [f.target_name for f in fields]
        columns = ", ".join(field_names)
        placeholders = ", ".join([f":{f.id}" for f in fields])

        conflict_cols = ", ".join(upsert_keys)

        update_fields = [f for f in fields if f.target_name not in upsert_keys]
        if update_fields:
            update_set = ", ".join(
                [f"{f.target_name} = EXCLUDED.{f.target_name}" for f in update_fields]
            )
            conflict_action = f"DO UPDATE SET {update_set}"
        else:
            conflict_action = "DO NOTHING"

        return f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols})
        {conflict_action}
        """
