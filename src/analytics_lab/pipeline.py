"""Incremental loads, immutable source revisions and a reconciled reporting grain."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from analytics_lab.contracts import Batch, require_aware

LOOKBACK = timedelta(days=2)


class DataQualityError(Exception):
    pass


@dataclass(frozen=True)
class LoadResult:
    rows_read: int
    revisions_added: int
    facts_changed: int


def reconcile(conn) -> None:
    # One statement gives every total the same READ COMMITTED snapshot.
    totals = conn.execute(
        """SELECT coalesce(sum(spend_minor), 0), coalesce(sum(revenue_minor), 0),
                  coalesce(sum(orders), 0),
                  (SELECT coalesce(sum(amount_minor), 0) FROM ad_facts),
                  (SELECT coalesce(sum(amount_minor), 0) FROM sale_facts),
                  (SELECT count(*) FROM sale_facts) FROM daily_metrics"""
    ).fetchone()
    if totals[:3] != totals[3:]:
        raise DataQualityError("Reporting view does not reconcile with the fact tables")


def load(dsn: str, batch: Batch, *, now: datetime) -> LoadResult:
    require_aware(now)
    if batch.window_end > now:
        raise DataQualityError("Extraction window cannot extend into the future")
    # A source adapter owns pagination/rate limits. This boundary accepts only a complete batch.
    table = "sale_facts" if batch.source == "crm" else "ad_facts"
    with psycopg.connect(dsn) as conn:
        # Serialize a source, including its first run when there is no state row to lock.
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("etl:" + batch.source,))
        state = conn.execute(
            "SELECT watermark FROM pipeline_state WHERE source = %s FOR UPDATE", (batch.source,)
        ).fetchone()
        lower = state[0] - LOOKBACK if state else batch.window_start
        if state and batch.window_end < state[0]:
            raise DataQualityError("Watermark cannot move backwards")
        if batch.window_start > lower:
            raise DataQualityError("Batch does not cover the required lookback window")

        revisions = changed = read = 0
        # Ordered updates make concurrent multi-source loads less prone to lock inversion.
        for record in sorted(batch.records, key=lambda r: (r.record_id, r.updated_at)):
            if record.updated_at < lower:
                continue
            read += 1
            payload = record.model_dump(mode="json")
            inserted = conn.execute(
                """INSERT INTO raw_revisions VALUES (%s, %s, %s, %s)
                   ON CONFLICT DO NOTHING RETURNING record_id""",
                (batch.source, record.record_id, record.updated_at, Jsonb(payload)),
            ).fetchone()
            if not inserted:
                prior = conn.execute(
                    """SELECT payload FROM raw_revisions
                       WHERE source = %s AND record_id = %s AND source_updated_at = %s""",
                    (batch.source, record.record_id, record.updated_at),
                ).fetchone()[0]
                if prior != payload:
                    raise DataQualityError("Same source revision contains conflicting values")
            else:
                revisions += 1
            row = conn.execute(
                sql.SQL("""INSERT INTO {table}
                    (source, record_id, day, campaign_id, amount_minor, currency, source_updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, record_id) DO UPDATE SET
                      day = EXCLUDED.day, campaign_id = EXCLUDED.campaign_id,
                      amount_minor = EXCLUDED.amount_minor, currency = EXCLUDED.currency,
                      source_updated_at = EXCLUDED.source_updated_at
                    WHERE EXCLUDED.source_updated_at > {table}.source_updated_at
                    RETURNING record_id""").format(table=sql.Identifier(table)),
                (
                    batch.source,
                    record.record_id,
                    record.day,
                    record.campaign_id,
                    record.amount_minor,
                    record.currency,
                    record.updated_at,
                ),
            ).fetchone()
            changed += int(row is not None)

        reconcile(conn)
        conn.execute(
            """INSERT INTO pipeline_state VALUES (%s, %s, %s)
               ON CONFLICT (source) DO UPDATE SET watermark = EXCLUDED.watermark,
                                                  completed_at = EXCLUDED.completed_at""",
            (batch.source, batch.window_end, now),
        )
        conn.execute(
            "INSERT INTO pipeline_runs VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (uuid.uuid4(), batch.source, lower, batch.window_end, read, revisions, changed, now),
        )
    return LoadResult(read, revisions, changed)


def report(dsn: str, *, now: datetime) -> dict:
    require_aware(now)
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        metrics = conn.execute("SELECT * FROM daily_metrics ORDER BY day, campaign_id").fetchall()
        states = {r["source"]: r for r in conn.execute("SELECT * FROM pipeline_state")}
    freshness = {}
    for source in ("google_ads", "meta_ads", "crm"):
        state = states.get(source)
        lag = max(0, int((now - state["watermark"]).total_seconds())) if state else None
        freshness[source] = {
            "watermark": state["watermark"] if state else None,
            "lag_seconds": lag,
            "status": "missing" if state is None else ("fresh" if lag <= 3600 else "stale"),
        }
    return {"metrics": metrics, "freshness": freshness}
