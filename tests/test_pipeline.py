import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest
from pydantic import ValidationError

from analytics_lab.contracts import Batch, Record
from analytics_lab.pipeline import DataQualityError, load, reconcile, report

NOW = datetime(2026, 10, 20, 12, tzinfo=UTC)
START = NOW - timedelta(days=3)


def record(key="a1", amount=5000, updated=None, day=None, campaign="spring"):
    return Record(
        record_id=key,
        updated_at=updated or NOW - timedelta(hours=1),
        day=day or date(2026, 10, 19),
        campaign_id=campaign,
        amount_minor=amount,
        currency="PLN",
    )


def batch(source="google_ads", records=None, end=NOW, start=START):
    return Batch(
        source=source,
        window_start=start,
        window_end=end,
        complete=True,
        records=records if records is not None else [record()],
    )


def state(dsn):
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT source, watermark FROM pipeline_state ORDER BY source"
        ).fetchall()


def totals(dsn):
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT sum(spend_minor), sum(revenue_minor), sum(orders) FROM daily_metrics"
        ).fetchone()


def test_duplicate_load_does_not_duplicate_facts(dsn):
    first = load(dsn, batch(), now=NOW)
    second = load(dsn, batch(), now=NOW)
    assert (first.revisions_added, first.facts_changed) == (1, 1)
    assert (second.revisions_added, second.facts_changed) == (0, 0)
    assert totals(dsn) == (5000, 0, 0)


def test_same_instant_in_another_timezone_is_the_same_revision(dsn):
    load(dsn, batch(), now=NOW)
    data = record().model_dump(mode="json")
    data["updated_at"] = "2026-10-20T13:00:00+02:00"
    result = load(dsn, batch(records=[Record.model_validate(data)]), now=NOW)
    assert result.revisions_added == result.facts_changed == 0


def test_spend_without_conversions_reports_zero_roas(dsn):
    load(dsn, batch(), now=NOW)
    assert report(dsn, now=NOW)["metrics"][0]["roas"] == 0


def test_duplicate_rows_in_same_batch_are_idempotent(dsn):
    result = load(dsn, batch(records=[record(), record()]), now=NOW)
    assert result.rows_read == 2 and result.facts_changed == result.revisions_added == 1


def test_join_grain_prevents_spend_and_revenue_fanout(dsn):
    load(dsn, batch(records=[record("a1", 5000), record("a2", 3000)]), now=NOW)
    load(dsn, batch("meta_ads", [record("a1", 6000)]), now=NOW)
    load(
        dsn, batch("crm", [record("c1", 10000), record("c2", 20000), record("c3", 15000)]), now=NOW
    )
    assert totals(dsn) == (14000, 45000, 3)
    with psycopg.connect(dsn) as conn:
        reconcile(conn)
    assert str(report(dsn, now=NOW)["metrics"][0]["roas"]) == "3.2143"


def test_late_business_event_rebuilds_historical_day(dsn):
    load(dsn, batch("crm", []), now=NOW)
    late = record("c1", 10000, updated=NOW + timedelta(minutes=30), day=date(2026, 9, 1))
    load(dsn, batch("crm", [late], end=NOW + timedelta(hours=1)), now=NOW + timedelta(hours=1))
    result = report(dsn, now=NOW + timedelta(hours=1))["metrics"]
    assert result[0]["day"] == date(2026, 9, 1)
    assert result[0]["revenue_minor"] == 10000


def test_lookback_catches_delayed_delivery_and_preserves_source_revisions(dsn):
    load(dsn, batch(), now=NOW)
    corrected = record(amount=6500, updated=NOW - timedelta(minutes=10))
    result = load(
        dsn, batch(records=[corrected], end=NOW + timedelta(hours=1)), now=NOW + timedelta(hours=1)
    )
    assert result.facts_changed == 1
    assert totals(dsn)[0] == 6500
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT count(*) FROM raw_revisions").fetchone()[0] == 2


def test_older_revision_does_not_overwrite_newer_fact(dsn):
    load(dsn, batch(records=[record(updated=NOW)]), now=NOW)
    result = load(dsn, batch(records=[record(amount=1, updated=NOW - timedelta(hours=2))]), now=NOW)
    assert result.revisions_added == 1 and result.facts_changed == 0
    assert totals(dsn)[0] == 5000


def test_conflicting_revision_rolls_back_whole_batch_and_watermark(dsn):
    load(dsn, batch(), now=NOW)
    with pytest.raises(DataQualityError, match="conflicting"):
        load(
            dsn,
            batch(records=[record("a0"), record(amount=9999)], end=NOW + timedelta(hours=1)),
            now=NOW + timedelta(hours=1),
        )
    assert state(dsn) == [("google_ads", NOW)]
    assert totals(dsn)[0] == 5000
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT count(*) FROM raw_revisions").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM pipeline_runs").fetchone()[0] == 1


def test_fact_write_failure_does_not_advance_watermark(dsn):
    with psycopg.connect(dsn) as conn:
        conn.execute("ALTER TABLE ad_facts ADD CONSTRAINT simulate_failure CHECK (false)")
    with pytest.raises(psycopg.errors.CheckViolation):
        load(dsn, batch(), now=NOW)
    assert state(dsn) == []
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT count(*) FROM raw_revisions").fetchone()[0] == 0


def test_incomplete_and_malformed_batches_are_rejected_before_loading(dsn):
    data = batch().model_dump()
    data["complete"] = False
    with pytest.raises(ValidationError):
        Batch.model_validate(data)


@pytest.mark.parametrize(
    "field,value",
    [
        ("currency", "EUR"),
        ("amount_minor", -1),
        ("amount_minor", "100"),
        ("updated_at", "2026-10-20T10:00:00"),
    ],
)
def test_source_contract_rejects_ambiguous_data(field, value):
    data = record().model_dump()
    data[field] = value
    with pytest.raises(ValidationError):
        Record.model_validate(data)


def test_missing_lookback_window_is_rejected(dsn):
    load(dsn, batch(), now=NOW)
    with pytest.raises(DataQualityError, match="lookback"):
        load(dsn, batch(start=NOW - timedelta(hours=1)), now=NOW)


def test_watermark_never_moves_backwards(dsn):
    load(dsn, batch(), now=NOW)
    with pytest.raises(DataQualityError, match="backwards"):
        load(dsn, batch(end=NOW - timedelta(minutes=30)), now=NOW)


def test_future_window_is_rejected(dsn):
    with pytest.raises(DataQualityError, match="future"):
        load(dsn, batch(end=NOW + timedelta(hours=1)), now=NOW)


def test_concurrent_same_source_loads_are_serialized(dsn):
    barrier = threading.Barrier(4)

    def run(_):
        barrier.wait()
        return load(dsn, batch(), now=NOW)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(run, range(4)))
    assert sum(r.facts_changed for r in results) == 1
    assert totals(dsn)[0] == 5000


def test_missing_and_stale_sources_are_visible(dsn):
    load(dsn, batch(), now=NOW)
    freshness = report(dsn, now=NOW + timedelta(hours=2))["freshness"]
    assert freshness["google_ads"]["status"] == "stale"
    assert freshness["google_ads"]["lag_seconds"] == 7200
    assert freshness["meta_ads"]["status"] == freshness["crm"]["status"] == "missing"


def test_revenue_without_spend_has_no_invented_roas(dsn):
    load(dsn, batch("crm", [record("c1", 10000)]), now=NOW)
    assert report(dsn, now=NOW)["metrics"][0]["roas"] is None


def test_bad_reporting_view_is_detected_and_load_rolls_back(dsn):
    with psycopg.connect(dsn) as conn:
        conn.execute("DROP VIEW daily_metrics")
        conn.execute(
            """CREATE VIEW daily_metrics AS
               SELECT day, campaign_id, amount_minor * 2 AS spend_minor,
                      0 AS revenue_minor, 0 AS orders FROM ad_facts"""
        )
    with pytest.raises(DataQualityError, match="reconcile"):
        load(dsn, batch(), now=NOW)
    assert state(dsn) == []
