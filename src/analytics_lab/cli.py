import argparse
import json
import os
from dataclasses import asdict
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path

from analytics_lab.contracts import Batch
from analytics_lab.db import migrate
from analytics_lab.pipeline import load, report


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic ads + CRM → reconciled daily metrics")
    parser.add_argument("command", choices=["migrate", "demo", "load", "report"])
    parser.add_argument("--file", type=Path)
    args = parser.parse_args()
    dsn = os.environ["DATABASE_URL"]
    migrate(dsn)
    if args.command == "migrate":
        print("Schema is current.")
    elif args.command == "demo":
        # Fixed fixture time keeps the example reproducible. It is not live business data.
        now = datetime(2026, 10, 20, 12, tzinfo=UTC)
        fixtures = files("analytics_lab").joinpath("fixtures")
        for source in ("google_ads", "meta_ads", "crm"):
            batch = Batch.model_validate_json(fixtures.joinpath(source + ".json").read_text())
            print(source, asdict(load(dsn, batch, now=now)))
        print(json.dumps(report(dsn, now=now), indent=2, default=str))
    elif args.command == "load":
        if not args.file:
            parser.error("--file is required for load")
        batch = Batch.model_validate_json(args.file.read_text())
        print(asdict(load(dsn, batch, now=datetime.now(UTC))))
    else:
        print(json.dumps(report(dsn, now=datetime.now(UTC)), indent=2, default=str))


if __name__ == "__main__":
    main()
