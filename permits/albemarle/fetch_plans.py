#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx",
#     "pydantic",
# ]
# ///
"""Fetch Albemarle County plan data from Socrata CSV export."""

import csv
import json
import sys
from collections import Counter
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path

import httpx

from models import AlbemarlePlan

SOCRATA_URL = (
    "https://albemarlecounty-va-cc.connect.socrata.com/api/download_dataset.json"
    "?dataset_domain=albemarlecounty.data.socrata.com"
    "&dataset_id=rm84-ftmr"
    "&dataset_name=Plans"
)

BASE_PATH = Path(__file__).parent
PLANS_CSV = BASE_PATH / "plans.csv"
PLANS_JSONL = BASE_PATH / "plans.jsonl"
CHANGELOG_JSONL = BASE_PATH / "changelog.jsonl"


def fetch_csv() -> str:
    """Download the Socrata CSV and save to plans.csv."""
    print(f"Downloading plans from Socrata...")
    response = httpx.get(SOCRATA_URL, follow_redirects=True, timeout=120)
    response.raise_for_status()
    PLANS_CSV.write_text(response.text, encoding="utf-8")
    print(f"Saved CSV to {PLANS_CSV}")
    return response.text


def parse_csv(csv_text: str) -> dict[str, AlbemarlePlan]:
    """Parse CSV text into AlbemarlePlan models keyed by plan_id."""
    reader = csv.DictReader(StringIO(csv_text))
    plans: dict[str, AlbemarlePlan] = {}
    errors = 0
    for row in reader:
        try:
            plan = AlbemarlePlan.from_csv_row(row)
            if plan.plan_id:
                plans[plan.plan_id] = plan
        except Exception as e:
            errors += 1
            plan_num = row.get("plannumber", "?")
            print(f"  Warning: failed to parse {plan_num}: {e}", file=sys.stderr)
    if errors:
        print(f"  {errors} rows failed to parse", file=sys.stderr)
    return plans


def load_existing() -> dict[str, AlbemarlePlan]:
    """Load existing plans.jsonl if it exists."""
    if not PLANS_JSONL.exists():
        return {}
    plans: dict[str, AlbemarlePlan] = {}
    with open(PLANS_JSONL) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            plan = AlbemarlePlan.model_validate_json(line)
            plans[plan.plan_id] = plan
    return plans


def merge_plans(
    old: dict[str, AlbemarlePlan], new: dict[str, AlbemarlePlan], today: date
) -> tuple[dict[str, AlbemarlePlan], dict]:
    """Merge new plans into old, tracking changes.

    Returns (merged_plans, changelog_entry).
    """
    merged = dict(old)  # Start with all existing plans

    new_plans = []
    status_changes = []
    removed_plan_ids = []

    for plan_id, plan in new.items():
        if plan_id in merged:
            existing = merged[plan_id]
            # Track status changes
            if existing.plan_status != plan.plan_status:
                status_changes.append(
                    {
                        "plan_number": plan.plan_number,
                        "old_status": existing.plan_status,
                        "new_status": plan.plan_status,
                    }
                )
            # Update all fields from CSV, preserve first_seen
            plan.first_seen = existing.first_seen
            plan.last_seen = today
            merged[plan_id] = plan
        else:
            # New record
            plan.first_seen = today
            plan.last_seen = today
            merged[plan_id] = plan
            new_plans.append(plan.plan_number)

    # Identify plans no longer in the CSV (stale); keep them, don't update last_seen
    for plan_id in old:
        if plan_id not in new:
            removed_plan_ids.append(old[plan_id].plan_number)

    changelog_entry = {
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "new_plans": new_plans,
        "status_changes": status_changes,
        "removed_plans": removed_plan_ids,
        "total_count": len(merged),
    }

    return merged, changelog_entry


def write_plans(plans: dict[str, AlbemarlePlan]) -> None:
    """Write merged plans to plans.jsonl."""
    with open(PLANS_JSONL, "w") as f:
        for plan in plans.values():
            f.write(plan.model_dump_json() + "\n")
    print(f"Wrote {len(plans)} plans to {PLANS_JSONL}")


def write_changelog(entry: dict) -> None:
    """Append changelog entry to changelog.jsonl."""
    with open(CHANGELOG_JSONL, "a") as f:
        f.write(json.dumps(entry) + "\n")


def print_summary(plans: dict[str, AlbemarlePlan], changelog: dict) -> None:
    """Print human-readable summary to stdout."""
    print()
    print("=" * 60)
    print(f"Fetch complete: {changelog['fetched_at']}")
    print(f"Total plans: {changelog['total_count']}")
    print()

    if changelog["new_plans"]:
        print(f"New plans ({len(changelog['new_plans'])}):")
        for pn in changelog["new_plans"][:20]:
            print(f"  + {pn}")
        if len(changelog["new_plans"]) > 20:
            print(f"  ... and {len(changelog['new_plans']) - 20} more")
        print()

    if changelog["status_changes"]:
        print(f"Status changes ({len(changelog['status_changes'])}):")
        for sc in changelog["status_changes"][:20]:
            print(f"  {sc['plan_number']}: {sc['old_status']} -> {sc['new_status']}")
        if len(changelog["status_changes"]) > 20:
            print(f"  ... and {len(changelog['status_changes']) - 20} more")
        print()

    if changelog["removed_plans"]:
        print(f"No longer in CSV ({len(changelog['removed_plans'])}):")
        for pn in changelog["removed_plans"][:10]:
            print(f"  - {pn}")
        if len(changelog["removed_plans"]) > 10:
            print(f"  ... and {len(changelog['removed_plans']) - 10} more")
        print()

    # Breakdown by plan type
    type_counts = Counter(p.plan_type for p in plans.values())
    print("By plan type:")
    for ptype, count in type_counts.most_common():
        print(f"  {ptype}: {count}")
    print("=" * 60)


def main() -> int:
    csv_text = fetch_csv()
    new_plans = parse_csv(csv_text)
    print(f"Parsed {len(new_plans)} plans from CSV")

    old_plans = load_existing()
    if old_plans:
        print(f"Loaded {len(old_plans)} existing plans from {PLANS_JSONL}")

    today = date.today()
    merged, changelog = merge_plans(old_plans, new_plans, today)

    write_plans(merged)
    write_changelog(changelog)
    print_summary(merged, changelog)

    return 0


if __name__ == "__main__":
    sys.exit(main())
