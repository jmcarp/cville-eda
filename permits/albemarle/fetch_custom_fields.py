#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx",
# ]
# ///
"""Fetch custom field data from the EnerGov self-service API.

Plans created in 2025+ have structured custom fields (e.g. "Proposed Number of
Dwelling Units" for ZMAs) that aren't exposed in the Socrata CSV export.  This
script fetches those fields via the public EnerGov API and caches the results.

Two API calls per plan:
  1. GET  /api/energov/plans/{planId}             → LayoutId, OnlineLayoutId
  2. POST /api/energov/customfields/data/          → field values

Cached plans are never re-fetched (custom fields don't change after submission).
"""

import argparse
import json
import time
from pathlib import Path

import httpx

BASE_DIR = Path(__file__).parent

API_BASE = (
    "https://albemarlecountyva-energovweb.tylerhost.net"
    "/apps/selfservice/api/energov"
)

HEADERS = {
    "accept": "application/json",
    "tenantid": "1",
    "tenantname": "EnerGovProd",
    "tyler-tenanturl": "Home",
    "tyler-tenant-culture": "en-US",
}


def load_plans(path: Path, min_year: int) -> list[dict]:
    """Load plans from JSONL, filtered to min_year+."""
    import re

    plans = []
    with open(path) as f:
        for line in f:
            plan = json.loads(line)
            pn = plan["plan_number"]
            # Extract year from plan number
            m = re.match(r"[A-Z]+-(\d{4})-", pn)
            if not m:
                m = re.match(r"[A-Z]+(20\d{2})\d+", pn)
            if m and int(m.group(1)) >= min_year:
                plans.append(plan)
    return plans


def load_cache(path: Path) -> dict:
    """Load existing cache or return empty dict."""
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def save_cache(cache: dict, path: Path) -> None:
    """Write cache atomically."""
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(cache, f, indent=2, default=str)
    tmp.rename(path)


def fetch_plan_detail(client: httpx.Client, plan_id: str) -> dict | None:
    """Fetch plan detail to get LayoutId and OnlineLayoutId."""
    url = f"{API_BASE}/plans/{plan_id}"
    resp = client.get(url, headers=HEADERS)
    if resp.status_code != 200:
        return None
    data = resp.json()
    result = data.get("Result")
    if not result:
        return None
    return result


def fetch_custom_fields(
    client: httpx.Client,
    plan_id: str,
    layout_id: str,
    online_layout_id: str,
) -> dict | None:
    """Fetch custom field values for a plan."""
    url = f"{API_BASE}/customfields/data/"
    body = {
        "EntityId": plan_id,
        "LayoutId": layout_id,
        "OnlineLayoutId": online_layout_id,
    }
    resp = client.post(
        url,
        headers={**HEADERS, "Content-Type": "application/json"},
        json=body,
    )
    if resp.status_code != 200:
        return None
    data = resp.json()
    if not data.get("Success") or not data.get("Result"):
        return None

    # Flatten custom fields into a simple dict
    fields = {}
    for group in data["Result"].get("CustomGroups", []):
        group_label = group.get("Label", "")
        for field in group.get("CustomFields", []):
            name = field.get("FieldName", "")
            if not name:
                continue
            value = field.get("Value")
            if value == "" or value is None:
                continue
            # Skip unsupported type markers
            if value == "Custom field type is not supported.":
                continue
            fields[name] = {
                "value": value,
                "label": field.get("Label", ""),
                "group": group_label,
            }
    return fields


def fetch_one(client: httpx.Client, plan: dict) -> dict | None:
    """Fetch all EnerGov data for a single plan. Returns cache entry or None."""
    plan_id = plan["plan_id"]

    detail = fetch_plan_detail(client, plan_id)
    if not detail:
        return None

    layout_id = detail.get("LayoutId")
    online_layout_id = detail.get("OnlineLayoutId")

    custom_fields = {}
    if layout_id and online_layout_id:
        custom_fields = (
            fetch_custom_fields(client, plan_id, layout_id, online_layout_id)
            or {}
        )

    return {
        "plan_number": plan["plan_number"],
        "plan_type": plan.get("plan_type", ""),
        "plan_status": detail.get("PlanStatus", ""),
        "layout_id": layout_id,
        "online_layout_id": online_layout_id,
        "custom_fields": custom_fields,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Fetch EnerGov custom fields for Albemarle plans"
    )
    parser.add_argument(
        "--plans",
        type=Path,
        default=BASE_DIR / "plans.jsonl",
        help="Input plans JSONL file",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=BASE_DIR / "custom_fields.json",
        help="Cache file path",
    )
    parser.add_argument(
        "--min-year",
        type=int,
        default=2025,
        help="Minimum plan year to fetch (default: 2025)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.15,
        help="Delay between API calls in seconds (default: 0.15)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max plans to fetch (0 = unlimited)",
    )
    args = parser.parse_args()

    plans = load_plans(args.plans, args.min_year)
    print(f"Found {len(plans)} plans from {args.min_year}+")

    cache = load_cache(args.cache)
    print(f"Cache has {len(cache)} entries")

    to_fetch = [p for p in plans if p["plan_id"] not in cache]
    if args.limit:
        to_fetch = to_fetch[: args.limit]
    print(f"Need to fetch {len(to_fetch)} plans")

    if not to_fetch:
        print("Nothing to do.")
        return

    fetched = 0
    errors = 0
    with_fields = 0

    with httpx.Client(timeout=30) as client:
        for i, plan in enumerate(to_fetch):
            pn = plan["plan_number"]
            print(
                f"\r  [{i + 1}/{len(to_fetch)}] {pn}...",
                end="",
                flush=True,
            )

            try:
                entry = fetch_one(client, plan)
            except httpx.HTTPError as e:
                print(f" error: {e}")
                errors += 1
                time.sleep(1)
                continue

            if entry is None:
                # API returned error — cache as empty so we don't retry
                cache[plan["plan_id"]] = {
                    "plan_number": pn,
                    "plan_type": plan.get("plan_type", ""),
                    "plan_status": plan.get("plan_status", ""),
                    "error": True,
                    "custom_fields": {},
                }
                errors += 1
            else:
                cache[plan["plan_id"]] = entry
                fetched += 1
                if entry["custom_fields"]:
                    with_fields += 1

            # Save cache periodically
            if (i + 1) % 50 == 0:
                save_cache(cache, args.cache)

            time.sleep(args.delay)

    print()
    save_cache(cache, args.cache)
    print(f"Done: {fetched} fetched, {with_fields} with custom fields, {errors} errors")
    print(f"Cache now has {len(cache)} entries")

    # Summary of custom fields found
    field_counts: dict[str, int] = {}
    for entry in cache.values():
        for name in entry.get("custom_fields", {}):
            field_counts[name] = field_counts.get(name, 0) + 1
    if field_counts:
        print("\nCustom field frequency:")
        for name, count in sorted(field_counts.items(), key=lambda x: -x[1])[:20]:
            print(f"  {name}: {count}")


if __name__ == "__main__":
    main()
