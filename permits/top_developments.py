#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pydantic",
#     "pyyaml",
# ]
# ///
"""List top developments by unit count with project type, zoning, and timeline."""

import argparse
import csv
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from models import Permit
from permit_utils import load_permits, load_parcel_zones, find_related_permits

# Permit types/subtypes that count for zoning code determination
# (Site plans, development plans, rezonings - not minor permits like HVAC)
QUALIFYING_PERMIT_TYPES = {"Site Plan"}
QUALIFYING_PLANNING_SUBTYPES = {
    "Rezoning",
    "Development Plan Review - Major",
    "Development Plan Review - Minor",
    "Special Use",
    "Special Exception Permit",
    "Preliminary Subdivision Plat Review",
}

# Permit types/subtypes to include even without unit counts
INCLUDE_WITHOUT_UNITS_TYPES = {"Site Plan"}
INCLUDE_WITHOUT_UNITS_SUBTYPES = {
    "Development Plan Review - Major",
    "Development Plan Review - Minor",
}

# Zoning code cutoff dates
# Projects approved before this date proceed under 2003 code
APPROVAL_CUTOFF = datetime(2024, 2, 19)
# Projects submitted before this date proceed under 2003 code
SUBMISSION_CUTOFF = datetime(2023, 12, 18)


def parse_date(date_str: str | None) -> datetime | None:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except ValueError:
        return None


def get_project_type(permit: Permit) -> str:
    """Determine project type from permit details."""
    type_field = None
    has_commercial_sf = False
    description = ""

    for d in permit.details:
        if d.description == "Type":
            type_field = d.data.lower()
        if "commercial" in d.description.lower() and (
            "square" in d.description.lower() or "sf" in d.description.lower()
        ):
            if d.data and d.data.strip() and d.data.strip() != "0":
                has_commercial_sf = True
        if d.description == "Description of Work":
            description = d.data.lower()

    # Check for conversions first
    if "into an apartment" in description or "into apartment" in description:
        return "Conversion"
    if "adaptive reuse" in description and "apartment" in description:
        return "Conversion"

    # Check description for specific types
    if re.search(r"\d+\s*room\s*hotel", description) or re.search(
        r"hotel\s*with", description
    ):
        if has_commercial_sf or "retail" in description:
            return "Hotel+Retail"
        return "Hotel"

    if "affordable" in description:
        if (
            has_commercial_sf
            or "retail" in description
            or (type_field and "mixed" in type_field)
        ):
            return "Afford+Retail"
        return "Affordable"

    if "student" in description or "student housing" in description:
        return "Student Hsg"
    if (
        "senior" in description
        or "elderly" in description
        or "assisted living" in description
    ):
        return "Senior Hsg"
    if "townhouse" in description or "townhome" in description:
        return "Townhouse"
    if "condo" in description:
        return "Condo"

    # Use type field if available
    if type_field:
        if "residential" in type_field:
            if has_commercial_sf:
                return "Mixed-Use"
            return "Residential"
        elif "commercial" in type_field:
            return "Commercial"
        elif "mixed" in type_field:
            return "Mixed-Use"

    # Infer from description
    if "apartment" in description or "unit" in description or "dwelling" in description:
        if has_commercial_sf:
            return "Mixed-Use"
        return "Residential"
    if "retail" in description or "office" in description:
        return "Commercial"

    return "?"


def is_qualifying_permit(permit: Permit) -> bool:
    """Check if permit type counts for zoning code determination."""
    ptype = permit.search_result.permit_type
    subtype = permit.search_result.sub_type
    return ptype in QUALIFYING_PERMIT_TYPES or (
        ptype == "Planning" and subtype in QUALIFYING_PLANNING_SUBTYPES
    )


def get_earliest_payment_date(permit: Permit) -> datetime | None:
    """Get earliest payment date from permit."""
    earliest = None
    for payment in permit.payments:
        dt = parse_date(payment.payment_date)
        if dt and (earliest is None or dt < earliest):
            earliest = dt
    return earliest


def get_submission_date(permit: Permit) -> datetime | None:
    """Extract submission date using the earliest of all available signals.

    We use the earliest date since it's unclear which is authoritative.
    """
    candidates: list[datetime] = []

    # Intake Application task date
    for task in permit.tasks:
        if task.description == "Intake Application":
            dt = parse_date(task.date_completed)
            if dt:
                candidates.append(dt)

    # Earliest payment date
    payment_date = get_earliest_payment_date(permit)
    if payment_date:
        candidates.append(payment_date)

    # date_created
    dt = parse_date(permit.search_result.date_created)
    if dt:
        candidates.append(dt)

    # Permit year (Jan 1 of that year) - only if no other dates
    if not candidates and permit.permit_year:
        return datetime(permit.permit_year, 1, 1)

    return min(candidates) if candidates else None


def get_approval_date(permit: Permit) -> datetime | None:
    """Extract approval date from permit tasks."""
    for task in permit.tasks:
        if "Final Approval" in task.description and task.result in (
            "YES_APPR",
            "YES",
            "APPROVED",
            "APPRV_PC",
        ):
            return parse_date(task.date_completed)
    # Also check "Final Site Plan Approved?" task
    for task in permit.tasks:
        if "Final Site Plan Approved" in task.description and task.result in (
            "YES_APPR",
            "YES",
            "APPROVED",
        ):
            return parse_date(task.date_completed)
    return None


def get_zoning_code(
    related_permits: list[Permit],
) -> tuple[str, str | None, datetime | None]:
    """Determine which zoning code applies (2003 or 2023).

    Returns (code, reason, qualifying_date) tuple.

    2003 code if:
    - Approved before 2024-02-19, OR
    - Submitted before 2023-12-18

    Only considers qualifying permits (Site Plan, Development Plan, Rezoning, etc.)
    """
    earliest_submitted: datetime | None = None
    earliest_approved: datetime | None = None
    submitted_permit_id: str | None = None
    approved_permit_id: str | None = None

    for permit in related_permits:
        if not is_qualifying_permit(permit):
            continue

        # Track submission date (from tasks, not date_created which is often wrong)
        submitted = get_submission_date(permit)
        if submitted:
            if earliest_submitted is None or submitted < earliest_submitted:
                earliest_submitted = submitted
                submitted_permit_id = permit.permit_id

        # Track approval date
        approved = get_approval_date(permit)
        if approved:
            if earliest_approved is None or approved < earliest_approved:
                earliest_approved = approved
                approved_permit_id = permit.permit_id

    # Determine code - check submission first (simpler case)
    if earliest_submitted and earliest_submitted < SUBMISSION_CUTOFF:
        return (
            "2003",
            f"submitted {earliest_submitted.strftime('%Y-%m-%d')} ({submitted_permit_id})",
            earliest_submitted,
        )

    if earliest_approved and earliest_approved < APPROVAL_CUTOFF:
        return (
            "2003",
            f"approved {earliest_approved.strftime('%Y-%m-%d')} ({approved_permit_id})",
            earliest_approved,
        )

    if earliest_submitted:
        return (
            "2023",
            f"submitted {earliest_submitted.strftime('%Y-%m-%d')} ({submitted_permit_id})",
            earliest_submitted,
        )

    return "?", None, None


def should_include_without_units(permit: Permit) -> bool:
    """Check if permit type should be included even without unit counts."""
    ptype = permit.search_result.permit_type
    subtype = permit.search_result.sub_type
    return (
        ptype in INCLUDE_WITHOUT_UNITS_TYPES
        or subtype in INCLUDE_WITHOUT_UNITS_SUBTYPES
    )


def get_unit_count(permit: Permit) -> int | None:
    """Extract unit count from permit details."""
    for detail in permit.details:
        desc = detail.description.lower()
        if "residential units" in desc or (
            ("# of units" == desc or "number of units" in desc)
            and permit.search_result.permit_type in ("Site Plan", "Planning")
        ):
            try:
                val = int(re.sub(r"[^\d]", "", detail.data))
                if 0 < val < 1000:
                    return val
            except (ValueError, TypeError):
                pass
    return None


def find_developments(
    permits: dict[str, Permit],
    parcel_zones: dict[str, str],
    min_units: int | None = None,
    include_without_units: bool = False,
) -> list[dict[str, Any]]:
    """Find all developments with unit counts above threshold.

    If include_without_units is True, also includes Site Plans and
    Major Development Plans that don't have unit counts specified.
    """
    projects: list[dict[str, Any]] = []

    for p in permits.values():
        units = get_unit_count(p)

        # Decide whether to include this permit.
        #
        # Primary path: permits with residential unit counts. These come from
        # Development Plan Reviews and have reliable metadata.
        #
        # Secondary path (--include-without-units): Site Plans and Major
        # Development Plans that lack unit counts. Many older Site Plans have
        # sparse metadata, so we require a known project type to filter out
        # entries that are too incomplete to be useful.
        if units:
            if min_units is not None and units < min_units:
                continue
        elif include_without_units and should_include_without_units(p):
            if get_project_type(p) == "?":
                continue  # Skip permits without enough metadata to determine type
        else:
            continue

        related_ids = find_related_permits([p.permit_id], permits)
        related = [permits[pid] for pid in related_ids if pid in permits]

        addrs: set[str] = set()
        parcels: set[str] = set()
        submit_dates: list[datetime] = []
        update_dates: list[datetime] = []

        for rp in related:
            for a in rp.site_addresses:
                addrs.add(a.address)
            if rp.search_result.parcel_number:
                parcels.add(rp.search_result.parcel_number)
            # Get submission date using our improved logic
            sub_dt = get_submission_date(rp)
            if sub_dt:
                submit_dates.append(sub_dt)
            # For last updated, use most recent task, payment, or intake date
            for task in rp.tasks:
                dt = parse_date(task.date_completed)
                if dt:
                    update_dates.append(dt)
            for payment in rp.payments:
                dt = parse_date(payment.payment_date)
                if dt:
                    update_dates.append(dt)

        zone = "?"
        for parcel in parcels:
            if parcel in parcel_zones:
                zone = parcel_zones[parcel]
                break

        zoning_code, code_reason, qualifying_date = get_zoning_code(related)

        # Use primary permit's address first, then others
        primary_addr = p.site_addresses[0].address if p.site_addresses else None
        other_addrs = sorted(a for a in addrs if a != primary_addr)
        all_addrs = ([primary_addr] if primary_addr else []) + other_addrs

        projects.append(
            {
                "units": units,
                "permit_id": p.permit_id,
                "project_number": p.project_number,
                "use_type": get_project_type(p),
                "status": p.search_result.status,
                "addresses": all_addrs,
                "parcels": sorted(parcels) if parcels else [],
                "zone": zone,
                "zoning_code": zoning_code,
                "code_reason": code_reason,
                "qualifying_date": qualifying_date,
                "initial_submit": min(submit_dates) if submit_dates else None,
                "last_updated": max(update_dates) if update_dates else None,
                "permit_count": len(related),
            }
        )

    # Deduplicate by parcel number (preferred) or primary address
    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for proj in projects:
        # Use first parcel if available, otherwise address
        if proj.get("parcels"):
            key = proj["parcels"][0]
        elif proj["addresses"]:
            key = proj["addresses"][0]
        else:
            key = proj["permit_id"]
        by_key[key].append(proj)

    # Status priority for deduplication (higher = better)
    status_priority = {
        "APPROVED": 100,
        "APPROVEDCL": 100,
        "UNDERCONST": 90,
        "PLANCOMM": 80,
        "REVIEW": 70,
        "RESUBMIT": 60,
        "COMMENTS": 50,
        "APPLIED": 40,
        "DEFERRED": 30,
        "EXPIRED": 20,
        "REJECTED": 10,
        "WITHDRAWN": 10,
        "DENIED": 10,
        "CLOSED": 10,
        "VOID": 0,
    }

    deduped = []
    for projs in by_key.values():
        # Prefer non-VOID permits, then best status, then highest unit count
        active = [p for p in projs if p["status"] != "VOID"]
        candidates = active if active else projs
        best = max(
            candidates,
            key=lambda x: (
                status_priority.get(x["status"], 25),
                x["units"] or 0,
            ),
        )
        deduped.append(best)

    # Sort by units descending (None sorts last)
    deduped.sort(key=lambda x: -(x["units"] or 0))

    # Filter out projects without a complete address or parcel number
    def has_complete_address(addr: str) -> bool:
        """Check if address starts with a number (e.g., '123 MAIN ST' not 'MAIN ST')."""
        return bool(addr and addr[0].isdigit())

    filtered = []
    for proj in deduped:
        has_parcel = bool(proj.get("parcels"))
        has_addr = any(has_complete_address(a) for a in proj.get("addresses", []))
        if has_parcel or has_addr:
            filtered.append(proj)

    return filtered


def load_overrides(overrides_path: Path) -> dict[str, Any]:
    """Load project overrides from YAML file.

    Expected format:
    ```yaml
    omit:
      - permit_id: "12345.00"
        reason: "Duplicate of another project"
      - address: "123 MAIN ST"
        reason: "Project cancelled"

    revise:
      - permit_id: "12345.00"
        units: 150
        use_type: "Mixed-Use"
      - address: "456 OAK AVE"
        units: 200

    add:
      - address: "789 ELM ST"
        units: 100
        use_type: "Residential"
        status: "PLANNED"
        zone: "R-2"
        zoning_code: "2023"
    ```
    """
    if not overrides_path.exists():
        raise FileNotFoundError(f"Overrides file not found: {overrides_path}")
    with open(overrides_path) as f:
        return yaml.safe_load(f) or {}


def apply_overrides(
    projects: list[dict[str, Any]], overrides: dict[str, Any]
) -> list[dict[str, Any]]:
    """Apply overrides to project list."""
    if not overrides:
        return projects

    # Build lookup indices
    by_permit_id = {p["permit_id"]: p for p in projects}
    by_address = {}
    for p in projects:
        for addr in p.get("addresses", []):
            by_address[addr.upper()] = p

    # Apply omissions
    omit_permit_ids = set()
    omit_addresses = []
    for omit in overrides.get("omit", []):
        if "permit_id" in omit:
            omit_permit_ids.add(omit["permit_id"])
        if "address" in omit:
            omit_addresses.append(omit["address"].upper())

    def should_omit(p: dict) -> bool:
        if p["permit_id"] in omit_permit_ids:
            return True
        for addr in p.get("addresses", []):
            addr_upper = addr.upper()
            for omit_addr in omit_addresses:
                # Match if address starts with the omit pattern (handles APT suffixes)
                if addr_upper.startswith(omit_addr):
                    return True
        return False

    projects = [p for p in projects if not should_omit(p)]

    # Apply revisions
    for rev in overrides.get("revise", []):
        target = None
        if "permit_id" in rev and rev["permit_id"] in by_permit_id:
            target = by_permit_id[rev["permit_id"]]
        elif "address" in rev:
            rev_addr = rev["address"].upper()
            # Try exact match first, then prefix match
            if rev_addr in by_address:
                target = by_address[rev_addr]
            else:
                for addr, proj in by_address.items():
                    if addr.startswith(rev_addr):
                        target = proj
                        break

        if target:
            for key in ["units", "use_type", "status", "zone", "zoning_code", "developer"]:
                if key in rev:
                    target[key] = rev[key]

    # Add new projects
    for add in overrides.get("add", []):
        projects.append(
            {
                "units": add.get("units"),
                "permit_id": add.get("permit_id", "OVERRIDE"),
                "project_number": add.get("project_number", ""),
                "use_type": add.get("use_type", "?"),
                "status": add.get("status", "?"),
                "addresses": [add["address"]] if "address" in add else [],
                "parcels": [add["parcel"]] if "parcel" in add else [],
                "zone": add.get("zone", "?"),
                "zoning_code": add.get("zoning_code", "?"),
                "code_reason": add.get("code_reason"),
                "qualifying_date": None,
                "initial_submit": None,
                "last_updated": None,
                "permit_count": 0,
            }
        )

    # Re-sort after modifications
    projects.sort(key=lambda x: -(x["units"] or 0))
    return projects


def main() -> int:
    parser = argparse.ArgumentParser(description="List top developments by unit count")
    parser.add_argument(
        "-n", "--top", type=int, default=15, help="Number of projects to show"
    )
    parser.add_argument(
        "--min-units", type=int, default=None, help="Minimum units to include"
    )
    parser.add_argument(
        "--sort-by",
        choices=["units", "date"],
        default="units",
        help="Sort by units (descending) or date (descending)",
    )
    parser.add_argument(
        "--data",
        type=Path,
        default=Path(__file__).parent / "permits.jsonl",
    )
    parser.add_argument(
        "--parcels",
        type=Path,
        default=Path(__file__).parent / "parcels.json",
    )
    parser.add_argument(
        "--overrides",
        type=Path,
        default=None,
        help="YAML file with project overrides (add, revise, omit)",
    )
    parser.add_argument(
        "--include-without-units",
        action="store_true",
        help="Include Site Plans and Major Development Plans even without unit counts",
    )
    args = parser.parse_args()

    if not args.data.exists():
        print(f"Error: Data file not found: {args.data}")
        return 1

    permits = load_permits(args.data)
    parcel_zones = load_parcel_zones(args.parcels)
    projects = find_developments(
        permits, parcel_zones, args.min_units, args.include_without_units
    )

    if args.overrides:
        overrides = load_overrides(args.overrides)
        projects = apply_overrides(projects, overrides)

    # Re-sort based on argument
    if args.sort_by == "date":
        projects.sort(key=lambda x: x["qualifying_date"] or datetime.min, reverse=True)
        sort_desc = "by Date (newest first)"
    else:
        sort_desc = "by Unit Count"

    writer = csv.writer(sys.stdout, delimiter="\t")
    writer.writerow(
        [
            "Units",
            "Type",
            "Code",
            "Zone",
            "Submit",
            "Updated",
            "Status",
            "Permit",
            "Address",
            "Parcel",
        ]
    )

    for proj in projects[: args.top]:
        addr = proj["addresses"][0] if proj["addresses"] else ""
        initial = (
            proj["initial_submit"].strftime("%Y-%m-%d")
            if proj["initial_submit"]
            else ""
        )
        updated = (
            proj["last_updated"].strftime("%Y-%m-%d") if proj["last_updated"] else ""
        )
        units_display = proj["units"] if proj["units"] is not None else "?"
        writer.writerow(
            [
                units_display,
                proj["use_type"],
                proj["zoning_code"],
                proj["zone"],
                initial,
                updated,
                proj["status"],
                proj.get("project_number", ""),
                addr,
                ",".join(proj["parcels"]),
            ]
        )

    return 0


if __name__ == "__main__":
    exit(main())
