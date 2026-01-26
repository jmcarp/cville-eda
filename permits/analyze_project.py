#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pydantic",
#     "python-dateutil",
#     "types-python-dateutil",
# ]
# ///
"""Generate detailed project reports from fetched permit data."""

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dateutil import parser as date_parser

from models import Permit


def load_permits(jsonl_path: Path) -> dict[str, Permit]:
    """Load permits from JSONL file, deduplicating by permit_id."""
    permits = {}
    with open(jsonl_path) as f:
        for line in f:
            data = json.loads(line)
            permit = Permit.model_validate(data)
            permits[permit.permit_id] = permit
    return permits


def load_parcel_zones(parcels_path: Path) -> dict[str, str]:
    """Load parcel number to zone mapping."""
    if not parcels_path.exists():
        return {}
    with open(parcels_path) as f:
        return json.load(f)


def find_permits_by_address(permits: dict[str, Permit], address: str) -> list[Permit]:
    """Find all permits matching an address (case-insensitive partial match)."""
    address_lower = address.lower()
    matches = []
    for permit in permits.values():
        for addr in permit.site_addresses:
            if address_lower in addr.address.lower():
                matches.append(permit)
                break
    return matches


def build_relationship_graph(permits: dict[str, Permit]) -> tuple[dict, dict]:
    """Build parent->children and child->parents mappings."""
    children_map = defaultdict(set)
    parents_map = defaultdict(set)

    for permit in permits.values():
        pid = permit.permit_id
        for parent in permit.parent_cases:
            children_map[parent.permit_id].add(pid)
            parents_map[pid].add(parent.permit_id)
        for child in permit.child_cases:
            children_map[pid].add(child.permit_id)
            parents_map[child.permit_id].add(pid)

    return dict(children_map), dict(parents_map)


def find_all_related(
    start_ids: set[str],
    permits: dict[str, Permit],
    children_map: dict,
    parents_map: dict,
) -> set[str]:
    """Find all permits related to the starting set via parent/child relationships."""
    visited = set()
    queue = list(start_ids)

    while queue:
        pid = queue.pop()
        if pid in visited:
            continue
        visited.add(pid)

        for child_id in children_map.get(pid, []):
            if child_id not in visited and child_id in permits:
                queue.append(child_id)

        for parent_id in parents_map.get(pid, []):
            if parent_id not in visited and parent_id in permits:
                queue.append(parent_id)

    return visited


def parse_date(date_str: str | None) -> datetime | None:
    """Parse a date string, returning None if invalid."""
    if not date_str:
        return None
    try:
        return date_parser.parse(date_str)
    except (ValueError, TypeError):
        return None


def format_date(dt: datetime | None) -> str:
    """Format a datetime for display."""
    if dt is None:
        return "Unknown"
    return dt.strftime("%Y-%m-%d")


def get_permit_sort_key(permit: Permit) -> tuple:
    """Sort key for permits: by date created, then by permit ID."""
    dt = parse_date(permit.search_result.date_created)
    return (dt or datetime.min, permit.permit_id)


def normalize_permit_id(pid: str, permits: dict[str, Permit]) -> str | None:
    """Normalize permit ID to match keys in permits dict."""
    if pid in permits:
        return pid
    if pid + ".00" in permits:
        return pid + ".00"
    if pid.endswith(".00") and pid[:-3] in permits:
        return pid[:-3]
    return None


def get_intake_date(permit: Permit) -> str | None:
    """Get intake application date from tasks."""
    for task in permit.tasks:
        if task.description == "Intake Application":
            return task.date_completed
    return None


def build_permit_tree(
    related_permits: list[Permit],
    permits: dict[str, Permit],
    children_map: dict,
    parents_map: dict,
) -> list[str]:
    """Build a tree view of permits based on parent-child relationships."""
    lines = []
    related_ids = {p.permit_id for p in related_permits}

    # Find roots (permits with no parents in our set)
    roots = []
    for p in related_permits:
        has_parent_in_set = False
        for parent_id in parents_map.get(p.permit_id, []):
            norm_id = normalize_permit_id(parent_id, permits)
            if norm_id and norm_id in related_ids:
                has_parent_in_set = True
                break
        if not has_parent_in_set:
            roots.append(p)

    # Sort roots by date
    roots.sort(key=get_permit_sort_key)

    def format_permit_line(p: Permit) -> str:
        ptype = p.search_result.permit_type
        subtype = p.search_result.sub_type
        status = p.search_result.status
        intake = get_intake_date(p)
        date_str = intake if intake else p.search_result.date_created
        return f"{p.permit_id}: {ptype}/{subtype} [{status}] {date_str}"

    def print_subtree(p: Permit, indent: int, visited: set[str]) -> None:
        if p.permit_id in visited:
            return
        visited.add(p.permit_id)

        prefix = "│ " * indent
        branch = "├─" if indent > 0 else ""
        lines.append(f"{prefix}{branch}{format_permit_line(p)}")

        # Find children in our related set
        children = []
        for child_id in children_map.get(p.permit_id, []):
            norm_id = normalize_permit_id(child_id, permits)
            if norm_id and norm_id in related_ids and norm_id not in visited:
                children.append(permits[norm_id])

        children.sort(key=get_permit_sort_key)
        for child in children:
            print_subtree(child, indent + 1, visited)

    visited: set[str] = set()
    for root in roots:
        print_subtree(root, 0, visited)

    return lines


def generate_report(
    address: str, permits: dict[str, Permit], parcel_zones: dict[str, str]
) -> str:
    """Generate a detailed project report for an address."""
    matches = find_permits_by_address(permits, address)
    if not matches:
        return f"No permits found matching '{address}'"

    children_map, parents_map = build_relationship_graph(permits)
    start_ids = {p.permit_id for p in matches}
    all_related_ids = find_all_related(start_ids, permits, children_map, parents_map)

    related_permits = [permits[pid] for pid in all_related_ids if pid in permits]
    related_permits.sort(key=get_permit_sort_key)

    # Gather summary stats
    all_addresses = set()
    all_parcels = set()
    earliest_date = None
    latest_date = None

    for p in related_permits:
        for addr in p.site_addresses:
            all_addresses.add(addr.address)
        if p.search_result.parcel_number:
            all_parcels.add(p.search_result.parcel_number)
        dt = parse_date(p.search_result.date_created)
        if dt:
            if earliest_date is None or dt < earliest_date:
                earliest_date = dt
            if latest_date is None or dt > latest_date:
                latest_date = dt

    # Get zones for parcels
    zones = {parcel_zones.get(p, "Unknown") for p in all_parcels}
    zones.discard("Unknown")

    lines = []
    lines.append(f"# Project Report: {address}")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- **Total permits:** {len(related_permits)}")
    lines.append(f"- **Addresses:** {', '.join(sorted(all_addresses)) or 'None'}")
    lines.append(f"- **Parcel numbers:** {', '.join(sorted(all_parcels)) or 'None'}")
    lines.append(f"- **Zoning:** {', '.join(sorted(zones)) or 'Unknown'}")
    lines.append(f"- **First submission:** {format_date(earliest_date)}")
    lines.append(f"- **Most recent:** {format_date(latest_date)}")
    lines.append("")

    # Group permits by type
    by_type = defaultdict(list)
    for p in related_permits:
        by_type[p.search_result.permit_type].append(p)

    lines.append("## Permits by Type")
    for ptype in sorted(by_type.keys()):
        lines.append(f"- **{ptype}:** {len(by_type[ptype])}")
    lines.append("")

    # Permit tree
    lines.append("## Permit Tree")
    lines.append("```")
    tree_lines = build_permit_tree(related_permits, permits, children_map, parents_map)
    lines.extend(tree_lines)
    lines.append("```")
    lines.append("")

    # Timeline
    lines.append("## Timeline")
    for p in related_permits:
        dt = parse_date(p.search_result.date_created)
        date_str = format_date(dt)
        status = p.search_result.status
        ptype = p.search_result.permit_type
        subtype = p.search_result.sub_type
        desc = p.info.location or "No description"

        lines.append(f"### {date_str} - {p.permit_id}")
        lines.append(f"**Type:** {ptype} / {subtype}  ")
        lines.append(f"**Status:** {status}  ")
        lines.append(f"**Location:** {desc}")

        # Show key details
        for detail in p.details:
            if detail.description in (
                "Number of Dwelling Units",
                "Number of Units",
                "Job Value",
                "Valuation",
            ):
                lines.append(f"**{detail.description}:** {detail.data}")

        # Show relationships
        if p.parent_cases:
            parent_ids = [c.permit_id for c in p.parent_cases]
            lines.append(f"**Parent cases:** {', '.join(parent_ids)}")
        if p.child_cases:
            child_ids = [c.permit_id for c in p.child_cases]
            lines.append(f"**Child cases:** {', '.join(child_ids)}")

        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate project reports from permit data"
    )
    parser.add_argument("address", help="Address to search for (partial match)")
    parser.add_argument(
        "--data",
        type=Path,
        default=Path(__file__).parent / "permits.jsonl",
        help="Path to permits.jsonl file",
    )
    parser.add_argument(
        "--parcels",
        type=Path,
        default=Path(__file__).parent / "parcels.json",
        help="Path to parcels.json file",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output file (default: stdout)",
    )
    args = parser.parse_args()

    if not args.data.exists():
        print(f"Error: Data file not found: {args.data}")
        return 1

    permits = load_permits(args.data)
    parcel_zones = load_parcel_zones(args.parcels)
    report = generate_report(args.address, permits, parcel_zones)

    if args.output:
        args.output.write_text(report)
        print(f"Report written to {args.output}")
    else:
        print(report)

    return 0


if __name__ == "__main__":
    exit(main())
