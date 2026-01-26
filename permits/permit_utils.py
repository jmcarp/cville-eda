"""Shared utilities for permit analysis."""

import json
import re
from pathlib import Path

from models import Permit


def load_permits(jsonl_path: Path) -> dict[str, Permit]:
    """Load permits from JSONL file into a dict keyed by permit_id."""
    permits = {}
    with open(jsonl_path) as f:
        for line in f:
            data = json.loads(line)
            permit = Permit.model_validate(data)
            permits[permit.permit_id] = permit
    return permits


def load_parcel_zones(parcels_path: Path) -> dict[str, str]:
    """Load parcel -> zone mapping from JSON file."""
    if not parcels_path.exists():
        return {}
    with open(parcels_path) as f:
        return json.load(f)


def normalize_permit_id(pid: str, permits: dict[str, Permit]) -> str | None:
    """Normalize permit ID to match keys in permits dict.

    Handles inconsistent formats like "44910" vs "44910.00".
    """
    if pid in permits:
        return pid
    if pid + ".00" in permits:
        return pid + ".00"
    if pid.endswith(".00") and pid[:-3] in permits:
        return pid[:-3]
    return None


def find_related_permits(
    start_ids: list[str], permits: dict[str, Permit]
) -> set[str]:
    """Find all permits related via parent/child relationships.

    Starting from one or more permit IDs, traverses parent and child
    links in both directions to find the full set of related permits.
    """
    visited: set[str] = set()
    queue = list(start_ids)

    while queue:
        pid = queue.pop()
        if pid in visited:
            continue
        visited.add(pid)

        if pid not in permits:
            continue

        p = permits[pid]
        for parent in p.parent_cases:
            norm = normalize_permit_id(parent.permit_id, permits)
            if norm and norm not in visited:
                queue.append(norm)
        for child in p.child_cases:
            norm = normalize_permit_id(child.permit_id, permits)
            if norm and norm not in visited:
                queue.append(norm)

    return visited


def find_permits_by_address(
    permits: dict[str, Permit], address: str
) -> list[Permit]:
    """Find permits where address matches at word boundaries.

    Uses word boundary matching so '0 5TH ST' matches '0 5TH ST SW'
    but not '210 5TH ST SW'.
    """
    # Escape regex special chars and match at word boundaries
    pattern = re.compile(r"\b" + re.escape(address) + r"\b", re.IGNORECASE)
    matches = []
    for permit in permits.values():
        for addr in permit.site_addresses:
            if pattern.search(addr.address):
                matches.append(permit)
                break
    return matches
