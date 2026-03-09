#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pydantic",
#     "pyyaml",
#     "shapely",
# ]
# ///
"""Generate site/cville/data.json and parcels.geojson from permit data."""

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from models import Permit
from permit_utils import load_permits, load_parcel_zones, normalize_permit_id
from top_developments import find_developments, load_overrides, apply_overrides


def build_relationship_graph(permits: dict[str, Permit]) -> tuple[dict, dict]:
    """Build parent->children and child->parents mappings."""
    children_map: dict[str, set[str]] = defaultdict(set)
    parents_map: dict[str, set[str]] = defaultdict(set)

    for permit in permits.values():
        pid = permit.permit_id
        for parent in permit.parent_cases:
            norm = normalize_permit_id(parent.permit_id, permits)
            if norm:
                children_map[norm].add(pid)
                parents_map[pid].add(norm)
        for child in permit.child_cases:
            norm = normalize_permit_id(child.permit_id, permits)
            if norm:
                children_map[pid].add(norm)
                parents_map[norm].add(pid)

    return dict(children_map), dict(parents_map)


def get_intake_date(permit: Permit) -> str | None:
    """Get intake application date from tasks."""
    for task in permit.tasks:
        if task.description == "Intake Application":
            return task.date_completed
    return None


def parse_date(date_str: str | None) -> datetime | None:
    """Parse a date string in MM/DD/YYYY format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except ValueError:
        return None


def get_permit_sort_key(permit: Permit) -> tuple:
    """Sort key for permits: by date created, then by permit ID."""
    dt = parse_date(permit.search_result.date_created)
    return (dt or datetime.min, permit.permit_id)


def build_permit_tree_json(
    project: dict,
    permits: dict[str, Permit],
    children_map: dict[str, set[str]],
    parents_map: dict[str, set[str]],
) -> list[dict]:
    """Build a JSON tree structure of permits for a project.

    Returns a list of root permit nodes, each with nested children.
    """
    # Find all related permits for this project
    from permit_utils import find_related_permits

    related_ids = find_related_permits([project["permit_id"]], permits)
    related_permits = [permits[pid] for pid in related_ids if pid in permits]
    related_permits.sort(key=get_permit_sort_key)

    if not related_permits:
        return []

    related_id_set = {p.permit_id for p in related_permits}

    # Find roots (permits with no parents in our set)
    roots = []
    for p in related_permits:
        has_parent_in_set = False
        for parent_id in parents_map.get(p.permit_id, set()):
            if parent_id in related_id_set:
                has_parent_in_set = True
                break
        if not has_parent_in_set:
            roots.append(p)

    roots.sort(key=get_permit_sort_key)

    def permit_to_dict(p: Permit) -> dict:
        """Convert a permit to a JSON-serializable dict."""
        intake = get_intake_date(p)
        date_str = intake if intake else p.search_result.date_created
        # Parse and format as ISO date
        dt = parse_date(date_str)
        iso_date = dt.strftime("%Y-%m-%d") if dt else None
        return {
            "permit_id": p.permit_id,
            "permit_type": p.search_result.permit_type,
            "sub_type": p.search_result.sub_type,
            "status": p.search_result.status,
            "date": iso_date,
            "url": p.url,
            "children": [],
        }

    def build_subtree(p: Permit, visited: set[str]) -> dict | None:
        """Recursively build the subtree for a permit."""
        if p.permit_id in visited:
            return None
        visited.add(p.permit_id)

        node = permit_to_dict(p)

        # Find children in our related set
        children = []
        for child_id in children_map.get(p.permit_id, set()):
            if child_id in related_id_set and child_id not in visited:
                children.append(permits[child_id])

        children.sort(key=get_permit_sort_key)
        for child in children:
            child_node = build_subtree(child, visited)
            if child_node:
                node["children"].append(child_node)

        return node

    visited: set[str] = set()
    tree = []
    for root in roots:
        node = build_subtree(root, visited)
        if node:
            tree.append(node)

    return tree


def serialize_project(project: dict, permit_tree: list[dict]) -> dict:
    """Serialize a project dict for JSON output."""
    # Convert datetime objects to ISO strings
    initial_submit = project.get("initial_submit")
    last_updated = project.get("last_updated")

    return {
        "units": project.get("units"),
        "permit_id": project["permit_id"],
        "project_number": project.get("project_number", ""),
        "use_type": project.get("use_type", "?"),
        "developer": project.get("developer"),
        "status": project.get("status", "?"),
        "addresses": project.get("addresses", []),
        "parcels": project.get("parcels", []),
        "zone": project.get("zone", "?"),
        "code_year": project.get("zoning_code", "?"),
        "initial_submit": initial_submit.strftime("%Y-%m-%d") if initial_submit else None,
        "last_updated": last_updated.strftime("%Y-%m-%d") if last_updated else None,
        "permit_count": project.get("permit_count", 0),
        "permit_tree": permit_tree,
    }


def main() -> int:
    base_path = Path(__file__).parent
    permits_path = base_path / "permits.jsonl"
    parcels_path = base_path / "parcels.json"
    parcels_geo_path = base_path / "parcels_geo.geojson"
    overrides_path = base_path / "overrides.yaml"
    output_dir = base_path / "site" / "cville"
    output_path = output_dir / "data.json"
    geojson_path = output_dir / "parcels.geojson"

    if not permits_path.exists():
        print(f"Error: Data file not found: {permits_path}")
        return 1

    print("Loading permits...")
    permits = load_permits(permits_path)
    parcel_zones = load_parcel_zones(parcels_path)

    print("Building relationship graph...")
    children_map, parents_map = build_relationship_graph(permits)

    print("Finding developments...")
    projects = find_developments(permits, parcel_zones, min_units=None, include_without_units=True)

    # Apply overrides if file exists
    if overrides_path.exists():
        print("Applying overrides...")
        overrides = load_overrides(overrides_path)
        projects = apply_overrides(projects, overrides)

    print(f"Processing {len(projects)} projects...")
    serialized_projects = []
    for project in projects:
        permit_tree = build_permit_tree_json(project, permits, children_map, parents_map)
        serialized = serialize_project(project, permit_tree)
        serialized_projects.append(serialized)

    output_data = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "projects": serialized_projects,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2)

    print(f"Wrote {len(serialized_projects)} projects to {output_path}")

    # Build parcel GeoJSON if geometry file is available
    if parcels_geo_path.exists():
        from build_parcels import build_parcels

        print("Building parcel GeoJSON...")
        pin_to_projects: dict[str, list[dict]] = defaultdict(list)
        for project in projects:
            for pin in project.get("parcels", []):
                pin_to_projects[pin].append(project)

        geojson = build_parcels(parcels_geo_path, dict(pin_to_projects))
        with open(geojson_path, "w") as f:
            json.dump(geojson, f)
        size_kb = geojson_path.stat().st_size / 1024
        print(f"Wrote {geojson_path} ({size_kb:.0f} KB)")
    else:
        print(f"Warning: {parcels_geo_path} not found, skipping parcels.geojson")

    return 0


if __name__ == "__main__":
    exit(main())
