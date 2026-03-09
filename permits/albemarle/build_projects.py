"""Project grouping, unit extraction, and deduplication for Albemarle plans."""

import re
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from models import AlbemarlePlan

CORE_PLAN_TYPES = {
    "Site Development Plan",
    "Subdivision",
    "Zoning Map Amendment",
    "Special Use",
    "Special Exceptions",
}

STATUS_PRIORITY = {
    "Approved": 100,
    "In Review": 80,
    "Submitted": 75,
    "Submitted - Online": 75,
    "Complete": 70,
    "Fees Paid": 65,
    "Fees Due": 60,
    "On Hold": 40,
    "Deferred Definite": 30,
    "Deferred Indefinite": 30,
    "Review Expired": 20,
    "Denied": 15,
    "Void": 10,
    "Withdrawn": 10,
}

# Patterns to extract unit counts from description text, ordered by specificity.
# Capture group matches digits with optional comma separators (e.g. "1,600").
_NUM = r"(\d[\d,]*)"
_UNIT_PATTERNS = [
    (re.compile(_NUM + r"\s*(?:dwelling|residential)\s*units?", re.I), "dwelling"),
    (
        re.compile(
            _NUM + r"\s*(?:apartment|townhome|townhouse|condo|multifamily|multi-family)\s*units?", re.I
        ),
        "housing_type",
    ),
    (re.compile(_NUM + r"\s+units?\b", re.I), "generic"),
]

_LOT_PATTERN = re.compile(_NUM + r"\s*(?:lots?|parcels?)\b", re.I)

# Density: "N dwelling units per acre", "N du/acre", "N units per acre", etc.
_DENSITY_PATTERN = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:dwelling\s+units?|du|units?)\s*(?:per\s+acre|/\s*acre)",
    re.I,
)

# Context that invalidates a unit match (preceding or following text)
_EXCLUDE_BEFORE = re.compile(r"(?:sq|square|\$|acre)\s*$", re.I)
_EXCLUDE_AFTER = re.compile(r"^\s*(?:sf|square\s*feet|per\s*acre|/\s*acre)", re.I)


def extract_units(description: str) -> tuple[int | None, str | None]:
    """Extract unit count from description text.

    Returns (count, match_type) or (None, None).
    """
    if not description:
        return (None, None)

    for pattern, match_type in _UNIT_PATTERNS:
        for m in pattern.finditer(description):
            count = int(m.group(1).replace(",", ""))
            if count < 2 or count > 4000:
                continue
            # Check context exclusions
            before = description[: m.start()]
            after = description[m.end() :]
            if _EXCLUDE_BEFORE.search(before):
                continue
            if _EXCLUDE_AFTER.match(after):
                continue
            return (count, match_type)

    return (None, None)


def extract_lots(description: str) -> int | None:
    """Extract lot/parcel count from description text."""
    if not description:
        return None
    for m in _LOT_PATTERN.finditer(description):
        count = int(m.group(1).replace(",", ""))
        if 2 <= count <= 4000:
            before = description[: m.start()]
            after = description[m.end() :]
            if _EXCLUDE_BEFORE.search(before):
                continue
            if _EXCLUDE_AFTER.match(after):
                continue
            return count
    return None


def extract_density(description: str) -> float | None:
    """Extract residential density (units per acre) from description text.

    Returns the highest density mentioned, or None.
    """
    if not description:
        return None
    best: float | None = None
    for m in _DENSITY_PATTERN.finditer(description):
        val = float(m.group(1))
        if 0.1 <= val <= 200:
            if best is None or val > best:
                best = val
    return best


_PLAN_NUM = r"[A-Z]{2,}\s*[-\d]+"
_PLAN_LIST = rf"(?:{_PLAN_NUM}[,\s]*(?:and\s+)?)+"
_PROJECT_PREFIX_RE = re.compile(rf"^PROJECT:\s*{_PLAN_LIST}\s*(.+)", re.I)
_PLAN_NUMBER_PREFIX_RE = re.compile(rf"^{_PLAN_LIST}\s*(.+)", re.I)
_SKIP_PREFIXES = (
    "LOR",
    "DEEMED",
    "REVISED",
    "PURPOSE",
    "The purpose",
    "This",
    "See ",
    "Application deemed",
    "A corrected",
)
# Delimiters that separate a project name from trailing description
_NAME_DELIMITERS = re.compile(r"\s*[(\[:]\s*|\s+[-–]\s+")
# Leading junk: punctuation, "and"/"see" connectors, plan numbers, whitespace
_LEADING_JUNK = re.compile(
    rf"^(?:[&–\-,\s]+(?:(?:and|see)\s+)?(?:{_PLAN_NUM}[,\s]*)*\s*)"
)

_MAX_NAME_WORDS = 8
# Plan-type suffixes that follow the actual project name
_PLAN_TYPE_SUFFIX = re.compile(
    r"\s*[-–]?\s*\b("
    r"(?:Initial|Final|Major|Minor)\s+Site\s+Plan"
    r"|Site\s+Plan\s+(?:Amendment|Exception)"
    r"|(?:Preliminary|Final)\s+(?:Subdivision\s+)?Plat"
    r"|Road\s+Plans?"
    r"|Erosion\s+Control\s+Plan"
    r")\b",
    re.I,
)


def _clean_extracted_name(raw: str) -> str:
    """Truncate at first delimiter, strip leading junk, enforce word cap."""
    # Strip leading punctuation and stray plan numbers
    name = _LEADING_JUNK.sub("", raw).strip()
    # Truncate at first delimiter (parens, colon, dash separator)
    name = _NAME_DELIMITERS.split(name, maxsplit=1)[0].strip()
    # Truncate at plan-type suffixes (e.g. "Initial Site Plan")
    name = _PLAN_TYPE_SUFFIX.split(name, maxsplit=1)[0].strip()
    name = name.rstrip(" .,;-–")
    if not name or len(name.split()) > _MAX_NAME_WORDS:
        return ""
    return name


def extract_project_name(description: str) -> str:
    """Extract a project name from the first line of a description."""
    if not description:
        return ""
    first_line = description.split("\n", 1)[0].strip()
    if not first_line:
        return ""

    m = _PROJECT_PREFIX_RE.match(first_line)
    if m:
        return _clean_extracted_name(m.group(1))

    m = _PLAN_NUMBER_PREFIX_RE.match(first_line)
    if m:
        return _clean_extracted_name(m.group(1))

    if len(first_line) <= 60 and not first_line.startswith(_SKIP_PREFIXES):
        name = _PLAN_TYPE_SUFFIX.split(first_line, maxsplit=1)[0].strip()
        name = name.rstrip(" .,;-–")
        return name if name else ""

    return ""


def _select_primary(plans: list[AlbemarlePlan]) -> AlbemarlePlan:
    """Select the primary plan from a group.

    Prefers Site Development Plans (Initial > Final > Amendment),
    then Zoning Map Amendments, then earliest application_date.
    """
    type_order = {
        "Site Development Plan": 0,
        "Zoning Map Amendment": 1,
        "Special Use": 2,
        "Special Exceptions": 3,
        "Subdivision": 4,
    }
    work_class_order = {"Initial": 0, "Final": 1, "Amendment": 2}

    def sort_key(p: AlbemarlePlan) -> tuple:
        t = type_order.get(p.plan_type, 99)
        w = work_class_order.get(p.plan_work_class, 99)
        status_pri = STATUS_PRIORITY.get(p.plan_status, 0)
        app_date = p.application_date or date.max
        return (t, w, -status_pri, app_date)

    return sorted(plans, key=sort_key)[0]


def _is_core_type(plan: AlbemarlePlan) -> bool:
    return plan.plan_type in CORE_PLAN_TYPES


def _get_custom_field(
    custom_fields: dict[str, dict], plan_id: str, field_name: str
) -> Any | None:
    """Look up a custom field value from the EnerGov cache."""
    entry = custom_fields.get(plan_id)
    if not entry:
        return None
    field = entry.get("custom_fields", {}).get(field_name)
    if field:
        return field.get("value")
    return None


def _build_project(
    primary: AlbemarlePlan,
    group: list[AlbemarlePlan],
    custom_fields: dict[str, dict],
) -> dict:
    """Build a project dict from a primary plan and its group."""
    # Collect addresses and parcels from all plans in group
    addresses: list[str] = []
    parcels: list[str] = []
    seen_addr: set[str] = set()
    seen_parcel: set[str] = set()

    for p in group:
        addr = p.address_concatenated.strip()
        if addr and addr not in seen_addr:
            addresses.append(addr)
            seen_addr.add(addr)
        if p.main_parcel_number and p.main_parcel_number not in seen_parcel:
            parcels.append(p.main_parcel_number)
            seen_parcel.add(p.main_parcel_number)

    # Extract units, lots, density from all descriptions in group, take highest
    best_units: int | None = None
    best_lots: int | None = None
    best_density: float | None = None

    for p in group:
        units, _ = extract_units(p.description)
        if units is not None and (best_units is None or units > best_units):
            best_units = units
        lots = extract_lots(p.description)
        if lots is not None and (best_lots is None or lots > best_lots):
            best_lots = lots
        density = extract_density(p.description)
        if density is not None and (best_density is None or density > best_density):
            best_density = density

    # Override with structured EnerGov custom fields where available
    energov_units: int | None = None
    for p in group:
        val = _get_custom_field(custom_fields, p.plan_id, "ProposedNumberofDwellingUnits")
        if val is not None:
            try:
                n = int(float(val))
                if n > 0 and (energov_units is None or n > energov_units):
                    energov_units = n
            except (ValueError, TypeError):
                pass
    if energov_units is not None:
        best_units = energov_units

    # Aggregate valuation and square footage
    total_valuation = None
    total_sqft = None
    for p in group:
        if p.plan_valuation:
            total_valuation = (total_valuation or 0) + p.plan_valuation
        if p.square_footage:
            total_sqft = (total_sqft or 0) + p.square_footage

    # Related plans (everything except the primary)
    related_plans = []
    for p in group:
        if p.plan_id == primary.plan_id:
            continue
        related_plans.append(
            {
                "plan_number": p.plan_number,
                "plan_type": p.plan_type,
                "work_class": p.plan_work_class,
                "status": p.plan_status,
                "application_date": (
                    p.application_date.isoformat() if p.application_date else None
                ),
                "description": p.description,
            }
        )

    project = {
        "plan_id": primary.plan_id,
        "plan_number": primary.plan_number,
        "project_name": primary.project_name,
        "project_number": primary.project_number,
        "plan_type": primary.plan_type,
        "work_class": primary.plan_work_class,
        "status": primary.plan_status,
        "units": best_units,
        "lots": best_lots,
        "density": best_density,
        "addresses": addresses,
        "parcels": parcels,
        "zone": primary.main_zone,
        "district": primary.district,
        "application_date": (
            primary.application_date.isoformat() if primary.application_date else None
        ),
        "complete_date": (
            primary.complete_date.isoformat() if primary.complete_date else None
        ),
        "valuation": total_valuation,
        "square_footage": total_sqft,
        "description": primary.description,
        "plan_count": len(group),
        "related_plans": related_plans,
    }

    if not project["project_name"]:
        for p in [primary] + [x for x in group if x != primary]:
            name = extract_project_name(p.description)
            if name:
                project["project_name"] = name
                break

    return project


def find_projects(
    plans: dict[str, AlbemarlePlan],
    custom_fields: dict[str, dict] | None = None,
) -> list[dict]:
    """Group plans into projects, extract units, deduplicate.

    Two-pass grouping:
    1. By project_number (non-empty)
    2. By main_parcel_number for ungrouped core development types

    If custom_fields is provided (from EnerGov API cache), structured field
    values override regex-extracted values where available.
    """
    cf = custom_fields or {}
    grouped_ids: set[str] = set()
    projects: list[dict] = []

    # Pass 1: group by project_number
    by_project_number: dict[str, list[AlbemarlePlan]] = defaultdict(list)
    for plan in plans.values():
        if plan.project_number:
            by_project_number[plan.project_number].append(plan)

    for project_number, group in by_project_number.items():
        # Only create a project if at least one plan is a core type
        if not any(_is_core_type(p) for p in group):
            continue
        primary = _select_primary(group)
        projects.append(_build_project(primary, group, cf))
        grouped_ids.update(p.plan_id for p in group)

    # Pass 2: group remaining by parcel, but only core types
    by_parcel: dict[str, list[AlbemarlePlan]] = defaultdict(list)
    for plan in plans.values():
        if plan.plan_id in grouped_ids:
            continue
        if not _is_core_type(plan):
            continue
        if plan.main_parcel_number:
            by_parcel[plan.main_parcel_number].append(plan)

    for parcel, group in by_parcel.items():
        primary = _select_primary(group)
        projects.append(_build_project(primary, group, cf))
        grouped_ids.update(p.plan_id for p in group)

    # Pass 3: remaining ungrouped core-type plans as standalone projects
    for plan in plans.values():
        if plan.plan_id in grouped_ids:
            continue
        if not _is_core_type(plan):
            continue
        projects.append(_build_project(plan, [plan], cf))

    # Sort by units descending, then by application_date
    projects.sort(
        key=lambda p: (-(p["units"] or 0), p["application_date"] or "9999")
    )

    return projects


def load_overrides(path: Path) -> dict[str, Any]:
    """Load overrides from YAML file."""
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def apply_overrides(
    projects: list[dict[str, Any]], overrides: dict[str, Any]
) -> list[dict[str, Any]]:
    """Apply overrides (omit/revise/add) to project list."""
    if not overrides:
        return projects

    # Build lookup indices
    by_plan_number = {p["plan_number"]: p for p in projects}
    by_address: dict[str, dict] = {}
    for p in projects:
        for addr in p.get("addresses", []):
            by_address[addr.upper()] = p

    # Apply omissions
    omit_plan_numbers: set[str] = set()
    omit_addresses: list[str] = []
    for omit in overrides.get("omit", []):
        if "plan_number" in omit:
            omit_plan_numbers.add(omit["plan_number"])
        if "address" in omit:
            omit_addresses.append(omit["address"].upper())

    def should_omit(p: dict) -> bool:
        if p["plan_number"] in omit_plan_numbers:
            return True
        for addr in p.get("addresses", []):
            addr_upper = addr.upper()
            for omit_addr in omit_addresses:
                if addr_upper.startswith(omit_addr):
                    return True
        return False

    projects = [p for p in projects if not should_omit(p)]

    # Apply revisions
    for rev in overrides.get("revise", []):
        target = None
        if "plan_number" in rev and rev["plan_number"] in by_plan_number:
            target = by_plan_number[rev["plan_number"]]
        elif "address" in rev:
            rev_addr = rev["address"].upper()
            if rev_addr in by_address:
                target = by_address[rev_addr]
            else:
                for addr, proj in by_address.items():
                    if addr.startswith(rev_addr):
                        target = proj
                        break

        if target:
            for key in [
                "units",
                "lots",
                "density",
                "plan_type",
                "status",
                "zone",
                "district",
                "project_name",
            ]:
                if key in rev:
                    target[key] = rev[key]

    # Add new projects
    for add in overrides.get("add", []):
        projects.append(
            {
                "plan_id": add.get("plan_id", "OVERRIDE"),
                "plan_number": add.get("plan_number", "OVERRIDE"),
                "project_name": add.get("project_name", ""),
                "project_number": add.get("project_number", ""),
                "plan_type": add.get("plan_type", "?"),
                "work_class": add.get("work_class", ""),
                "status": add.get("status", "?"),
                "units": add.get("units"),
                "lots": add.get("lots"),
                "density": add.get("density"),
                "addresses": [add["address"]] if "address" in add else [],
                "parcels": [add["parcel"]] if "parcel" in add else [],
                "zone": add.get("zone", "?"),
                "district": add.get("district", "?"),
                "application_date": None,
                "complete_date": None,
                "valuation": add.get("valuation"),
                "square_footage": add.get("square_footage"),
                "description": add.get("description", ""),
                "plan_count": 0,
                "related_plans": [],
            }
        )

    # Re-sort
    projects.sort(
        key=lambda p: (-(p["units"] or 0), p["application_date"] or "9999")
    )
    return projects
