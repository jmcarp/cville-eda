"""Pydantic models for Albemarle County plan records."""

import re
from datetime import date
from typing import Any

from pydantic import BaseModel


def _parse_csv_date(value: Any) -> date | None:
    """Parse date from CSV format 'MM/DD/YYYY HH:MM:SS AM/PM' to date."""
    if not value or not str(value).strip():
        return None
    s = str(value).strip()
    # Try full datetime format first
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y"):
        try:
            from datetime import datetime

            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_csv_bool(value: Any) -> bool:
    """Parse boolean from CSV string 'true'/'false'."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def _parse_csv_float(value: Any) -> float | None:
    """Parse float from CSV string, returning None for empty/zero."""
    if value is None or str(value).strip() == "":
        return None
    try:
        v = float(str(value).strip())
        return v if v != 0 else None
    except ValueError:
        return None


def _parse_geocoded_column(value: Any) -> tuple[float | None, float | None]:
    """Parse 'POINT (lon lat)' WKT format to (latitude, longitude)."""
    if not value or not str(value).strip():
        return (None, None)
    m = re.match(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", str(value).strip())
    if m:
        lon, lat = float(m.group(1)), float(m.group(2))
        return (lat, lon)
    return (None, None)


class AlbemarlePlan(BaseModel):
    """A single plan record from the Albemarle County Socrata dataset."""

    plan_id: str
    plan_number: str
    plan_type: str = ""
    plan_type_group: str = ""
    plan_work_class: str = ""
    plan_status: str = ""
    is_status_cancelled: bool = False
    is_status_successful: bool = False
    is_status_failure: bool = False
    is_status_hold: bool = False
    project_name: str = ""
    project_number: str = ""
    district: str = ""
    main_zone: str = ""
    main_parcel_number: str = ""
    address_line1: str = ""
    predirection: str = ""
    address_line2: str = ""
    city: str = ""
    state: str = ""
    street_type: str = ""
    post_direction: str = ""
    unit_or_suite: str = ""
    address_line3: str = ""
    address_concatenated: str = ""
    application_date: date | None = None
    expiration_date: date | None = None
    approval_expiration_date: date | None = None
    complete_date: date | None = None
    plan_valuation: float | None = None
    square_footage: float | None = None
    balance_due: float | None = None
    amount_paid: float | None = None
    description: str = ""
    assigned_user: str = ""
    latitude: float | None = None
    longitude: float | None = None
    css_record_url: str = ""
    energov_url: str = ""

    # Tracking fields added by fetch_plans.py
    first_seen: date | None = None
    last_seen: date | None = None

    @property
    def plan_year(self) -> int | None:
        """Extract year from plan number.

        Handles formats like:
          SDP-2025-00034 -> 2025
          SDP202200030   -> 2022
          STM-392.03     -> None (no year)
        """
        # Try PREFIX-YYYY-NNNNN format
        m = re.match(r"[A-Z]+-(\d{4})-", self.plan_number)
        if m:
            return int(m.group(1))
        # Try PREFIX + YYYY (no dash) like SDP202200030
        m = re.match(r"[A-Z]+(20\d{2})\d+", self.plan_number)
        if m:
            return int(m.group(1))
        return None

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "AlbemarlePlan":
        """Construct from a csv.DictReader row with raw string values."""
        lat, lon = _parse_geocoded_column(row.get("geocoded_column", ""))

        return cls(
            plan_id=row.get("planid", "").strip(),
            plan_number=row.get("plannumber", "").strip(),
            plan_type=row.get("plantype", "").strip(),
            plan_type_group=row.get("plantypegroup", "").strip(),
            plan_work_class=row.get("planworkclassname", "").strip(),
            plan_status=row.get("planstatus", "").strip(),
            is_status_cancelled=_parse_csv_bool(row.get("isstatuscancelled", "")),
            is_status_successful=_parse_csv_bool(row.get("isstatussuccessful", "")),
            is_status_failure=_parse_csv_bool(row.get("isstatusfailure", "")),
            is_status_hold=_parse_csv_bool(row.get("isstatushold", "")),
            project_name=row.get("projectname", "").strip(),
            project_number=row.get("projectnumber", "").strip(),
            district=row.get("district", "").strip(),
            main_zone=row.get("mainzone", "").strip(),
            main_parcel_number=row.get("mainparcelnumber", "").strip(),
            address_line1=row.get("addressline1", "").strip(),
            predirection=row.get("predirection", "").strip(),
            address_line2=row.get("addressline2", "").strip(),
            city=row.get("city", "").strip(),
            state=row.get("state", "").strip(),
            street_type=row.get("streettype", "").strip(),
            post_direction=row.get("postdirection", "").strip(),
            unit_or_suite=row.get("unitorsuite", "").strip(),
            address_line3=row.get("addressline3", "").strip(),
            address_concatenated=row.get("address_concatenated", "").strip(),
            application_date=_parse_csv_date(row.get("applicationdate", "")),
            expiration_date=_parse_csv_date(row.get("expirationdate", "")),
            approval_expiration_date=_parse_csv_date(
                row.get("approvalexpirationdate", "")
            ),
            complete_date=_parse_csv_date(row.get("completedate", "")),
            plan_valuation=_parse_csv_float(row.get("planvaluation", "")),
            square_footage=_parse_csv_float(row.get("squarefootage", "")),
            balance_due=_parse_csv_float(row.get("balancedue", "")),
            amount_paid=_parse_csv_float(row.get("amountpaid", "")),
            description=row.get("description", "").strip(),
            assigned_user=row.get("assigneduser", "").strip(),
            latitude=lat,
            longitude=lon,
            css_record_url=row.get("css_record_url", "").strip(),
            energov_url=row.get("cssrecorenergov_urldattachurl", "").strip(),
        )
