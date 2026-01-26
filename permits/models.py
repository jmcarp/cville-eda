"""Pydantic models for Charlottesville permit data."""

import re

from pydantic import BaseModel, computed_field, field_validator


class SearchResult(BaseModel):
    permit_id: str
    project_number: str
    permit_type: str
    sub_type: str
    status: str
    site_address: str
    parcel_number: str
    date_created: str


class CaseLink(BaseModel):
    permit_id: str
    project_number: str


class PermitInfo(BaseModel):
    permit_number: str
    location: str
    permit_type: str
    status: str
    date_issued: str | None
    case_type: str
    case_type_id: str
    sub_type_id: str


class SiteAddress(BaseModel):
    address: str
    suite: str
    city: str
    state: str
    zip: str
    parcel_id: str


class Contact(BaseModel):
    name: str
    role: str


class Contractor(BaseModel):
    business_name: str
    contractor_type: str
    city: str
    state: str


class Detail(BaseModel):
    category: str
    description: str
    data: str


class TaskComment(BaseModel):
    text: str
    date_created: str


class Task(BaseModel):
    description: str
    result: str
    date_completed: str
    completed_by: str
    task_id: str | None = None  # caTaskId for fetching comments
    comments: list[TaskComment] = []

    @field_validator("comments", mode="before")
    @classmethod
    def parse_comments(cls, v):
        # Handle string values like "No Comments" or "View Comments" as empty list
        if isinstance(v, str):
            return []
        return v


class Inspection(BaseModel):
    inspection_type: str
    inspection_date: str
    requested_by: str
    status: str


class Condition(BaseModel):
    description: str
    details: str
    date_applied: str
    date_completed: str


class Flag(BaseModel):
    description: str
    date_applied: str
    date_completed: str


class Fee(BaseModel):
    description: str
    amount: str
    balance_due: str


class Payment(BaseModel):
    description: str
    fee_amount: str
    payment_amount: str
    payment_date: str
    payment_method: str
    reference: str


class Attachment(BaseModel):
    attachment_type: str
    filename: str
    date: str
    download_url: str


class Permit(BaseModel):
    permit_id: str
    project_number: str
    url: str
    fetched_at: str
    search_result: SearchResult
    info: PermitInfo
    parent_cases: list[CaseLink] = []
    child_cases: list[CaseLink] = []
    site_addresses: list[SiteAddress] = []
    contacts: list[Contact] = []
    contractors: list[Contractor] = []
    details: list[Detail] = []
    tasks: list[Task] = []
    inspections: list[Inspection] = []
    conditions: list[Condition] = []
    flags: list[Flag] = []
    notes: list[str] = []
    fees: list[Fee] = []
    payments: list[Payment] = []
    attachments: list[Attachment] = []

    @computed_field  # type: ignore[prop-decorator]
    @property
    def permit_year(self) -> int | None:
        """Extract year from permit number (e.g., ZM23-00004 -> 2023)."""
        pnum = self.info.permit_number
        if not pnum:
            return None
        match = re.match(r"^[A-Z]+(\d{2})-", pnum)
        if match:
            yy = int(match.group(1))
            # Assume 00-29 = 2000-2029, 30-99 = 1930-1999
            return 2000 + yy if yy < 30 else 1900 + yy
        return None
