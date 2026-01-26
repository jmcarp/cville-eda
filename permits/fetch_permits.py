#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx",
#     "lxml",
#     "pydantic",
#     "python-dotenv",
#     "tenacity",
#     "tqdm",
# ]
# ///
"""Fetch permit data from the Charlottesville permits portal."""

import argparse
import asyncio
import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import lxml.html
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from tqdm import tqdm

from models import (
    SearchResult, CaseLink, PermitInfo, SiteAddress, Contact, Contractor,
    Detail, Task, TaskComment, Inspection, Condition, Flag, Fee, Payment, Attachment, Permit,
)

BASE_URL = "https://permits.charlottesville.gov/portal"
LOGIN_URL = BASE_URL
SEARCH_URL = f"{BASE_URL}/SearchByLocation/Search"
PERMIT_URL = f"{BASE_URL}/PermitInfo/Index"

HEADERS = {"User-Agent": "cville-permits-fetcher"}

HTML_CACHE_DIR = Path("html_cache")


async def login(client: httpx.AsyncClient, username: str, password: str) -> None:
    resp = await client.post(
        LOGIN_URL,
        data={"LoginName": username, "Password": password},
        follow_redirects=False,
    )
    if resp.status_code != 302:
        raise RuntimeError(f"Login failed with status {resp.status_code}: {resp.text[:500]}")


async def search_permits(
    client: httpx.AsyncClient, start_date: date, end_date: date
) -> list[SearchResult]:
    """Returns deduplicated search results (one per permit_id)."""
    resp = await client.get(
        SEARCH_URL,
        params={
            "keyword": "",
            "fromDateInput": start_date.strftime("%m-%d-%Y"),
            "toDateInput": end_date.strftime("%m-%d-%Y"),
        },
    )
    resp.raise_for_status()

    doc = lxml.html.fromstring(resp.content)
    rows = doc.xpath("//table[@id='search-table']/tbody/tr")

    # Deduplicate by permit_id (search returns one row per address)
    seen: dict[str, SearchResult] = {}
    for row in rows:
        cells = row.xpath("./td")
        if len(cells) < 8:
            raise ValueError(f"Expected 8 columns in search results, got {len(cells)}")

        permit_id = cells[0].text_content().strip()
        if permit_id not in seen:
            seen[permit_id] = SearchResult(
                permit_id=permit_id,
                project_number=cells[1].text_content().strip(),
                permit_type=cells[2].text_content().strip(),
                sub_type=cells[3].text_content().strip(),
                status=cells[4].text_content().strip(),
                site_address=cells[5].text_content().strip(),
                parcel_number=cells[6].text_content().strip(),
                date_created=cells[7].text_content().strip(),
            )

    return list(seen.values())


def parse_info(doc: lxml.html.HtmlElement) -> PermitInfo:
    panel = doc.xpath("//div[@id='casePanel']")[0]

    def extract_field(label: str) -> str | None:
        elems = panel.xpath(f".//p[@class='font-13'][strong[contains(text(), '{label}')]]")
        if not elems:
            return None
        text = elems[0].text_content()
        # Extract value after the colon
        if ":" in text:
            return text.split(":", 1)[1].strip()
        return text.replace(label, "").strip()

    def extract_hidden(field_id: str) -> str:
        elems = panel.xpath(f".//input[@id='{field_id}']/@value")
        return elems[0] if elems else ""

    return PermitInfo(
        permit_number=extract_field("Permit/License Number") or "",
        location=extract_field("Location") or "",
        permit_type=extract_field("Permit Type") or "",
        status=extract_field("Status") or "",
        date_issued=extract_field("Date Issued") or None,
        case_type=extract_hidden("caseType"),
        case_type_id=extract_hidden("caseTypeId"),
        sub_type_id=extract_hidden("subTypeId"),
    )


def parse_case_links(doc: lxml.html.HtmlElement, label: str) -> list[CaseLink]:
    """Parse Parent Cases or Child Cases links."""
    panel = doc.xpath("//div[@id='casePanel']")[0]
    elems = panel.xpath(f".//p[@class='font-13'][strong[contains(text(), '{label}')]]")
    if not elems:
        return []

    links = elems[0].xpath(".//a")
    results = []
    for link in links:
        href = link.get("href", "")
        match = re.search(r"caobjectid=(\S+)", href, re.IGNORECASE)
        if match:
            project_number = link.text_content().strip().rstrip("|").strip()
            results.append(CaseLink(permit_id=match.group(1), project_number=project_number))
    return results


def parse_table_rows(
    doc: lxml.html.HtmlElement, panel_id: str
) -> list[list[str]]:
    """Generic helper to extract table rows from a panel."""
    panels = doc.xpath(f"//div[@id='{panel_id}']//table")
    if not panels:
        return []
    rows = panels[0].xpath(".//tbody/tr")
    return [[td.text_content().strip() for td in row.xpath("./td")] for row in rows]


def parse_site_addresses(doc: lxml.html.HtmlElement) -> list[SiteAddress]:
    rows = parse_table_rows(doc, "addressPanel")
    return [
        SiteAddress(
            address=r[0] if len(r) > 0 else "",
            suite=r[1] if len(r) > 1 else "",
            city=r[2] if len(r) > 2 else "",
            state=r[3] if len(r) > 3 else "",
            zip=r[4] if len(r) > 4 else "",
            parcel_id=r[5] if len(r) > 5 else "",
        )
        for r in rows
    ]


def parse_contacts(doc: lxml.html.HtmlElement) -> list[Contact]:
    rows = parse_table_rows(doc, "peoplePanel")
    return [
        Contact(name=r[0] if len(r) > 0 else "", role=r[1] if len(r) > 1 else "")
        for r in rows
    ]


def parse_contractors(doc: lxml.html.HtmlElement) -> list[Contractor]:
    # Contractors section doesn't have a consistent panel ID, find by heading
    headings = doc.xpath("//h5[@class='card-title mb-0'][contains(text(), 'Contractors')]")
    if not headings:
        return []
    card = headings[0].getparent().getparent()
    tables = card.xpath(".//table")
    if not tables:
        return []
    rows = tables[0].xpath(".//tbody/tr")
    results = []
    for row in rows:
        cells = [td.text_content().strip() for td in row.xpath("./td")]
        results.append(
            Contractor(
                business_name=cells[0] if len(cells) > 0 else "",
                contractor_type=cells[1] if len(cells) > 1 else "",
                city=cells[2] if len(cells) > 2 else "",
                state=cells[3] if len(cells) > 3 else "",
            )
        )
    return results


def parse_details(doc: lxml.html.HtmlElement) -> list[Detail]:
    rows = parse_table_rows(doc, "detailsPanel")
    return [
        Detail(
            category=r[0] if len(r) > 0 else "",
            description=r[1] if len(r) > 1 else "",
            data=r[2] if len(r) > 2 else "",
        )
        for r in rows
    ]


def parse_tasks(doc: lxml.html.HtmlElement) -> list[Task]:
    """Parse tasks, extracting task IDs from 'View Comments' links."""
    panels = doc.xpath("//div[@id='tasksPanel']//table")
    if not panels:
        return []

    tasks = []
    rows = panels[0].xpath(".//tbody/tr")
    for row in rows:
        cells = row.xpath("./td")
        cell_texts = [td.text_content().strip() for td in cells]

        # Extract task_id from the "View Comments" link if present
        task_id = None
        comment_links = row.xpath(".//a[contains(@class, 'taskId')]/@id")
        if comment_links:
            task_id = comment_links[0]

        tasks.append(Task(
            description=cell_texts[0] if len(cell_texts) > 0 else "",
            result=cell_texts[1] if len(cell_texts) > 1 else "",
            date_completed=cell_texts[2] if len(cell_texts) > 2 else "",
            completed_by=cell_texts[3] if len(cell_texts) > 3 else "",
            task_id=task_id,
            comments=[],  # Populated later by fetch_task_comments
        ))
    return tasks


def parse_inspections(doc: lxml.html.HtmlElement) -> list[Inspection]:
    rows = parse_table_rows(doc, "inspectionsPanel")
    return [
        Inspection(
            inspection_type=r[0] if len(r) > 0 else "",
            inspection_date=r[1] if len(r) > 1 else "",
            requested_by=r[2] if len(r) > 2 else "",
            status=r[3] if len(r) > 3 else "",
        )
        for r in rows
    ]


def parse_conditions(doc: lxml.html.HtmlElement) -> list[Condition]:
    rows = parse_table_rows(doc, "conditionsPanel")
    return [
        Condition(
            description=r[0] if len(r) > 0 else "",
            details=r[1] if len(r) > 1 else "",
            date_applied=r[2] if len(r) > 2 else "",
            date_completed=r[3] if len(r) > 3 else "",
        )
        for r in rows
    ]


def parse_flags(doc: lxml.html.HtmlElement) -> list[Flag]:
    rows = parse_table_rows(doc, "flagsPanel")
    return [
        Flag(
            description=r[0] if len(r) > 0 else "",
            date_applied=r[1] if len(r) > 1 else "",
            date_completed=r[2] if len(r) > 2 else "",
        )
        for r in rows
    ]


def parse_notes(doc: lxml.html.HtmlElement) -> list[str]:
    rows = parse_table_rows(doc, "notesPanel")
    return [r[0] for r in rows if r]


def parse_fees(doc: lxml.html.HtmlElement) -> list[Fee]:
    rows = parse_table_rows(doc, "feesPanel")
    return [
        Fee(
            description=r[0] if len(r) > 0 else "",
            amount=r[1] if len(r) > 1 else "",
            balance_due=r[2] if len(r) > 2 else "",
        )
        for r in rows
    ]


def parse_payments(doc: lxml.html.HtmlElement) -> list[Payment]:
    rows = parse_table_rows(doc, "paymentsPanel")
    return [
        Payment(
            description=r[0] if len(r) > 0 else "",
            fee_amount=r[1] if len(r) > 1 else "",
            payment_amount=r[2] if len(r) > 2 else "",
            payment_date=r[3] if len(r) > 3 else "",
            payment_method=r[4] if len(r) > 4 else "",
            reference=r[5] if len(r) > 5 else "",
        )
        for r in rows
    ]


def parse_attachments(doc: lxml.html.HtmlElement) -> list[Attachment]:
    container = doc.xpath("//div[@id='docDownloadContainer']")
    if not container:
        return []

    attachments = []
    cards = container[0].xpath(".//div[contains(@class, 'card')]")
    for card in cards:
        badge = card.xpath(".//span[contains(@class, 'badge')]")
        filename_span = card.xpath(".//span[@class='text-dark']")
        date_span = card.xpath(".//span[@class='small']")
        link = card.xpath(".//a[@href]")

        attachments.append(
            Attachment(
                attachment_type=badge[0].text_content().strip() if badge else "",
                filename=filename_span[0].text_content().strip() if filename_span else "",
                date=date_span[0].text_content().strip() if date_span else "",
                download_url=link[0].get("href", "") if link else "",
            )
        )
    return attachments


TASK_COMMENTS_URL = f"{BASE_URL}/PermitInfo/GetTaskComments"


async def fetch_task_comments(
    client: httpx.AsyncClient, task_id: str
) -> list[TaskComment]:
    """Fetch comments for a task via XHR endpoint."""
    try:
        resp = await client.post(TASK_COMMENTS_URL, data={"caTaskId": task_id})
        resp.raise_for_status()
        data = resp.json()
        return [
            TaskComment(
                text=c.get("COMMENT_TEXT", ""),
                date_created=c.get("DATE_CREATED", ""),
            )
            for c in data.get("comments", [])
        ]
    except Exception:
        # If comment fetch fails, return empty list rather than failing the whole permit
        return []


def is_transient_error(exc: BaseException) -> bool:
    if isinstance(exc, (httpx.ReadError, httpx.ReadTimeout, httpx.ConnectTimeout)):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500:
        return True
    return False


def get_cached_html(permit_id: str) -> bytes | None:
    """Load cached HTML if available."""
    cache_file = HTML_CACHE_DIR / f"{permit_id}.html"
    if cache_file.exists():
        return cache_file.read_bytes()
    return None


def save_html_cache(permit_id: str, content: bytes) -> None:
    """Save HTML to cache."""
    HTML_CACHE_DIR.mkdir(exist_ok=True)
    cache_file = HTML_CACHE_DIR / f"{permit_id}.html"
    cache_file.write_bytes(content)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(is_transient_error),
)
async def fetch_permit(
    client: httpx.AsyncClient, search_result: SearchResult, use_cache: bool = True
) -> Permit:
    url = f"{PERMIT_URL}?caObjectId={search_result.permit_id}"

    # Try cache first
    content = get_cached_html(search_result.permit_id) if use_cache else None

    if content is None:
        resp = await client.get(PERMIT_URL, params={"caObjectId": search_result.permit_id})
        resp.raise_for_status()
        content = resp.content
        save_html_cache(search_result.permit_id, content)

    doc = lxml.html.fromstring(content)

    # Parse tasks and fetch comments for each task with a task_id
    tasks = parse_tasks(doc)
    for task in tasks:
        if task.task_id:
            task.comments = await fetch_task_comments(client, task.task_id)

    return Permit(
        permit_id=search_result.permit_id,
        project_number=search_result.project_number,
        url=url,
        fetched_at=datetime.now(timezone.utc).isoformat(),
        search_result=search_result,
        info=parse_info(doc),
        parent_cases=parse_case_links(doc, "Parent Cases"),
        child_cases=parse_case_links(doc, "Child Cases"),
        site_addresses=parse_site_addresses(doc),
        contacts=parse_contacts(doc),
        contractors=parse_contractors(doc),
        details=parse_details(doc),
        tasks=tasks,
        inspections=parse_inspections(doc),
        conditions=parse_conditions(doc),
        flags=parse_flags(doc),
        notes=parse_notes(doc),
        fees=parse_fees(doc),
        payments=parse_payments(doc),
        attachments=parse_attachments(doc),
    )


async def fetch_permit_with_semaphore(
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    search_result: SearchResult,
    use_cache: bool = True,
) -> Permit:
    async with semaphore:
        return await fetch_permit(client, search_result, use_cache)


def load_fetched_ids(output_path: Path) -> set[str]:
    if not output_path.exists():
        return set()
    fetched = set()
    with open(output_path) as f:
        for line in f:
            if line.strip():
                permit = json.loads(line)
                fetched.add(permit["permit_id"])
    return fetched


async def main(
    start_date: date,
    end_date: date,
    output_path: Path,
    overwrite: bool,
    concurrency: int,
    no_cache: bool = False,
) -> None:
    load_dotenv()
    username = os.environ["PERMITS_USERNAME"]
    password = os.environ["PERMITS_PASSWORD"]

    if overwrite and output_path.exists():
        output_path.unlink()

    fetched_ids = load_fetched_ids(output_path)
    print(f"Already fetched: {len(fetched_ids)} permits")

    async with httpx.AsyncClient(timeout=60, headers=HEADERS, follow_redirects=True) as client:
        await login(client, username, password)
        print("Logged in successfully")

        print(f"Searching for permits from {start_date} to {end_date}...")
        search_results = await search_permits(client, start_date, end_date)
        print(f"Found {len(search_results)} unique permits")

        to_fetch = [r for r in search_results if r.permit_id not in fetched_ids]
        print(f"To fetch: {len(to_fetch)} permits")

        if not to_fetch:
            print("Nothing to fetch")
            return

        semaphore = asyncio.Semaphore(concurrency)
        use_cache = not no_cache
        tasks = [
            fetch_permit_with_semaphore(semaphore, client, sr, use_cache)
            for sr in to_fetch
        ]

        with open(output_path, "a") as f:
            for coro in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc="Fetching permits",
            ):
                permit = await coro
                f.write(permit.model_dump_json() + "\n")
                f.flush()

        print(f"Done. Total permits in {output_path}: {len(fetched_ids) + len(to_fetch)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Charlottesville permit data")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=date.fromisoformat, help="End date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--output", required=True, type=Path, help="Output JSONL file")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output file")
    parser.add_argument("--concurrency", type=int, default=10, help="Max concurrent requests")
    parser.add_argument("--no-cache", action="store_true", help="Bypass HTML cache and re-fetch")

    args = parser.parse_args()

    asyncio.run(main(
        args.start_date,
        args.end_date or date.today(),
        args.output,
        args.overwrite,
        args.concurrency,
        args.no_cache,
    ))
