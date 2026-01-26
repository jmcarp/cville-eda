#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pydantic",
# ]
# ///
"""Summarize a project using Claude to analyze each permit."""

import argparse
import hashlib
import subprocess
from pathlib import Path

from models import Permit
from permit_utils import (
    load_permits,
    load_parcel_zones,
    find_permits_by_address,
    find_related_permits,
)

SUMMARY_CACHE_DIR = Path(__file__).parent / "summary_cache"


def claude_prompt(prompt: str) -> str:
    """Run a prompt through claude -p."""
    result = subprocess.run(
        ["claude", "-p", prompt],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def get_permit_cache_key(permit: Permit) -> str:
    """Generate a cache key based on permit content."""
    # Hash the permit JSON so cache invalidates if permit data changes
    permit_json = permit.model_dump_json()
    content_hash = hashlib.sha256(permit_json.encode()).hexdigest()[:12]
    return f"{permit.permit_id}_{content_hash}"


def get_cached_summary(permit: Permit) -> str | None:
    """Load cached summary if available."""
    cache_key = get_permit_cache_key(permit)
    cache_file = SUMMARY_CACHE_DIR / f"{cache_key}.txt"
    if cache_file.exists():
        return cache_file.read_text()
    return None


def save_summary_cache(permit: Permit, summary: str) -> None:
    """Save summary to cache."""
    SUMMARY_CACHE_DIR.mkdir(exist_ok=True)
    cache_key = get_permit_cache_key(permit)
    cache_file = SUMMARY_CACHE_DIR / f"{cache_key}.txt"
    cache_file.write_text(summary)


def summarize_permit(permit: Permit, use_cache: bool = True) -> str:
    """Ask Claude to summarize a single permit."""
    if use_cache:
        cached = get_cached_summary(permit)
        if cached:
            return cached

    permit_json = permit.model_dump_json(indent=2)
    prompt = f"""Summarize this permit in 2-3 sentences based ONLY on the data provided below.

Rules:
- Only state facts explicitly present in the JSON data
- Do not infer, speculate, or add information not in the data
- If a field is empty or missing, do not guess what it might contain
- For reviewer comments, summarize what reviewers actually wrote
- Use "unknown" or omit details rather than making assumptions

Focus on: permit type, current status, key dates, unit counts, job value, and any
reviewer comments that indicate issues or requirements.

{permit_json}"""
    summary = claude_prompt(prompt)
    save_summary_cache(permit, summary)
    return summary


def summarize_summaries(
    address: str, summaries: list[tuple[str, str]], zones: set[str]
) -> str:
    """Ask Claude to create a final summary from individual permit summaries."""
    summaries_text = "\n\n".join(
        f"**Permit {pid}:**\n{summary}" for pid, summary in summaries
    )

    zone_info = f"Zoning: {', '.join(sorted(zones))}" if zones else ""

    prompt = f"""Here are summaries of {len(summaries)} permits related to {address}.
{zone_info}

Write a comprehensive project summary that explains:
1. What is being built
2. The timeline and current status
3. Key milestones and approvals
4. Any notable details

Individual permit summaries:

{summaries_text}"""
    return claude_prompt(prompt)


def main():
    parser = argparse.ArgumentParser(description="Summarize a project using Claude")
    parser.add_argument("address", help="Address to search for")
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
        "--output-dir",
        "-o",
        type=Path,
        help="Directory to write summaries (default: stdout only)",
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Regenerate summaries even if cached",
    )
    args = parser.parse_args()

    permits = load_permits(args.data)
    parcel_zones = load_parcel_zones(args.parcels)
    matches = find_permits_by_address(permits, args.address)

    if not matches:
        print(f"No permits found matching '{args.address}'")
        return 1

    start_ids = [p.permit_id for p in matches]
    related_ids = find_related_permits(start_ids, permits)
    related = [permits[pid] for pid in related_ids if pid in permits]
    related.sort(key=lambda p: p.search_result.date_created)

    print(f"Found {len(related)} related permits for '{args.address}'")
    print("Summarizing each permit...\n")

    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)

    use_cache = not args.regenerate
    summaries = []
    for i, permit in enumerate(related, 1):
        cached = get_cached_summary(permit) if use_cache else None
        status = "cached" if cached else "generating"
        print(f"[{i}/{len(related)}] {permit.permit_id} ({status})...")
        summary = summarize_permit(permit, use_cache)
        summaries.append((permit.permit_id, summary))
        print(f"  {summary[:100]}...")

        if args.output_dir:
            permit_file = args.output_dir / f"{permit.permit_id}.md"
            permit_file.write_text(f"# Permit {permit.permit_id}\n\n{summary}\n")

    print("\n" + "=" * 60)
    print("Generating final summary...\n")

    # Collect zones from related permits
    zones = set()
    for permit in related:
        parcel = permit.search_result.parcel_number
        if parcel and parcel in parcel_zones:
            zones.add(parcel_zones[parcel])

    final_summary = summarize_summaries(args.address, summaries, zones)
    print(final_summary)

    if args.output_dir:
        # Write all per-permit summaries to one file
        all_summaries = "\n\n".join(
            f"## Permit {pid}\n\n{summary}" for pid, summary in summaries
        )
        (args.output_dir / "permit_summaries.md").write_text(
            f"# Per-Permit Summaries: {args.address}\n\n{all_summaries}\n"
        )

        # Write final summary
        (args.output_dir / "project_summary.md").write_text(final_summary + "\n")

        print(f"\nWrote summaries to {args.output_dir}/")

    return 0


if __name__ == "__main__":
    exit(main())
