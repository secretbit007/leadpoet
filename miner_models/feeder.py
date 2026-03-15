"""Lead feeder: loads leads from miner_models/leads.json for the miner."""

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set

logger = logging.getLogger(__name__)
logging.getLogger("miner_models.feeder").setLevel(logging.DEBUG)

SUBMITTED_LEADS_FILENAME = "submitted_leads.json"
_submitted_lock = threading.Lock()

# Required string fields (must be present and non-empty)
REQUIRED_STRING_FIELDS = [
    "business",
    "full_name",
    "first",
    "last",
    "email",
    "role",
    "website",
    "industry",
    "sub_industry",
    "country",
    "city",
    "linkedin",
    "company_linkedin",
    "source_url",
    "description",
    "employee_count",
    "hq_country",
]

VALID_EMPLOYEE_COUNTS = {
    "0-1",
    "2-10",
    "11-50",
    "51-200",
    "201-500",
    "501-1,000",
    "1,001-5,000",
    "5,001-10,000",
    "10,001+",
}

# Country values that mean US (for state / hq_state requirement)
US_COUNTRY_VARIANTS = {"united states", "us", "usa"}


def _is_us_country(value: str) -> bool:
    return (value or "").strip().lower() in US_COUNTRY_VARIANTS


def _nonempty_string(val: Any) -> bool:
    return isinstance(val, str) and len(val.strip()) > 0


def _is_url_or_proprietary(val: str) -> bool:
    s = (val or "").strip().lower()
    return s == "proprietary_database" or s.startswith("http://") or s.startswith("https://")


def validate_lead(lead: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate a lead against required fields and format rules.
    Returns (is_valid, list of error messages).
    """
    errors: List[str] = []

    for field in REQUIRED_STRING_FIELDS:
        val = lead.get(field)
        if not _nonempty_string(val):
            errors.append(f"missing or empty required field: {field}")
            continue
        if field == "employee_count":
            if val.strip() not in VALID_EMPLOYEE_COUNTS:
                errors.append(
                    f"employee_count must be one of {sorted(VALID_EMPLOYEE_COUNTS)}; got: {val!r}"
                )
        elif field == "source_url":
            if not _is_url_or_proprietary(val):
                errors.append(
                    'source_url must be a URL (http(s)://...) or "proprietary_database"; got: {!r}'.format(
                        val[:80] + "..." if len(val) > 80 else val
                    )
                )

    country = (lead.get("country") or "").strip()
    state = (lead.get("state") or "").strip()
    if _is_us_country(country) and not state:
        errors.append("state is required for US leads (country is US)")

    hq_country = (lead.get("hq_country") or "").strip()
    hq_state = (lead.get("hq_state") or "").strip()
    if _is_us_country(hq_country) and not hq_state:
        errors.append("hq_state is required for US companies (hq_country is US)")

    return (len(errors) == 0, errors)


def _leads_path() -> Path:
    return Path(__file__).resolve().parent / "leads.json"


def _submitted_path() -> Path:
    return Path(__file__).resolve().parent / SUBMITTED_LEADS_FILENAME


def _load_submitted() -> Set[str]:
    """Return set of submitted lead emails (lowercase) from submitted_leads.json."""
    path = _submitted_path()
    if not path.exists():
        return set()
    with _submitted_lock:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("could not load submitted leads from %s: %s", path, e)
            return set()
    if not isinstance(data, list):
        logger.warning("submitted_leads.json root is not a list, ignoring")
        return set()
    return {r.get("email", "").strip().lower() for r in data if r.get("email")}


def _add_submitted_emails(emails: List[str]) -> None:
    """Append new submitted emails to submitted_leads.json (skips already present)."""
    if not emails:
        return
    path = _submitted_path()
    now = datetime.now(timezone.utc).isoformat()
    with _submitted_lock:
        try:
            existing = json.load(open(path, "r", encoding="utf-8")) if path.exists() else []
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("could not read submitted leads from %s: %s", path, e)
            existing = []
        if not isinstance(existing, list):
            existing = []
        existing_set = {r.get("email", "").strip().lower() for r in existing if r.get("email")}
        added = 0
        for email in emails:
            e = (email or "").strip()
            if not e:
                continue
            e_lower = e.lower()
            if e_lower in existing_set:
                continue
            existing.append({"email": e_lower, "submitted_at": now})
            existing_set.add(e_lower)
            added += 1
        if added:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(existing, f, indent=2)
                logger.info("recorded %d new submitted lead(s) in %s", added, path)
            except OSError as e:
                logger.exception("failed to write submitted leads to %s: %s", path, e)


def mark_lead_submitted(lead: Dict[str, Any]) -> None:
    """Record a single lead as submitted (by email). Call after successfully submitting."""
    email = (lead.get("email") or "").strip()
    if email:
        _add_submitted_emails([email])


def mark_leads_submitted(leads: List[Dict[str, Any]]) -> None:
    """Record multiple leads as submitted (by email). Call after successfully submitting."""
    emails = [(lead.get("email") or "").strip() for lead in leads if (lead.get("email") or "").strip()]
    _add_submitted_emails(emails)


def _load_leads() -> List[Dict[str, Any]]:
    path = _leads_path()
    if not path.exists():
        logger.warning("leads file not found: %s", path)
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            leads = json.load(f)
        if not isinstance(leads, list):
            logger.error("leads.json root is not a list (got %s)", type(leads).__name__)
            return []
        logger.info("loaded %d leads from %s", len(leads), path)
        return leads
    except json.JSONDecodeError as e:
        logger.exception("invalid JSON in %s: %s", path, e)
        return []
    except OSError as e:
        logger.exception("failed to read %s: %s", path, e)
        return []


async def get_leads(
    count: int,
    industry: Optional[str] = None,
    region: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return up to `count` leads from miner_models/leads.json.
    Only leads that pass required-field validation are returned.
    Optionally filter by industry and region (country).
    """
    all_leads = _load_leads()
    valid_leads: List[Dict[str, Any]] = []
    invalid_count = 0
    for i, lead in enumerate(all_leads):
        ok, errs = validate_lead(lead)
        if ok:
            valid_leads.append(lead)
        else:
            invalid_count += 1
            ident = lead.get("email") or lead.get("business") or f"index_{i}"
            logger.debug(
                "lead validation failed for %s: %s",
                ident,
                "; ".join(errs),
            )
    if invalid_count:
        logger.warning(
            "feeder: %d/%d leads failed validation (see debug log for details)",
            invalid_count,
            len(all_leads),
        )

    before_submitted = len(valid_leads)
    submitted_emails = _load_submitted()
    if submitted_emails:
        logger.debug("excluding already-submitted leads (tracked: %d)", len(submitted_emails))
    valid_leads = [
        l
        for l in valid_leads
        if (l.get("email") or "").strip().lower() not in submitted_emails
    ]
    skipped_submitted = before_submitted - len(valid_leads)
    if skipped_submitted:
        logger.info("skipped %d lead(s) already submitted", skipped_submitted)

    before_filter = len(valid_leads)
    if industry:
        industry_lower = industry.lower()
        valid_leads = [
            l
            for l in valid_leads
            if (l.get("industry") or "").strip().lower() == industry_lower
        ]
    if region:
        region_lower = region.lower()
        valid_leads = [
            l
            for l in valid_leads
            if (l.get("country") or l.get("hq_country") or "").strip().lower()
            == region_lower
        ]
    after_filter = len(valid_leads)
    result = valid_leads[:count]
    logger.info(
        "get_leads(count=%s, industry=%s, region=%s): loaded=%d valid=%d after_filter=%d returning=%d",
        count,
        industry,
        region,
        len(all_leads),
        before_filter,
        after_filter,
        len(result),
    )
    return result
