"""Lead feeder: loads leads from miner_models/leads.json for the miner."""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

logging.getLogger("miner_models.feeder").setLevel(logging.DEBUG)

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
