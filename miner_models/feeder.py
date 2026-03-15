"""Lead feeder: loads leads from miner_models/leads.json for the miner."""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional


def _leads_path() -> Path:
    return Path(__file__).resolve().parent / "leads.json"


def _load_leads() -> List[Dict[str, Any]]:
    path = _leads_path()
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


async def get_leads(
    count: int,
    industry: Optional[str] = None,
    region: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return up to `count` leads from miner_models/leads.json.
    Optionally filter by industry and region (country).
    """
    leads = _load_leads()
    if industry:
        industry_lower = industry.lower()
        leads = [l for l in leads if (l.get("industry") or "").lower() == industry_lower]
    if region:
        region_lower = region.lower()
        leads = [
            l
            for l in leads
            if (l.get("country") or l.get("hq_country") or "").lower() == region_lower
        ]
    return leads[:count]
