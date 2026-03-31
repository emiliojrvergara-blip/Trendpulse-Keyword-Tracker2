"""
TrendPulse — Google Trends Fetcher
Pulls real keyword interest data using pytrends (unofficial Google Trends API).

Rate limiting:
- pytrends can get rate-limited by Google if too many requests are made.
- This module includes retry logic with exponential backoff.
- For production use with heavy load, consider upgrading to SerpApi.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("trendpulse.trends")

# Country code mapping for pytrends geo parameter
GEO_MAP = {
    "MY": "MY",
    "SG": "SG",
    "ID": "ID",
    "TH": "TH",
    "VN": "VN",
    "PH": "PH",
    "CN": "CN",
    "HK": "HK",
    "LK": "LK",
    "": "",  # global
}


def fetch_google_trends(
    keyword: str,
    country: str = "MY",
    timeframe: str = "today 3-m",
    max_retries: int = 3
) -> list:
    """
    Fetch Google Trends interest-over-time data for a keyword.
    
    Args:
        keyword: The search term to track
        country: ISO country code (MY, SG, ID, etc.)
        timeframe: pytrends timeframe string (default: last 3 months)
        max_retries: Number of retry attempts on failure
    
    Returns:
        List of {"date": "YYYY-MM-DD", "value": int} dicts,
        or empty list on failure.
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        logger.error("pytrends not installed. Run: pip install pytrends")
        return []

    geo = GEO_MAP.get(country, country)

    for attempt in range(max_retries):
        try:
            # Create a new session each attempt to avoid stale cookies
            pytrends = TrendReq(
                hl="en-US",
                tz=480,  # UTC+8 (Malaysia)
                retries=2,
                backoff_factor=1.0
            )

            pytrends.build_payload(
                kw_list=[keyword],
                cat=0,
                timeframe=timeframe,
                geo=geo,
                gprop=""
            )

            df = pytrends.interest_over_time()

            if df is None or df.empty:
                logger.info(f"No data returned for '{keyword}' in {country}")
                return []

            # Drop the 'isPartial' column if present
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])

            # Convert to our standard format
            data = []
            for idx, row in df.iterrows():
                data.append({
                    "date": idx.strftime("%Y-%m-%d"),
                    "value": int(row[keyword])
                })

            logger.info(f"Fetched {len(data)} data points for '{keyword}' ({country})")
            return data

        except Exception as e:
            wait_time = (2 ** attempt) * 5  # 5s, 10s, 20s
            logger.warning(
                f"Attempt {attempt + 1}/{max_retries} failed for '{keyword}': {e}. "
                f"Retrying in {wait_time}s..."
            )
            if attempt < max_retries - 1:
                time.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} attempts failed for '{keyword}' ({country})")
                return []

    return []


def fetch_related_queries(keyword: str, country: str = "MY") -> dict:
    """
    Fetch related queries for a keyword (useful for discovering new terms).
    
    Returns:
        {"rising": [...], "top": [...]} or empty dict on failure.
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        return {}

    geo = GEO_MAP.get(country, country)

    try:
        pytrends = TrendReq(hl="en-US", tz=480)
        pytrends.build_payload(kw_list=[keyword], geo=geo, timeframe="today 3-m")
        related = pytrends.related_queries()

        result = {"rising": [], "top": []}

        if keyword in related:
            rising_df = related[keyword].get("rising")
            top_df = related[keyword].get("top")

            if rising_df is not None and not rising_df.empty:
                result["rising"] = rising_df.head(10).to_dict("records")
            if top_df is not None and not top_df.empty:
                result["top"] = top_df.head(10).to_dict("records")

        return result

    except Exception as e:
        logger.warning(f"Failed to fetch related queries for '{keyword}': {e}")
        return {}
