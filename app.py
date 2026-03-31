"""
TrendPulse — Keyword Trend Monitor (Production)
Main FastAPI application for DPO International keyword monitoring.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import Optional
import uvicorn

from database import Database
from trends import fetch_google_trends
from scheduler import TrendScheduler

# ─── Logging ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("trendpulse")

# ─── Database & Scheduler ───────────────────────────────────────────────
db = Database()
scheduler = TrendScheduler(db)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start scheduler on app startup, stop on shutdown."""
    db.init()
    scheduler.start()
    logger.info("TrendPulse started — scheduler running")
    yield
    scheduler.stop()
    logger.info("TrendPulse stopped")

app = FastAPI(title="TrendPulse", version="1.0.0", lifespan=lifespan)

# ─── Static files ───────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

# ─── Models ─────────────────────────────────────────────────────────────

class KeywordCreate(BaseModel):
    term: str
    category: str = "Custom"
    threshold: int = 100
    country: str = "MY"
    platform: str = "google"
    active: bool = True

class KeywordUpdate(BaseModel):
    term: Optional[str] = None
    category: Optional[str] = None
    threshold: Optional[int] = None
    country: Optional[str] = None
    platform: Optional[str] = None
    active: Optional[bool] = None

class SettingsUpdate(BaseModel):
    check_interval: Optional[str] = None
    alert_channels: Optional[list] = None

class LinkedInDataPoint(BaseModel):
    keyword_id: int
    impressions: int = 0
    clicks: int = 0
    reactions: int = 0
    comments: int = 0
    shares: int = 0
    date: Optional[str] = None  # ISO date string, defaults to today

# ─── Routes: Dashboard ──────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the main dashboard."""
    return FileResponse("static/index.html")


# ─── Routes: Keywords ───────────────────────────────────────────────────

@app.get("/api/keywords")
async def list_keywords():
    """Get all tracked keywords."""
    keywords = db.get_keywords()
    return {"keywords": keywords}

@app.post("/api/keywords")
async def create_keyword(kw: KeywordCreate):
    """Add a new keyword to track."""
    keyword_id = db.add_keyword(
        term=kw.term,
        category=kw.category,
        threshold=kw.threshold,
        country=kw.country,
        platform=kw.platform,
        active=kw.active
    )
    # Trigger an immediate trend fetch for this keyword
    try:
        await fetch_and_store_single(keyword_id)
    except Exception as e:
        logger.warning(f"Initial fetch failed for '{kw.term}': {e}")
    return {"id": keyword_id, "message": f"Keyword '{kw.term}' added"}

@app.put("/api/keywords/{keyword_id}")
async def update_keyword(keyword_id: int, kw: KeywordUpdate):
    """Update a keyword's settings."""
    updates = {k: v for k, v in kw.dict().items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")
    db.update_keyword(keyword_id, **updates)
    return {"message": "Keyword updated"}

@app.delete("/api/keywords/{keyword_id}")
async def delete_keyword(keyword_id: int):
    """Remove a keyword and its data."""
    db.delete_keyword(keyword_id)
    return {"message": "Keyword deleted"}

@app.post("/api/keywords/{keyword_id}/toggle")
async def toggle_keyword(keyword_id: int):
    """Toggle a keyword's active state."""
    kw = db.get_keyword(keyword_id)
    if not kw:
        raise HTTPException(404, "Keyword not found")
    db.update_keyword(keyword_id, active=not kw["active"])
    return {"active": not kw["active"]}


# ─── Routes: Trend Data ────────────────────────────────────────────────

@app.get("/api/trends/{keyword_id}")
async def get_trends(keyword_id: int, days: int = Query(default=30, le=90)):
    """Get trend data for a specific keyword."""
    data = db.get_trend_data(keyword_id, days=days)
    return {"keyword_id": keyword_id, "data": data}

@app.get("/api/trends")
async def get_all_trends(days: int = Query(default=30, le=90)):
    """Get trend data for all keywords."""
    keywords = db.get_keywords()
    result = {}
    for kw in keywords:
        data = db.get_trend_data(kw["id"], days=days)
        result[kw["id"]] = data
    return {"trends": result}

@app.post("/api/trends/refresh")
async def refresh_trends():
    """Manually trigger a trend data refresh for all active keywords."""
    try:
        count = await run_full_refresh()
        return {"message": f"Refreshed {count} keywords"}
    except Exception as e:
        logger.error(f"Refresh failed: {e}")
        raise HTTPException(500, f"Refresh failed: {str(e)}")


# ─── Routes: Alerts ─────────────────────────────────────────────────────

@app.get("/api/alerts")
async def get_alerts(limit: int = Query(default=50, le=200)):
    """Get recent alerts."""
    alerts = db.get_alerts(limit=limit)
    return {"alerts": alerts}

@app.delete("/api/alerts/{alert_id}")
async def dismiss_alert(alert_id: int):
    """Dismiss a single alert."""
    db.dismiss_alert(alert_id)
    return {"message": "Alert dismissed"}

@app.delete("/api/alerts")
async def clear_alerts():
    """Clear all alerts."""
    db.clear_alerts()
    return {"message": "All alerts cleared"}


# ─── Routes: LinkedIn Manual Entry ──────────────────────────────────────

@app.post("/api/linkedin/data")
async def add_linkedin_data(entry: LinkedInDataPoint):
    """Manually log LinkedIn post performance data for a keyword."""
    kw = db.get_keyword(entry.keyword_id)
    if not kw:
        raise HTTPException(404, "Keyword not found")
    if kw["platform"] != "linkedin":
        raise HTTPException(400, "Keyword is not a LinkedIn keyword")
    
    date = entry.date or datetime.utcnow().strftime("%Y-%m-%d")
    engagement = entry.reactions + entry.comments + entry.shares
    
    # Store as trend data point (using engagement as the "volume")
    db.add_trend_point(
        keyword_id=entry.keyword_id,
        value=engagement,
        date=date,
        meta=json.dumps({
            "impressions": entry.impressions,
            "clicks": entry.clicks,
            "reactions": entry.reactions,
            "comments": entry.comments,
            "shares": entry.shares,
            "source": "linkedin_manual"
        })
    )
    
    # Check threshold
    if engagement >= kw["threshold"]:
        pct_over = round(((engagement - kw["threshold"]) / kw["threshold"]) * 100)
        db.add_alert(
            keyword_id=entry.keyword_id,
            severity="high" if pct_over > 50 else "medium",
            message=f"LinkedIn engagement reached {engagement} ({pct_over}% above threshold of {kw['threshold']}). Impressions: {entry.impressions}, Clicks: {entry.clicks}."
        )
    
    return {"message": "LinkedIn data recorded", "engagement": engagement}


# ─── Routes: Settings ───────────────────────────────────────────────────

@app.get("/api/settings")
async def get_settings():
    """Get current app settings."""
    return db.get_settings()

@app.put("/api/settings")
async def update_settings(settings: SettingsUpdate):
    """Update app settings."""
    updates = {k: v for k, v in settings.dict().items() if v is not None}
    if "alert_channels" in updates:
        updates["alert_channels"] = json.dumps(updates["alert_channels"])
    db.update_settings(**updates)
    
    # Restart scheduler if interval changed
    if "check_interval" in updates:
        scheduler.restart_with_interval(updates["check_interval"])
    
    return {"message": "Settings updated"}


# ─── Routes: Status ─────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    """Get app status info."""
    keywords = db.get_keywords()
    active_count = sum(1 for k in keywords if k["active"])
    alerts = db.get_alerts(limit=100)
    critical_count = sum(1 for a in alerts if a["severity"] == "high")
    settings = db.get_settings()
    last_refresh = db.get_last_refresh_time()
    
    return {
        "tracking": active_count,
        "total_keywords": len(keywords),
        "critical_alerts": critical_count,
        "total_alerts": len(alerts),
        "check_interval": settings.get("check_interval", "6h"),
        "last_refresh": last_refresh,
        "scheduler_running": scheduler.is_running
    }


# ─── Helper: Fetch & Store ─────────────────────────────────────────────

async def fetch_and_store_single(keyword_id: int):
    """Fetch trend data for a single keyword and store it."""
    kw = db.get_keyword(keyword_id)
    if not kw or kw["platform"] != "google":
        return  # Only Google Trends is auto-fetched for now
    
    data = fetch_google_trends(kw["term"], kw["country"])
    if data:
        for point in data:
            db.add_trend_point(
                keyword_id=keyword_id,
                value=point["value"],
                date=point["date"]
            )
        # Check latest value against threshold
        latest = data[-1]["value"] if data else 0
        if latest >= kw["threshold"]:
            pct_over = round(((latest - kw["threshold"]) / kw["threshold"]) * 100)
            db.add_alert(
                keyword_id=keyword_id,
                severity="high" if pct_over > 50 else "medium",
                message=f"Google Trends interest for '{kw['term']}' reached {latest} ({pct_over}% above threshold of {kw['threshold']}) in {kw['country']}."
            )

async def run_full_refresh():
    """Refresh all active Google Trends keywords."""
    keywords = db.get_keywords()
    active_google = [k for k in keywords if k["active"] and k["platform"] == "google"]
    count = 0
    for kw in active_google:
        try:
            await fetch_and_store_single(kw["id"])
            count += 1
        except Exception as e:
            logger.warning(f"Failed to refresh '{kw['term']}': {e}")
    db.set_last_refresh_time(datetime.utcnow().isoformat())
    return count


# ─── Main ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
