"""
TrendPulse — Background Scheduler
Runs periodic Google Trends data fetches using APScheduler.
"""

import logging
import json
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

from trends import fetch_google_trends

logger = logging.getLogger("trendpulse.scheduler")

# Map interval strings to minutes
INTERVAL_MAP = {
    "15m": 15,
    "1h": 60,
    "6h": 360,
    "12h": 720,
    "24h": 1440,
}


class TrendScheduler:
    def __init__(self, db):
        self.db = db
        self.scheduler = BackgroundScheduler()
        self.is_running = False
        self.job_id = "trend_refresh"

    def start(self):
        """Start the scheduler with the configured interval."""
        settings = self.db.get_settings()
        interval_str = settings.get("check_interval", "6h")
        minutes = INTERVAL_MAP.get(interval_str, 360)

        self.scheduler.add_job(
            self._refresh_all,
            "interval",
            minutes=minutes,
            id=self.job_id,
            replace_existing=True,
            next_run_time=None  # Don't run immediately on start; let user trigger first
        )
        self.scheduler.start()
        self.is_running = True
        logger.info(f"Scheduler started — checking every {interval_str} ({minutes} min)")

    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        self.is_running = False
        logger.info("Scheduler stopped")

    def restart_with_interval(self, interval_str: str):
        """Restart the scheduler with a new interval."""
        minutes = INTERVAL_MAP.get(interval_str, 360)
        try:
            self.scheduler.reschedule_job(
                self.job_id,
                trigger="interval",
                minutes=minutes
            )
            logger.info(f"Scheduler rescheduled to every {interval_str} ({minutes} min)")
        except Exception as e:
            logger.warning(f"Failed to reschedule: {e}")

    def _refresh_all(self):
        """Fetch fresh Google Trends data for all active keywords."""
        logger.info("Scheduled refresh started")
        keywords = self.db.get_keywords()
        active_google = [k for k in keywords if k["active"] and k["platform"] == "google"]

        success_count = 0
        for kw in active_google:
            try:
                data = fetch_google_trends(kw["term"], kw["country"])
                if data:
                    for point in data:
                        self.db.add_trend_point(
                            keyword_id=kw["id"],
                            value=point["value"],
                            date=point["date"]
                        )

                    # Check latest value against threshold
                    latest = data[-1]["value"]
                    if latest >= kw["threshold"]:
                        pct_over = round(((latest - kw["threshold"]) / kw["threshold"]) * 100)
                        self.db.add_alert(
                            keyword_id=kw["id"],
                            severity="high" if pct_over > 50 else "medium",
                            message=(
                                f"Google Trends interest for '{kw['term']}' reached {latest} "
                                f"({pct_over}% above threshold of {kw['threshold']}) in {kw['country']}."
                            )
                        )

                    success_count += 1
            except Exception as e:
                logger.warning(f"Failed to refresh '{kw['term']}': {e}")

        self.db.set_last_refresh_time(datetime.utcnow().isoformat())
        logger.info(f"Scheduled refresh complete: {success_count}/{len(active_google)} keywords updated")
