"""
TrendPulse — Database Layer
SQLite-based storage for keywords, trend data, alerts, and settings.
"""

import sqlite3
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("trendpulse.db")

DB_PATH = os.environ.get("TRENDPULSE_DB", "trendpulse.db")


class Database:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    # ─── Initialisation ─────────────────────────────────────────────────

    def init(self):
        """Create tables if they don't exist, seed default data."""
        conn = self._conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                term TEXT NOT NULL,
                category TEXT DEFAULT 'Custom',
                threshold INTEGER DEFAULT 100,
                country TEXT DEFAULT 'MY',
                platform TEXT DEFAULT 'google',
                active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS trend_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword_id INTEGER NOT NULL,
                value INTEGER NOT NULL,
                date TEXT NOT NULL,
                meta TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_trend_keyword_date ON trend_data(keyword_id, date);

            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword_id INTEGER NOT NULL,
                severity TEXT DEFAULT 'medium',
                message TEXT NOT NULL,
                dismissed INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at DESC);

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)

        # Seed default settings if empty
        cursor = conn.execute("SELECT COUNT(*) FROM settings")
        if cursor.fetchone()[0] == 0:
            defaults = {
                "check_interval": "6h",
                "alert_channels": json.dumps(["email"]),
                "last_refresh": "",
            }
            for k, v in defaults.items():
                conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (k, v))

        # Seed default keywords if empty
        cursor = conn.execute("SELECT COUNT(*) FROM keywords")
        if cursor.fetchone()[0] == 0:
            self._seed_default_keywords(conn)

        conn.commit()
        conn.close()
        logger.info(f"Database initialised at {self.db_path}")

    def _seed_default_keywords(self, conn):
        """Seed the default DPO-relevant keywords."""
        defaults = [
            ("food ingredients supplier", "Industry", 80, "MY", "google"),
            ("food manufacturing Malaysia", "Industry", 75, "MY", "google"),
            ("halal food ingredients", "Industry", 90, "MY", "google"),
            ("plant-based protein", "Tech", 85, "SG", "google"),
            ("low glycaemic sweetener", "Product", 70, "MY", "google"),
            ("food distribution ASEAN", "Industry", 80, "SG", "google"),
            ("cold chain logistics", "Industry", 75, "TH", "google"),
            ("DPO International", "Marketing", 50, "MY", "google"),
            ("food innovation Asia", "Tech", 80, "SG", "google"),
            ("bakery ingredients wholesale", "Product", 70, "MY", "google"),
            ("food service suppliers", "Industry", 80, "MY", "linkedin"),
            ("food manufacturing trends", "Marketing", 70, "MY", "linkedin"),
        ]
        for term, category, threshold, country, platform in defaults:
            conn.execute(
                "INSERT INTO keywords (term, category, threshold, country, platform) VALUES (?, ?, ?, ?, ?)",
                (term, category, threshold, country, platform)
            )

    # ─── Keywords ───────────────────────────────────────────────────────

    def get_keywords(self):
        conn = self._conn()
        rows = conn.execute("SELECT * FROM keywords ORDER BY id").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_keyword(self, keyword_id: int):
        conn = self._conn()
        row = conn.execute("SELECT * FROM keywords WHERE id = ?", (keyword_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def add_keyword(self, term, category="Custom", threshold=100, country="MY", platform="google", active=True):
        conn = self._conn()
        cursor = conn.execute(
            "INSERT INTO keywords (term, category, threshold, country, platform, active) VALUES (?, ?, ?, ?, ?, ?)",
            (term, category, threshold, country, platform, int(active))
        )
        conn.commit()
        keyword_id = cursor.lastrowid
        conn.close()
        return keyword_id

    def update_keyword(self, keyword_id: int, **kwargs):
        conn = self._conn()
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k == "active":
                v = int(v)
            sets.append(f"{k} = ?")
            vals.append(v)
        vals.append(keyword_id)
        conn.execute(f"UPDATE keywords SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit()
        conn.close()

    def delete_keyword(self, keyword_id: int):
        conn = self._conn()
        conn.execute("DELETE FROM keywords WHERE id = ?", (keyword_id,))
        conn.commit()
        conn.close()

    # ─── Trend Data ─────────────────────────────────────────────────────

    def add_trend_point(self, keyword_id: int, value: int, date: str, meta: str = None):
        conn = self._conn()
        # Upsert: replace if same keyword+date exists
        conn.execute("""
            INSERT INTO trend_data (keyword_id, value, date, meta)
            VALUES (?, ?, ?, ?)
            ON CONFLICT DO NOTHING
        """, (keyword_id, value, date, meta))
        conn.commit()
        conn.close()

    def get_trend_data(self, keyword_id: int, days: int = 30):
        conn = self._conn()
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT date, value, meta FROM trend_data
            WHERE keyword_id = ? AND date >= ?
            ORDER BY date ASC
        """, (keyword_id, cutoff)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ─── Alerts ─────────────────────────────────────────────────────────

    def add_alert(self, keyword_id: int, severity: str, message: str):
        conn = self._conn()
        conn.execute(
            "INSERT INTO alerts (keyword_id, severity, message) VALUES (?, ?, ?)",
            (keyword_id, severity, message)
        )
        conn.commit()
        conn.close()

    def get_alerts(self, limit: int = 50):
        conn = self._conn()
        rows = conn.execute("""
            SELECT a.*, k.term, k.country, k.platform
            FROM alerts a
            JOIN keywords k ON a.keyword_id = k.id
            WHERE a.dismissed = 0
            ORDER BY a.created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def dismiss_alert(self, alert_id: int):
        conn = self._conn()
        conn.execute("UPDATE alerts SET dismissed = 1 WHERE id = ?", (alert_id,))
        conn.commit()
        conn.close()

    def clear_alerts(self):
        conn = self._conn()
        conn.execute("UPDATE alerts SET dismissed = 1")
        conn.commit()
        conn.close()

    # ─── Settings ───────────────────────────────────────────────────────

    def get_settings(self):
        conn = self._conn()
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        conn.close()
        result = {}
        for r in rows:
            val = r["value"]
            try:
                val = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                pass
            result[r["key"]] = val
        return result

    def update_settings(self, **kwargs):
        conn = self._conn()
        for k, v in kwargs.items():
            if isinstance(v, (list, dict)):
                v = json.dumps(v)
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (k, str(v))
            )
        conn.commit()
        conn.close()

    def get_last_refresh_time(self):
        conn = self._conn()
        row = conn.execute("SELECT value FROM settings WHERE key = 'last_refresh'").fetchone()
        conn.close()
        return row["value"] if row else None

    def set_last_refresh_time(self, time_str: str):
        self.update_settings(last_refresh=time_str)
