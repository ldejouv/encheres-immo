"""Shared progress tracker for scraping jobs.

Writes/reads a JSON file so that the dashboard (Streamlit)
can monitor a scraper running in a separate process.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Optional

_PROGRESS_DIR = Path(__file__).resolve().parent.parent / "data"
_PROGRESS_FILE = _PROGRESS_DIR / "scrape_progress.json"
_CANCEL_FILE = _PROGRESS_DIR / "scrape_cancel.flag"


def _now_ts() -> float:
    return time.time()


def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m}m {s:02d}s"


# ── Writer (used by scraper processes) ──────────────────────────────

class ProgressWriter:
    """Call from the scraper to report progress to disk."""

    def __init__(self, job_type: str, total: int):
        self.job_type = job_type
        self.total = total
        self.processed = 0
        self.updated = 0
        self.errors = 0
        self.not_found = 0
        self.started_at = _now_ts()
        self.current_item = ""
        # Clear any leftover cancel flag from a previous run
        _clear_cancel_flag()
        self._flush()

    def tick(
        self,
        *,
        updated: bool = False,
        error: bool = False,
        not_found: bool = False,
        current_item: str = "",
    ):
        self.processed += 1
        if updated:
            self.updated += 1
        if error:
            self.errors += 1
        if not_found:
            self.not_found += 1
        if current_item:
            self.current_item = current_item
        # Flush every tick — file is small, cheap to write
        self._flush()

    def finish(self, status: str = "finished"):
        self._flush(status=status)

    def cancel(self):
        """Mark the job as cancelled by the user."""
        self._flush(status="cancelled")

    def abort(self, reason: str = ""):
        self._flush(status="error", error_message=reason)

    @staticmethod
    def is_cancel_requested() -> bool:
        """Check if the user has requested cancellation."""
        return _CANCEL_FILE.exists()

    def _flush(self, status: str = "running", error_message: str = ""):
        elapsed = _now_ts() - self.started_at
        remaining = self.total - self.processed
        speed = self.processed / elapsed if elapsed > 0 else 0
        eta_seconds = remaining / speed if speed > 0 else 0

        data = {
            "job_type": self.job_type,
            "status": status,
            "pid": os.getpid(),
            "started_at": self.started_at,
            "elapsed_seconds": elapsed,
            "elapsed_fmt": _fmt_duration(elapsed),
            "total": self.total,
            "processed": self.processed,
            "updated": self.updated,
            "errors": self.errors,
            "not_found": self.not_found,
            "remaining": remaining,
            "progress_pct": (self.processed / self.total * 100) if self.total else 0,
            "speed_per_min": speed * 60,
            "eta_seconds": eta_seconds,
            "eta_fmt": _fmt_duration(eta_seconds),
            "current_item": self.current_item,
            "error_message": error_message,
        }
        _PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
        tmp = _PROGRESS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(_PROGRESS_FILE)


# ── Reader (used by Streamlit dashboard) ────────────────────────────

def read_progress() -> Optional[dict]:
    """Read current scraper progress. Returns None if no job data."""
    if not _PROGRESS_FILE.exists():
        return None
    try:
        data = json.loads(_PROGRESS_FILE.read_text(encoding="utf-8"))
        # Recompute elapsed for live display
        if data.get("status") == "running":
            data["elapsed_seconds"] = _now_ts() - data["started_at"]
            data["elapsed_fmt"] = _fmt_duration(data["elapsed_seconds"])
            # Recompute ETA
            speed = data["processed"] / data["elapsed_seconds"] if data["elapsed_seconds"] > 0 else 0
            remaining = data["total"] - data["processed"]
            data["eta_seconds"] = remaining / speed if speed > 0 else 0
            data["eta_fmt"] = _fmt_duration(data["eta_seconds"])
            data["speed_per_min"] = speed * 60
        return data
    except (json.JSONDecodeError, OSError):
        return None


def is_job_running() -> bool:
    """Check if a scraper job is currently running."""
    data = read_progress()
    if not data or data.get("status") != "running":
        return False
    # Check if the process is actually alive
    pid = data.get("pid")
    if pid:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    return False


def clear_progress():
    """Remove the progress file."""
    if _PROGRESS_FILE.exists():
        _PROGRESS_FILE.unlink()
    _clear_cancel_flag()


def request_cancel():
    """Signal the running scraper to stop gracefully."""
    _PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    _CANCEL_FILE.write_text("cancel", encoding="utf-8")


def _clear_cancel_flag():
    """Remove the cancel flag file."""
    if _CANCEL_FILE.exists():
        _CANCEL_FILE.unlink()
