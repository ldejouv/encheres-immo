#!/usr/bin/env python3
"""Launch the Streamlit dashboard."""

import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent


def main():
    app_path = PROJECT_DIR / "dashboard" / "app.py"
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(app_path), "--server.headless", "true"],
        cwd=str(PROJECT_DIR),
    )


if __name__ == "__main__":
    main()
