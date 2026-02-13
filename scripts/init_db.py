#!/usr/bin/env python3
"""Initialize the SQLite database from schema.sql."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.database import Database


def main():
    db = Database()
    db.initialize()
    print(f"Database initialized at {db.db_path}")


if __name__ == "__main__":
    main()
