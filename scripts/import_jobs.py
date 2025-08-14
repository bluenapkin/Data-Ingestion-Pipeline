import sqlite3
import csv
import json
from datetime import datetime

DB_FILE = "jobs.db"

# Setup DB tables
def setup_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            title TEXT,
            company TEXT,
            location TEXT,
            salary REAL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS ingestion_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file TEXT,
            status TEXT,
            timestamp TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Logging
def log_ingestion(source_file, status):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT INTO ingestion_logs (source_file, status, timestamp) VALUES (?, ?, ?)",
                (source_file, status, datetime.now().isoformat()))
    conn.commit()
    conn.close()

# Import CSV
def import_csv(file_path):
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                cur.execute("INSERT OR IGNORE INTO jobs VALUES (?, ?, ?, ?, ?)",
                            (row["id"], row["title"], row["company"], row["location"], row["salary"]))
        conn.commit()
        conn.close()
        log_ingestion(file_path, "SUCCESS")
    except Exception as e:
        log_ingestion(file_path, f"FAILED: {str(e)}")

# Import JSON
def import_json(file_path):
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        with open(file_path, 'r', encoding='utf-8') as jsonfile:
            data = json.load(jsonfile)
            for row in data:
                cur.execute("INSERT OR IGNORE INTO jobs VALUES (?, ?, ?, ?, ?)",
                            (row["id"], row["title"], row["company"], row["location"], row["salary"]))
        conn.commit()
        conn.close()
        log_ingestion(file_path, "SUCCESS")
    except Exception as e:
        log_ingestion(file_path, f"FAILED: {str(e)}")

if __name__ == "__main__":
    setup_db()
    import_csv("../data/jobs.csv")
    import_json("../data/jobs.json")
