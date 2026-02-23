"""
main.py — start producer + orchestrator together.

    python main.py
"""
import os
import threading
import time

# Load env vars before any other imports touch them
from dotenv import load_dotenv
load_dotenv()

# print(f"DEBUG: API Key starts with: {os.getenv('GOOGLE_API_KEY')}...")
from db import init_db
from producer import run_producer
from orchestrator import run_orchestrator


def main():
    init_db()

    producer_thread = threading.Thread(
        target=run_producer,
        kwargs={"interval": 6.0},   # new deal every 6s
        daemon=True,
        name="Producer",
    )
    producer_thread.start()

    # Give producer time to insert first deal before orchestrator starts polling
    time.sleep(2)

    run_orchestrator()


if __name__ == "__main__":
    main()