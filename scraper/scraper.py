# scraper_production.py
import requests
import json
import os
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path

API_KEY = os.getenv("API_KEY", "ok_561346d2d2bc79640d992fbd66ed3b43")
BASE_URL = "https://tripadvisor-scraper-api.omkar.cloud/tripadvisor/reviews"
QUERY = "Pashupatinath Temple, Kathmandu"
OUTPUT_DIR = "raw_reviews"
CHECKPOINT_FILE = "checkpoint.json"

def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")

def fetch_page(page, max_retries=3):
    params = {
        "query": QUERY,
        "page": page,
        "sort_by": "most_recent",
        "locale": "en-US"
    }
    headers = {"API-Key": API_KEY}

    for attempt in range(max_retries):
        try:
            resp = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt  # 1s, 2s, 4s
            log(f"Page {page} attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    raise Exception(f"Page {page} failed after {max_retries} retries")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"last_page": 0, "run_id": None}

def save_checkpoint(page, run_id):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_page": page, "run_id": run_id}, f)

def scrape():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    checkpoint = load_checkpoint()
    start_page = checkpoint["last_page"] + 1
    run_id = checkpoint["run_id"] or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    scraped_at = datetime.now(timezone.utc).isoformat()

    log(f"Starting run {run_id} from page {start_page}")

    page = start_page
    total_pages = None
    all_reviews = []

    while True:
        data = fetch_page(page)
        reviews = data.get("results", [])

        # Save individual page (idempotent — overwrites if re-run)
        page_file = f"{OUTPUT_DIR}/page_{page:04d}_{run_id}.json"
        with open(page_file, "w", encoding="utf-8") as f:
            json.dump({
                "scraped_at": scraped_at,
                "run_id": run_id,
                "page": page,
                "api_response": data
            }, f, ensure_ascii=False)

        all_reviews.extend(reviews)
        save_checkpoint(page, run_id)

        if total_pages is None:
            total_pages = data.get("total_pages", page)
            log(f"Total pages: {total_pages}")

        log(f"Page {page}: {len(reviews)} reviews | total: {len(all_reviews)}")

        if page >= total_pages or not data.get("next"):
            break

        page += 1
        time.sleep(0.5)  # Be nice to the API

    # Flatten to JSONL for Databricks ingestion
    jsonl_file = f"{OUTPUT_DIR}/reviews_{run_id}.jsonl"
    with open(jsonl_file, "w", encoding="utf-8") as f:
        for r in all_reviews:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Metadata manifest
    manifest = {
        "run_id": run_id,
        "scraped_at": scraped_at,
        "total_pages": page,
        "total_reviews": len(all_reviews),
        "jsonl_file": jsonl_file,
        "query": QUERY
    }
    with open(f"{OUTPUT_DIR}/manifest_{run_id}.json", "w") as f:
        json.dump(manifest, f, indent=2)

    # Clear checkpoint on success
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    log(f"✓ Complete. {len(all_reviews)} reviews → {jsonl_file}")
    return manifest

if __name__ == "__main__":
    scrape()