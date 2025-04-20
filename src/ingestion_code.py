#!/usr/bin/env python3
import os
import csv
import logging
import requests
from datetime import datetime, timedelta
from auth import get_token
from config import API_BASE_URL

# ─────────────────────────────────────────────────────────────
FUND          = "CODE_DATE"
LIST_URL      = f"{API_BASE_URL}/dila/legifrance/lf-engine-app/list/code"
LOG_PATH      = "logs/ingestion_code.log"
CSV_PATH      = "data/codes.csv"
MAX_RETRIES   = 3
PAGE_SIZE     = 200 
CUTOFF_DATE   = datetime.utcnow().date() - timedelta(days=3*365)
CODE_LINK_TMPL = "https://www.legifrance.gouv.fr/codes/texte_lc/{cid}"

# ─────────────────────────────────────────────────────────────
def fetch_codes():
    """
    Fetch all codes via the list/code endpoint (single page).
    """
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {get_token()}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    })
    payload = {"pageNumber": 1, "pageSize": PAGE_SIZE}

    for attempt in range(1, MAX_RETRIES+1):
        r = session.post(LIST_URL, json=payload, timeout=30)
        if r.status_code == 401:
            session.headers["Authorization"] = f"Bearer {get_token()}"
            continue
        if r.status_code in (500, 503) and attempt < MAX_RETRIES:
            logging.warning(f"List fetch error {r.status_code}, retry {attempt}")
            continue
        r.raise_for_status()
        break

    data = r.json()
    return data.get("results", []) 

# ─────────────────────────────────────────────────────────────
def parse_date(dt_str: str) -> datetime.date:
    """
    Parse ISO date like '2025-04-17T00:00:00.000+0000'.
    """
    try:
        return datetime.fromisoformat(dt_str[:19]).date()
    except ValueError:
        return datetime.strptime(dt_str.split('T')[0], '%Y-%m-%d').date()

# ─────────────────────────────────────────────────────────────
def main():
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s"
    )

    logging.info(f"Fetching all codes via list/code...")
    codes = fetch_codes()
    logging.info(f"Total codes returned: {len(codes)}")

    kept = []
    for item in codes:
        last_update = item.get("lastUpdate")
        if not last_update:
            continue
        update_date = parse_date(last_update)
        if update_date >= CUTOFF_DATE:
            kept.append(item)

    logging.info(f"Codes with lastUpdate >= {CUTOFF_DATE}: {len(kept)}")

    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["fund", "cid", "data_link"])
        for item in kept:
            cid = item.get("cid")
            link = CODE_LINK_TMPL.format(cid=cid)
            writer.writerow([FUND, cid, link])

    logging.info(f"Wrote {len(kept)} records to {CSV_PATH}")

if __name__ == '__main__':
    main()
