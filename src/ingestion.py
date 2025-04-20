import os
import time
import csv
import logging
import requests
from datetime import datetime, timedelta
from auth import get_token  
from config import API_BASE_URL

FUND_CONFIG = {
    "LODA_DATE": "DATE_SIGNATURE",
    "CIRC":      "DATE_SIGNATURE",
    "KALI":      "DATE_SIGNATURE",
    "ACCO":      "DATE_SIGNATURE",
    "CONSTIT":   "DATE_DECISION",
    "CNIL":      "DATE_DELIB",
}

PAGE_SIZE   = 10
SEARCH_URL  = f"{API_BASE_URL}/dila/legifrance/lf-engine-app/search"
LOG_PATH    = "logs/data_collection.log"
CSV_PATH    = "data/records.csv"
MAX_PAGES   = 1000
MAX_RETRIES = 3


def get_date_range():
    end   = datetime.utcnow().date()
    start = end - timedelta(days=3*365)
    return start.isoformat(), end.isoformat()


def collect_ids_for(fund: str, date_facet: str, start: str, end: str) -> list[str]:
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {get_token()}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    })

    ids = []
    page = 1

    while True:
        if page > MAX_PAGES:
            logging.warning(f"{fund}: reached API page cap at {MAX_PAGES}, stopping pagination")
            break

        payload = {
            "recherche": {
                "champs": [],
                "filtres": [
                    {"facette": date_facet, "dates": {"start": start, "end": end}}
                ],
                "pageNumber":     page,
                "pageSize":       PAGE_SIZE,
                "operateur":      "ET",
                "sort":           date_facet,
                "typePagination": "DEFAUT",
            },
            "fond": fund
        }

        for attempt in range(1, MAX_RETRIES + 1):
            r = session.post(SEARCH_URL, json=payload, timeout=30)
            if r.status_code == 401:
                session.headers["Authorization"] = f"Bearer {get_token()}"
                time.sleep(1)
                continue
            if r.status_code in (500, 503) and attempt < MAX_RETRIES:
                logging.warning(f"503 on {fund} page {page}, retry {attempt}")
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            break

        data = r.json()
        hits = data.get("results") or data.get("liste") or []
        if not hits:
            logging.info(f"{fund} page {page}: no results, stopping pagination")
            break

        for item in hits:
            if fund == "LODA_DATE":
                doc_id = (item.get("titles", [{}])[0].get("cid"))
            else:
                doc_id = (
                    (item.get("titles") or [{}])[0].get("id")
                    or item.get("id")
                    or item.get("textId")
                )
            if doc_id:
                ids.append(doc_id)

        logging.info(f"{fund} page {page}: fetched {len(hits)} IDs")

        if len(hits) < PAGE_SIZE:
            break

        page += 1

    return ids


def build_data_link(fund: str, doc_id: str) -> str:
    base_map = {
        "LODA_DATE": "https://www.legifrance.gouv.fr/loda/id/",
        "CIRC":      "https://www.legifrance.gouv.fr/circulaire/id/",
        "KALI":      "https://www.legifrance.gouv.fr/conv_coll/id/",
        "ACCO":      "https://www.legifrance.gouv.fr/acco/id/",
        "CONSTIT":   "https://www.legifrance.gouv.fr/cons/id/",
        "CNIL":      "https://www.legifrance.gouv.fr/cnil/id/",
    }
    if fund in base_map:
        return f"{base_map[fund]}{doc_id}"
    return f"{API_BASE_URL}/dila/legifrance/lf-engine-app/consult/legiPart?textId={doc_id}&date=<ms-since-epoch>"


def main():
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s"
    )

    start, end = get_date_range()
    logging.info(f"Collecting IDs from {start} to {end}…")

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["fund", "id", "data_link"])

        for fund, facet in FUND_CONFIG.items():
            ids = collect_ids_for(fund, facet, start, end)
            logging.info(f"{fund}: total {len(ids)} IDs")

            for doc_id in ids:
                writer.writerow([fund, doc_id, build_data_link(fund, doc_id)])

            print(f"\n=== {fund} ({len(ids)} IDs) ===")
            for sample in ids[:5]:
                print("  ", sample)
            if len(ids) > 5:
                print("   ...\n")

    logging.info(f"All records written to {CSV_PATH}")

if __name__ == "__main__":
    main()
