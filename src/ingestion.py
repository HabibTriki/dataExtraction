import os
import time
import csv
import logging
import requests
from datetime import datetime, timedelta
from auth import get_token
from config import API_BASE_URL
from concurrent.futures import ThreadPoolExecutor, as_completed

from fetch_transform import api_request

FUND_CONFIG = {
    "LODA_DATE": "DATE_SIGNATURE",
    "CIRC":      "DATE_SIGNATURE",
    "KALI":      "DATE_SIGNATURE",
    "ACCO":      "DATE_SIGNATURE",
    "CONSTIT":   "DATE_DECISION",
    "CNIL":      "DATE_DELIB",
}

PAGE_SIZE   = 100
SEARCH_URL  = f"{API_BASE_URL}/dila/legifrance/lf-engine-app/search"
LOG_PATH    = "logs/data_collection.log"
MAX_RETRIES = 5


def get_date_range():
    end   = datetime.utcnow().date()
    start = end - timedelta(days=3*365)
    return start.isoformat(), end.isoformat()


def collect_ids_for(fund: str, date_facet: str, start: str, end: str) -> list[str]:
    ids = set()
    page = 1

    while True:
        payload = {
            "recherche": {
                "champs": [],
                "filtres": [
                    {"facette": date_facet, "dates": {"start": start, "end": end}}
                ],
                "pageNumber":     page,
                "pageSize":       PAGE_SIZE,
            },
            "fond": fund
        }

        try:
            data = api_request("/dila/legifrance/lf-engine-app/search", payload)
        except Exception as e:
            logging.error(f"{fund} page {page}: failed to fetch data - {e}")
            break

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
                ids.add(doc_id)

        logging.info(f"{fund} page {page}: fetched {len(hits)} IDs")

        if len(hits) < PAGE_SIZE:
            break

        page += 1

    return list(ids)


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


def collect_year_chunk(fund, facet, year_start, year_end):
    logging.info(f" -> {fund} : {year_start} to {year_end}")
    return collect_ids_for(fund, facet, year_start.isoformat(), year_end.isoformat())


def main():
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s"
    )

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=3*365)

    logging.info(f"Collecting IDs from {start_date} to {end_date} (year by year)...")

    for fund, facet in FUND_CONFIG.items():
        all_ids = set()
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            current = start_date
            while current < end_date:
                next_quarter = current + timedelta(days=91)
                quarter_start = current
                quarter_end = min(next_quarter - timedelta(days=1), end_date)
                futures.append(
                    executor.submit(collect_year_chunk, fund, facet, quarter_start, quarter_end)
                )
                current = next_quarter

            for future in as_completed(futures):
                try:
                    all_ids.update(future.result())
                except Exception as e:
                    logging.error(f"Error collecting for {fund}: {e}")

        logging.info(f"{fund}: total {len(all_ids)} IDs across 3 years")

        output_path = f"data/{fund.lower()}_records.csv"
        with open(output_path, "w", newline="", encoding="utf-8") as fund_file:
            writer = csv.writer(fund_file)
            writer.writerow(["fund", "id", "data_link"])
            for doc_id in all_ids:
                writer.writerow([fund, doc_id, build_data_link(fund, doc_id)])

        print(f"\n=== {fund} ({len(all_ids)} IDs) ===")
        for sample in list(all_ids)[:5]:
            print("  ", sample)
        if len(all_ids) > 5:
            print("   ...\n")

if __name__ == "__main__":
    main()
