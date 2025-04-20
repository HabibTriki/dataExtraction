import os
import csv
from datetime import datetime
from neo4j import GraphDatabase
from dotenv import load_dotenv
from auth import auth_client
from config import CATEGORIES, API_BASE_URL
import requests
import time

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")

MAX_RETRIES = 3
API_TIMEOUT = 30


def get_existing_documents(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (d:Document)
            RETURN d.fund AS fund, d.cid AS cid, d.pubDate AS pubDate, d.updatedAt AS updatedAt
        """)
        docs = {}
        for r in result:
            key = (r["fund"], r["cid"])
            date = r["updatedAt"] or r["pubDate"]
            docs[key] = date.isoformat() if isinstance(date, datetime) else date
        return docs


def fetch_api_list(fund, page_size=1000):
    endpoint = CATEGORIES.get(fund)
    if not endpoint:
        return []

    url = f"{API_BASE_URL}{endpoint}"
    token = auth_client.get_token()
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    offset = 0
    all_results = []

    while True:
        body = {
            "pageSize": page_size,
            "offset": offset
        }

        for attempt in range(MAX_RETRIES):
            try:
                res = requests.post(url, headers=headers, json=body, timeout=API_TIMEOUT)
                if res.status_code == 401:
                    auth_client.token_data["access_token"] = None
                    continue
                res.raise_for_status()
                data = res.json()
                results = data.get("results", [])
                if not results:
                    return all_results
                all_results.extend(results)
                offset += page_size
                if len(results) < page_size:
                    break
                time.sleep(0.5)
            except Exception as e:
                print(f"Error fetching list for {fund}: {e}")
                break
    return all_results


def parse_last_date(obj):
    for key in ['lastUpdate', 'datePubli', 'dateSignature', 'date']:
        if key in obj and obj[key]:
            try:
                return datetime.fromisoformat(obj[key][:10])
            except:
                continue
    return None


def compare_and_filter(api_list, existing_docs, fund):
    to_update = []

    for rec in api_list:
        cid = rec.get("cid") or rec.get("id")
        if not cid:
            continue

        key = (fund, cid)
        api_date = parse_last_date(rec)
        existing_date = existing_docs.get(key)

        if not existing_date:
            to_update.append({
                "fund": fund,
                "cid": cid,
                "lastUpdate": api_date.isoformat() if api_date else None
            })
            continue

        try:
            existing_date = datetime.fromisoformat(existing_date[:10])
            if api_date and api_date > existing_date:
                to_update.append({
                    "fund": fund,
                    "cid": cid,
                    "lastUpdate": api_date.isoformat()
                })
        except:
            continue

    return to_update


def write_csv(path, records):
    if not records:
        print("No new or updated records.")
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["fund", "cid", "lastUpdate"])
        writer.writeheader()
        writer.writerows(records)
    print(f"Saved {len(records)} records to update → {path}")


def run_diff_pipeline():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    existing_docs = get_existing_documents(driver)
    all_to_update = []

    for fund in CATEGORIES:
        print(f"Checking {fund}...")
        api_list = fetch_api_list(fund)
        new_or_updated = compare_and_filter(api_list, existing_docs, fund)
        all_to_update.extend(new_or_updated)

    driver.close()
    write_csv("data/to_update.csv", all_to_update)


if __name__ == "__main__":
    run_diff_pipeline()
