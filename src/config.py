import os
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL", "https://api.piste.gouv.fr")

CATEGORIES = {
    "CODE_DATE" :  "/dila/legifrance/lf-engine-app/consult/legiPart",
    "LODA_DATE":  "/dila/legifrance/lf-engine-app/consult/legiPart",
    "CIRC":       "/dila/legifrance/lf-engine-app/consult/circulaire",
    "ACCO":       "/dila/legifrance/lf-engine-app/consult/acco",
    "CONSTIT":    "/dila/legifrance/lf-engine-app/consult/juri",     
    "CNIL":       "/dila/legifrance/lf-engine-app/consult/cnil",
    "KALI":   "/dila/legifrance/lf-engine-app/consult/kaliArticle"
}

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "10"))

if __name__ == "__main__":
    print("API_BASE_URL:", API_BASE_URL)
    for name, endpoint in CATEGORIES.items():
        print(f"{name}: {API_BASE_URL}{endpoint}")
    print("PAGE_SIZE:", PAGE_SIZE)
