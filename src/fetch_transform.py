import os
import csv
import json
import requests
import logging
from datetime import datetime
import time
import traceback
import tempfile
import requests
from markdownify import MarkdownConverter
from extract_doc import extract_file
from dotenv import load_dotenv
from neo4j import GraphDatabase
from config import CATEGORIES, API_BASE_URL
from auth import auth_client
import re

try:
    from google import genai
    from google.genai import types
    EMBEDDING_AVAILABLE = True
except ImportError:
    logging.warning("Google GenAI package not available, embeddings will be disabled")
    EMBEDDING_AVAILABLE = False

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join("logs", "fetch_transform.log")),
        logging.StreamHandler()
    ]
)

if EMBEDDING_AVAILABLE:
    try:
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path and os.path.exists(creds_path):
            genai_client = genai.Client(
                vertexai=True,
                project=os.getenv("GOOGLE_PROJECT_ID"),
                location=os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1"),
                http_options=types.HttpOptions(api_version="v1")
            )
            EMBEDDING_MODEL = os.getenv("GOOGLE_EMBEDDING_MODEL", "text-embedding-004")
            logging.info(f"Google Vertex AI Embedding initialized with model: {EMBEDDING_MODEL}")
        else:
            raise RuntimeError("No valid credentials for Vertex AI")
    except Exception as e:
        logging.error(f"Failed to initialize Google Embedding: {e}")
        EMBEDDING_AVAILABLE = False


NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "lawGraph")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 500)) 
OVERLAP = int(os.getenv("CHUNK_OVERLAP", 100))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))


def api_request(endpoint, data):
    """Make an authenticated request using AuthClient with retry mechanism."""
    url = f"{API_BASE_URL}{endpoint}"
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            token = auth_client.get_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            response = requests.post(url, headers=headers, json=data, timeout=30)
            
            if response.status_code == 401:
                auth_client.token_data["access_token"] = None
                continue
                
            if response.status_code in (500, 503) and attempt < MAX_RETRIES:
                sleep_time = 2 ** attempt
                logging.warning(f"Server error {response.status_code}, retrying in {sleep_time}s")
                time.sleep(sleep_time)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                logging.error(f"API request failed after {MAX_RETRIES} attempts: {str(e)}")
                raise
            time.sleep(2 ** attempt)
    
    return None


class NoLinksMarkdownConverter(MarkdownConverter):
    def convert_a(self, el, text, convert_as_inline):
        return text 
def clean_html(html: str) -> str:
    return NoLinksMarkdownConverter(heading_style="ATX").convert(html)

def download_and_extract_file(url: str) -> str:
    """Download and extract legal file (PDF/DOCX/TXT) to Markdown."""
    if not url or not url.startswith("http"):
        return ""

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        suffix = os.path.splitext(url)[-1].split("?")[0] 
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name

        return extract_file(tmp_path)
    except Exception as e:
        logging.warning(f"Failed to download or extract file from {url}: {e}")
        return ""

def get_embedding(text: str) -> list[float]:
    if not EMBEDDING_AVAILABLE or not text:
        return [0.0] * 768

    try:
        if len(text) > 25000:
            text = text[:25000]

        api_key = os.getenv("GOOGLE_API_KEY")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:embedContent?key={api_key}"
        headers = {"Content-Type": "application/json"}
        body = {
            "model": "models/text-embedding-004",
            "content": {"parts": [{"text": text}]}
        }

        resp = requests.post(url, headers=headers, json=body)
        resp.raise_for_status()

        return resp.json()["embedding"]["values"]
    except Exception as e:
        logging.error(f"Embedding error (Gemini API key): {e}")
        return [0.0] * 768




def load_csv(path: str) -> list[dict]:
    """Load and parse a CSV file."""
    if not os.path.exists(path):
        logging.warning(f"CSV file not found: {path}")
        return []
        
    try:
        with open(path, newline='', encoding='utf-8') as f:
            return list(csv.DictReader(f))
    except Exception as e:
        logging.error(f"Error loading CSV {path}: {str(e)}")
        return []


def fetch_doc_json(fund: str, cid: str, version_date: str = None) -> dict:
    """Fetch document JSON from API based on fund type and ID."""
    endpoint = CATEGORIES.get(fund)
    if not endpoint:
        raise ValueError(f"No consult endpoint for fund '{fund}'")

    if fund in ('CODE_DATE', 'LODA_DATE'):
        body = {'textId': cid}
        if version_date:
            if version_date.isdigit() and len(version_date) > 10:
                dt = datetime.fromtimestamp(int(version_date) / 1000)
                body['date'] = dt.strftime('%Y-%m-%d')
            elif re.match(r'\d{4}-\d{2}-\d{2}', version_date):
                body['date'] = version_date
            else:
                try:
                    dt = datetime.fromisoformat(version_date.replace('Z', '+00:00'))
                    body['date'] = dt.strftime('%Y-%m-%d')
                except ValueError:
                    body['date'] = datetime.now().strftime('%Y-%m-%d')
        else:
            body['date'] = datetime.now().strftime('%Y-%m-%d')
    else:
        body = {'id': cid}

    return api_request(endpoint, body)


def extract_article_content(article: dict) -> str:
    if not article:
        return ""

    num = article.get("num", "")
    title_line = f"### Article {num}" if num else "### Article"

    modif = article.get("modificatorTitle")
    list_modif = article.get("lstLienModification", [])
    
    nature = ""
    for lien in list_modif:
        if lien.get("natureText", "").upper() == "DECRET":
            nature = "DECRET"
            break

    if modif:
        if nature == "DECRET":
            modif_line = f"Création {modif}"
        else:
            modif_line = f"Modifié par {modif}"
    else:
        modif_line = ""

    raw_html = article.get("content") or article.get("texte") or article.get("text") or ""
    text_md = clean_html(raw_html)

    nota = article.get("nota")
    nota_clean = clean_html(nota)
    nota_block = f"\n\nNOTA :\n{nota_clean.strip()}" if nota else ""

    return f"{title_line}\n\n{modif_line}\n\n{text_md.strip()}{nota_block}\n\n"



def extract_section_content(section: dict, level: int = 2) -> str:
    if not isinstance(section, dict):
        return ""

    output = ""

    title = section.get("title") or section.get("titre")
    if title:
        output += f"{'#' * level} {title}\n\n"

    raw = section.get("content") or section.get("texte") or section.get("text") or ""
    if raw:
        output += clean_html(raw) + "\n\n"

    articles = sorted(section.get("articles", []), key=lambda a: a.get("intOrdre", 0))
    for art in articles:
        output += extract_article_content(art)

    subsections = (
        section.get("sections", []) +
        section.get("children", []) +
        section.get("subsections", [])
    )
    subsections = sorted(subsections, key=lambda s: s.get("intOrdre", 0))

    for sub in subsections:
        output += extract_section_content(sub, level + 1)

    return output



def json_to_markdown(fund: str, data: dict) -> str:
    """
    Convert API JSON response to markdown format based on document type.
    """
    if not data:
        return ""
        
    md = ''
    
    title = data.get('title') or data.get('titre')
    if title:
        md += f"# {title}\n\n"
    
    if 'datePubli' in data or 'dateSignature' in data:
        md += "## Metadata\n\n"
        if 'datePubli' in data:
            md += f"* Publication Date: {data['datePubli']}\n"
        if 'dateSignature' in data:
            md += f"* Signature Date: {data['dateSignature']}\n"
        md += "\n"

    if fund == 'CODE_DATE':
        root_section = {
            "title": data.get("title", ""),
            "sections": data.get("sections", []),
            "articles": data.get("articles", [])
        }
        md += extract_section_content(root_section)

    else:
        text = data.get('text') or data.get('content') or ''
        if text:
            md += f"{text}\n\n"
            
        for section in data.get('sections', []):
            md += extract_section_content(section)
            
        for art in data.get('articles', []):
            md += extract_article_content(art)
            
        if fund == 'CONSTIT' and 'decision' in data:
            md += extract_section_content(data['decision'])

    md = re.sub(r'\n{3,}', '\n\n', md)
    
    return md


def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = OVERLAP) -> list[str]:
    """
    Split text into overlapping chunks for embedding.
    """
    if not text:
        return []
        
    if len(text) <= size:
        return [text]
        
    paras = text.split('\n\n')
    chunks, curr = [], ''
    
    for p in paras:
        if not p.strip():
            continue
            
        if len(curr) + len(p) + 2 <= size:
            curr = f"{curr}\n\n{p}" if curr else p
        else:
            if curr:
                chunks.append(curr)
                
            if len(p) <= size:
                curr = p
            else:
                for i in range(0, len(p), size - overlap):
                    chunk = p[i:i + size]
                    if chunk:
                        chunks.append(chunk)
                curr = ''
    
    if curr:
        chunks.append(curr)
        
    return chunks


def ingest_to_neo(driver, fund: str, cid: str, url: str, md: str, data: dict):
    """
    Store document content and chunks in Neo4j database.
    """
    chunks = chunk_text(md)
    
    try:
        with driver.session() as sess:
            sess.execute_write(_create_document, fund, cid, url, md, data)
            
            if chunks:
                sess.execute_write(_create_chunks, fund, cid, chunks)
                
            sess.execute_write(_create_document_relationships, fund, cid, data)
            
    except Exception as e:
        logging.error(f"Neo4j ingestion error for {fund}/{cid}: {str(e)}")
        raise


def _create_document(tx, fund: str, cid: str, url: str, content: str, data: dict):
    """Create or update a document node in Neo4j."""
    title = data.get('title') or data.get('titre') or ''
    category = data.get('categorie') or data.get('category') or fund
    pub_date = (
        data.get('publicationDate') or 
        data.get('dateSignature') or 
        data.get('datePubli') or
        ''
    )
    
    metadata = {
        "source": fund,
        "dateCreated": datetime.now().isoformat()
    }
    
    for key in ['dateSignature', 'datePubli', 'nature', 'num', 'nor']:
        if key in data and data[key]:
            metadata[key] = data[key]
    
    tx.run(
        """
        MERGE (d:Document {cid: $cid, fund: $fund})
        SET d.url = $url,
            d.content = $content,
            d.updatedAt = datetime(),
            d.title = $title,
            d.category = $category,
            d.pubDate = $pub,
            d.metadata = $metadata
        """,        
        cid=cid,
        fund=fund,
        url=url,
        content=content,
        title=title,
        category=category,
        pub=pub_date,
        metadata=json.dumps(metadata)
    )


def _create_chunks(tx, fund: str, cid: str, chunks: list[str]):
    """Create text chunks and connect them to their parent document."""
    tx.run(
        """
        MATCH (d:Document {cid: $cid, fund: $fund})-[r:HAS_CHUNK]->(c:Chunk)
        DELETE r, c
        """,
        cid=cid,
        fund=fund
    )
    
    for i, txt in enumerate(chunks):
        if not txt.strip():
            continue
            
        chunk_id = f"{cid}__{i}"
        
        vec = get_embedding(txt)
        
        tx.run(
            """
            MATCH (d:Document {cid: $cid, fund: $fund})
            CREATE (c:Chunk {
                id: $chunk_id,
                text: $txt,
                vector: $vec,
                index: $i
            })
            CREATE (d)-[:HAS_CHUNK {order: $i}]->(c)
            """,
            cid=cid,
            fund=fund,
            chunk_id=chunk_id,
            txt=txt,
            vec=vec,
            i=i
        )


def _create_document_relationships(tx, fund: str, cid: str, data: dict):
    """Create relationships between this document and others it references."""
    references = []
    
    if fund == 'CODE_DATE':
        for code in data.get('codeList', []):
            ref_id = code.get('cid')
            if ref_id:
                references.append(('REFERENCES', 'CODE_DATE', ref_id))
                
    elif fund == 'LODA_DATE':
        for link in data.get('liens', []):
            ref_id = link.get('id')
            ref_type = link.get('typeCode')
            if ref_id and ref_type:
                references.append(('REFERENCES', ref_type, ref_id))
                
    elif fund == 'CONSTIT':
        for ref in data.get('references', []):
            ref_id = ref.get('id')
            if ref_id:
                references.append(('CITES', 'CONSTIT', ref_id))
    
    for rel_type, ref_fund, ref_id in references:
        tx.run(
            f"""
            MATCH (d1:Document {{cid: $cid1, fund: $fund1}})
            MERGE (d2:Document {{cid: $cid2, fund: $fund2}})
            MERGE (d1)-[:{rel_type}]->(d2)
            """,
            cid1=cid,
            fund1=fund,
            cid2=ref_id,
            fund2=ref_fund
        )


def process_record(driver, fund: str, cid: str, url: str, version_date: str = None):
    try:
        logging.info(f"Processing {fund}/{cid}...")
        
        data = fetch_doc_json(fund, cid, version_date)
        
        if not data:
            logging.warning(f"No data returned for {fund}/{cid}")
            return False
            
        md = json_to_markdown(fund, data)
        
        if not md.strip():
            logging.warning(f"Empty content for {fund}/{cid}")
            return False
        
        file_url = data.get("fileUrl") or data.get("fichier") or data.get("urlFichier")
        file_md = download_and_extract_file(file_url)

        if file_md:
            md = f"{md.strip()}\n\n---\n\n## 📎 Attached Document\n\n{file_md.strip()}"

        ingest_to_neo(driver, fund, cid, url, md, data)
        
        logging.info(f"Successfully processed {fund}/{cid}")
        return True
        
    except Exception as e:
        logging.error(f"Error processing {fund}/{cid}: {str(e)}")
        traceback.print_exc()
        return False


def main():
    try:
        token = auth_client.get_token()
        if not token:
            raise RuntimeError("Unable to obtain OAuth token via AuthClient")
    except Exception as e:
        logging.error(f"Authentication error: {str(e)}")
        return
        
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        # Test connection
        with driver.session() as session:
            session.run("RETURN 1")
    except Exception as e:
        logging.error(f"Neo4j connection error: {str(e)}")
        return
        
    records = []
    
    if os.path.exists('data/to_update.csv'):
        records = load_csv('data/to_update.csv')
        logging.info(f"Found {len(records)} records to update")
    
    if not records:
        if os.path.exists('data/ids/codes.csv'):
            records += load_csv('data/ids/codes.csv')
        if os.path.exists('data/ids/records.csv'):
            records += load_csv('data/ids/records.csv')
        if os.path.exists('data/codes.csv'):
            records += load_csv('data/codes.csv')
        if os.path.exists('data/records.csv'):
            records += load_csv('data/records.csv')
    
    if not records:
        logging.error("No records found to process")
        driver.close()
        return
        
    logging.info(f"Found {len(records)} total records to process")
    
    success, failure = 0, 0
    
    for i, rec in enumerate(records):
        fund = rec.get('fund')
        cid = rec.get('cid') or rec.get('id')
        url = rec.get('data_link') or rec.get('url', f"https://www.legifrance.gouv.fr/loda/id/{cid}")
        version_date = rec.get('lastUpdate') or rec.get('date')
        
        if not fund or not cid:
            logging.warning(f"Record {i} missing fund or cid: {rec}")
            failure += 1
            continue
            
        if i > 0 and i % 10 == 0:
            time.sleep(1)
            
        if i % 25 == 0:
            logging.info(f"Progress: {i}/{len(records)} records processed")
            
        if process_record(driver, fund, cid, url, version_date):
            success += 1
        else:
            failure += 1

    driver.close()
    logging.info(f"Processing complete! Success: {success}, Failure: {failure}")


if __name__ == '__main__':
    main()