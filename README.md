# 🇫🇷 French Legal Corpus Ingestion Pipeline

This project builds a complete ETL pipeline for ingesting, transforming, and storing French legal documents from [Légifrance](https://www.legifrance.gouv.fr/) into a **Neo4j graph database**. It is designed to support legal AI applications, semantic search, and RAG (Retrieval-Augmented Generation) workflows.

---

## ✅ Goals Achieved

### 1. ⚡ Automated Data Collection
- Pulls **document metadata and IDs** from the Légifrance API using authenticated requests.
- Optimized retry/backoff and token refresh are handled via a custom `AuthClient`.
- All repositories are supported: Codes, Textes consolidés, Jurisprudence, CNIL, etc.

### 2. 🧠 Document Content Ingestion to Graph
- Automatically fetches **full structured content** (sections, articles) for each record.
- Transforms the JSON structure into **GitHub-flavored Markdown**, preserving fidelity and readability.

### 3. 🔄 Update Checker (Delta Detection)
- Compares `cid` and `lastUpdate` values between:
  - Neo4j stored documents
  - API's latest metadata
- Efficiently identifies documents that are **new or updated**.

### 4. 📝 Unified Markdown Transformation
- All document formats (JSON, DOCX, PDF, TXT) are parsed into a **single markdown format**.
- Includes recursive parsing for:
  - Sections → Articles → Paragraphs
  - DOCX and PDF are parsed with `docx2txt`, `pdfplumber`, and `markdownify`.

### 5. ✂️ Markdown Chunking for RAG
- Long markdowns are **split into overlapping chunks** (configurable size/overlap).
- Ensures alignment with **embedding best practices** for RAG-based systems.

### 6. 🔗 Graph Modeling + Embedding
- Each document is stored as a `:Document` node.
- Each chunk is stored as a `:Chunk` node with:
  - Raw text
  - Vector embedding (`Google text-embedding-004`, free API key)
- Chunks are linked via `[:HAS_CHUNK]` relationships.

### 7. 🧱 Document Relationships
- Automatically builds `[:REFERENCES]` or `[:CITES]` relationships using metadata.
- Enables navigation across related laws and structured legal interpretation.

---

## 💾 Storage Schema in Neo4j

```plaintext
(:Document {cid, title, category, metadata, pubDate, content})
  -[:HAS_CHUNK {order}]→ (:Chunk {text, vector, index})

(:Document)-[:REFERENCES|CITES]→(:Document)
```

---

## 📁 File Overview

| File | Purpose |
|------|---------|
| `auth.py` | Handles OAuth token authentication |
| `config.py` | API endpoints, categories, constants |
| `fetch_transform.py` | Main logic for fetching, transforming, vectorizing, and ingesting |
| `compare_update.py` | Diffs current database against remote records |
| `extract_doc.py` | Parses DOCX, PDF, and TXT to markdown |
| `ingestion.py` | Functions to ingest |

---

## 🐳 Local Setup

1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```

3. Start Neo4j using Docker:
   ```bash
   docker-compose up
   ```

4. Create a `.env` file:
   ```env
   API_BASE_URL=https://api.piste.gouv.fr
   TOKEN_URL=https://oauth.piste.gouv.fr/api/oauth/token
   CLIENT_ID= you client id
   CLIENT_SECRET=your client secret
   PAGE_SIZE= 10
   NEO4J_URI      = "bolt://localhost:7687"
   NEO4J_USER     = 
   NEO4J_PASSWORD = 
   GOOGLE_APPLICATION_CREDENTIALS= full path ../yourkey.json
   GOOGLE_GENAI_USE_VERTEXAI=False
   GOOGLE_CLOUD_LOCATION=us-central1
   ```

5. Run the ingestion pipeline step by step:
   ```bash
   python src/ingestion_code.py

   python src/ingestion.py

   python src/fetch_transform.py
   ```

---

## 📊 Example Query in Neo4j

```cypher
MATCH (d:Document)-[:HAS_CHUNK]->(c:Chunk)
RETURN d.title, c.text LIMIT 10
```
---

## 📬 Contact

Feel free to reach out if you have any questions or would like to extend this project.
