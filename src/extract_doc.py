import os
import logging
import docx2txt
import fitz  # PyMuPDF
import chardet

def extract_text_file(path: str) -> str:
    with open(path, 'rb') as f:
        raw = f.read()
    encoding = chardet.detect(raw)['encoding'] or 'utf-8'
    text = raw.decode(encoding, errors='ignore')
    return f"```\n{text.strip()}\n```"

def extract_docx_file(path: str) -> str:
    try:
        text = docx2txt.process(path)
        return f"```\n{text.strip()}\n```"
    except Exception as e:
        logging.error(f"DOCX parsing failed: {e}")
        return ""

def extract_pdf_file(path: str) -> str:
    try:
        doc = fitz.open(path)
        output = []
        for i, page in enumerate(doc):
            text = page.get_text("text", sort=True)
            output.append(f"\n\n## Page {i+1}\n\n{text.strip()}")
        return "\n".join(output)
    except Exception as e:
        logging.error(f"PDF parsing failed: {e}")
        return ""

def extract_file(path: str) -> str:
    if not os.path.exists(path):
        logging.error(f"File not found: {path}")
        return ""

    ext = os.path.splitext(path)[-1].lower()

    if ext == '.txt':
        return extract_text_file(path)
    elif ext == '.docx':
        return extract_docx_file(path)
    elif ext == '.pdf':
        return extract_pdf_file(path)
    else:
        logging.warning(f"Unsupported file type: {ext}")
        return ""

