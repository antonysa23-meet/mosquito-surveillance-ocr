"""
Vision LLM Approach: Whole-Page Table Extraction via Ollama
===========================================================

Alternative pipeline that uses a multimodal (vision) Large Language Model
to extract tabular data directly from the PDF image. Instead of cell-by-cell
OCR, this sends the entire page image to a vision-capable LLM and asks it
to extract the table structure in one pass.

This approach is conceptually simpler and can handle:
- Mixed printed/handwritten text
- Complex table layouts with merged cells
- Contextual understanding of abbreviations

Prerequisites:
    1. Install Ollama: https://ollama.com/download
    2. Pull a vision model:
       ollama pull moondream       # ~1.7GB, fast, good for table extraction
       ollama pull llava:7b        # ~4.7GB, more capable
       ollama pull minicpm-v       # ~5.5GB, strong on documents
    3. Ensure Ollama is running: ollama serve

Usage:
    python ollama_vision_pipeline.py                    # Default: moondream
    python ollama_vision_pipeline.py --model llava:7b   # Use llava instead
    python ollama_vision_pipeline.py --model minicpm-v  # Use minicpm-v

Privacy: All processing runs locally via Ollama. No data leaves this machine.
"""

import os
import sys
import json
import argparse
import logging
import base64
import subprocess
from pathlib import Path

import cv2
import fitz
import numpy as np
import pandas as pd
from PIL import Image

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ollama-vision")

SCRIPT_DIR = Path(__file__).parent
PDF_PATH = SCRIPT_DIR / "example_mosquito_record_test.pdf"
OUTPUT_CSV = SCRIPT_DIR / "ollama_extracted_data.csv"
OUTPUT_XLSX = SCRIPT_DIR / "ollama_extracted_data.xlsx"

# Extraction prompt designed for tabular handwritten documents
EXTRACTION_PROMPT = """You are analyzing a scanned handwritten document from Harris County mosquito surveillance.

This is a table with these columns (left to right):
SITE NO. | COLL NO. | SPECIES | POOL | NO. MOSQ POOLED | VIAL NO. | M | A | S | I | STATUS | AREA | CITY | ADDRESS

Each row represents a mosquito collection record. The handwriting may be difficult to read.

Please extract ALL data rows from this table into a JSON array. Each row should be a JSON object with these keys:
- SITE_NO (4-digit number)
- COLL_NO (format: T4GV followed by 3-digit number)
- SPECIES (mosquito species abbreviations like Cx.qf, Ae.ab, etc. with counts)
- POOL (number of pools)
- NO_MOSQ_POOLED (number of mosquitoes pooled)
- VIAL_NO (4-digit vial numbers)
- M, A, S, I (status indicators, may be empty)
- STATUS (3-digit number)
- AREA (may be empty)
- CITY (2-3 letter abbreviation like NEC, CB, HG, BR, BS)
- ADDRESS (street address with optional map grid reference in parentheses)

Common mosquito species abbreviations:
- Cx.qf = Culex quinquefasciatus
- Ae.ab = Aedes albopictus
- Cu.ng = Culex nigripalpus
- Ps.co = Psorophora columbiae
- Ps.fx = Psorophora ferox

Output ONLY the JSON array, no other text. Example format:
[
  {"SITE_NO": "1841", "COLL_NO": "T4GV 508", "SPECIES": "40 Cx.qf 20F", ...},
  ...
]"""


def check_ollama() -> bool:
    """Check if Ollama is running and accessible."""
    try:
        import urllib.request
        req = urllib.request.Request("http://localhost:11434/api/tags")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            models = [m["name"] for m in data.get("models", [])]
            log.info(f"Ollama is running. Available models: {models}")
            return True
    except Exception as e:
        log.error(f"Ollama not available: {e}")
        log.error("Please ensure Ollama is installed and running (ollama serve)")
        return False


def list_vision_models() -> list[str]:
    """List installed Ollama models that support vision."""
    vision_capable = ["moondream", "llava", "minicpm-v", "bakllava", "llava-phi3",
                      "cogvlm", "yi-vl", "internvl", "qwen2-vl"]
    try:
        import urllib.request
        req = urllib.request.Request("http://localhost:11434/api/tags")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            installed = [m["name"] for m in data.get("models", [])]
            return [m for m in installed if any(v in m.lower() for v in vision_capable)]
    except Exception:
        return []


def pdf_to_base64(pdf_path: Path) -> str:
    """Convert first page of PDF to base64-encoded PNG."""
    doc = fitz.open(str(pdf_path))
    page = doc[0]
    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # 2x zoom
    img_bytes = pix.tobytes("png")
    doc.close()
    return base64.b64encode(img_bytes).decode("utf-8")


def query_ollama_vision(model: str, image_b64: str, prompt: str) -> str:
    """Send an image + prompt to Ollama's vision API."""
    import urllib.request

    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "images": [image_b64],
        "stream": False,
        "options": {
            "temperature": 0.1,  # Low temp for factual extraction
            "num_predict": 4096,
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        "http://localhost:11434/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    log.info(f"Sending image to {model} for extraction...")
    log.info("  (This may take 30-120 seconds on CPU)")

    with urllib.request.urlopen(req, timeout=300) as resp:
        result = json.loads(resp.read())
        return result.get("response", "")


def parse_llm_response(response: str) -> pd.DataFrame:
    """Parse the LLM's JSON response into a DataFrame."""
    # Try to extract JSON from the response
    text = response.strip()

    # Find JSON array boundaries
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        log.error("No JSON array found in LLM response")
        log.error(f"Raw response: {text[:500]}")
        return pd.DataFrame()

    json_str = text[start:end + 1]
    try:
        records = json.loads(json_str)
        df = pd.DataFrame(records)
        log.info(f"Parsed {len(df)} records from LLM response")
        return df
    except json.JSONDecodeError as e:
        log.error(f"JSON parse error: {e}")
        log.error(f"JSON string: {json_str[:500]}")
        return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(description="Vision LLM Table Extractor")
    parser.add_argument("--model", type=str, default="moondream",
                        help="Ollama vision model to use (default: moondream)")
    parser.add_argument("--pdf", type=str, default=str(PDF_PATH))
    parser.add_argument("--pull", action="store_true",
                        help="Pull the model first if not installed")
    args = parser.parse_args()

    log.info("=" * 65)
    log.info("  VISION LLM TABLE EXTRACTION (Ollama)")
    log.info("  All processing runs locally. No data leaves this machine.")
    log.info("=" * 65)

    # Check Ollama
    if not check_ollama():
        log.error("\nTo set up Ollama:")
        log.error("  1. Download from https://ollama.com/download")
        log.error("  2. Install and run: ollama serve")
        log.error(f"  3. Pull a vision model: ollama pull {args.model}")
        sys.exit(1)

    # Check/pull model
    vision_models = list_vision_models()
    if not any(args.model in m for m in vision_models):
        if args.pull:
            log.info(f"Pulling model {args.model}...")
            subprocess.run(["ollama", "pull", args.model], check=True)
        else:
            log.error(f"Model '{args.model}' not found. Available vision models: {vision_models}")
            log.error(f"Pull it with: ollama pull {args.model}")
            log.error(f"Or run this script with --pull flag")
            sys.exit(1)

    # Convert PDF to image
    pdf_path = Path(args.pdf)
    log.info(f"Processing: {pdf_path}")
    image_b64 = pdf_to_base64(pdf_path)
    log.info(f"  Image encoded ({len(image_b64) // 1024} KB base64)")

    # Query vision LLM
    response = query_ollama_vision(args.model, image_b64, EXTRACTION_PROMPT)
    log.info(f"  Got response ({len(response)} chars)")

    # Parse response
    df = parse_llm_response(response)
    if df.empty:
        log.error("Failed to extract data. Raw LLM response saved to debug/")
        Path(SCRIPT_DIR / "debug").mkdir(exist_ok=True)
        (SCRIPT_DIR / "debug" / "ollama_raw_response.txt").write_text(response)
        sys.exit(1)

    # Save outputs
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    log.info(f"CSV → {OUTPUT_CSV}")

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Vision LLM Extraction")
        meta = pd.DataFrame({
            "Field": ["Model", "Type", "Privacy", "Date"],
            "Value": [args.model, "Multimodal Vision LLM (local via Ollama)",
                      "All processing local, no cloud APIs", pd.Timestamp.now().isoformat()]
        })
        meta.to_excel(writer, index=False, sheet_name="Metadata")

    log.info(f"XLSX → {OUTPUT_XLSX}")

    print("\nExtracted data:")
    print(df.to_string(index=False))
    return df


if __name__ == "__main__":
    main()
