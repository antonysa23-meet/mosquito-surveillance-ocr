# Mosquito Surveillance OCR Pipeline

Reads scanned handwritten Harris County CDC Gravid trap collection forms (PDF)
and converts them into structured CSV and Excel files using **100% local AI** —
no data is ever sent to any cloud service.

**Current accuracy: ~47.6% field match rate on CPU hardware.**
On a GPU with `llama3.2-vision`, accuracy is expected to jump to 70–90%+.

---

## Quick Start

```bash
pip install pymupdf opencv-python pillow pytesseract openpyxl requests easyocr transformers torch
```

Install Tesseract 5.x: download from https://github.com/UB-Mannheim/tesseract/wiki

```bash
# Default: hybrid OCR mode (best CPU accuracy)
python pipeline.py example_mosquito_record_test.pdf

# Other modes
python pipeline.py example_mosquito_record_test.pdf --mode easyocr
python pipeline.py example_mosquito_record_test.pdf --mode trocr
python pipeline.py example_mosquito_record_test.pdf --mode whole-page --model llama3.2-vision
```

---

## How It Works

The pipeline has six stages:

| Stage | What it does |
|-------|-------------|
| 1. PDF → image | Renders the PDF at 300 DPI using PyMuPDF. High resolution is important for OCR quality. |
| 2. Cell detection | OpenCV morphological line detection isolates each table cell. Fully-black (redacted) rows are skipped. |
| 3. OCR (hybrid) | **TrOCR** (Microsoft handwriting transformer) reads numeric pool fields. **EasyOCR** reads text codes and addresses. Each engine is used where benchmarking shows it performs best. |
| 4. Type coercion | Each column's raw OCR output is cleaned by a column-specific fixer: digit substitutions (l→1, O→0), range checks, pattern extraction. |
| 5. Species matching | Species abbreviations are matched against a 38-species Harris County database using fuzzy string matching. |
| 6. Output | Writes CSV + formatted XLSX. A QC report compares extracted values against ground truth for the example PDF. |

### Why Two OCR Engines?

Benchmarking on this form type found that different engines win on different column types:

| Engine | Best at |
|--------|---------|
| **TrOCR** | POOL, NUM_POOLS, NO_MOSQ_POOLED, VIAL_NO |
| **EasyOCR** | SITE_NO, COLL_NO, CITY, AREA, SPECIES, ADDRESS |

Using each engine where it performs best gives 47.6% accuracy vs. 34.9% for TrOCR-only or 31.7% for EasyOCR-only.

---

## OCR Modes

| Mode | Engine | CPU Accuracy | Notes |
|------|--------|-------------|-------|
| `hybrid` (default) | EasyOCR + TrOCR per-column | **47.6%** | Best CPU option |
| `trocr` | TrOCR (all columns) | 34.9% | Good on handwritten numerics |
| `easyocr` | EasyOCR (all columns) | 31.7% | Faster than TrOCR |
| `tesseract` | Tesseract 5 | 15.9% | Baseline, no handwriting support |
| `whole-page` | Ollama LLM | TBD (GPU) | Use `--model llama3.2-vision` on a GPU machine |

---

## Column Schema

| Column | Type | Example | Notes |
|--------|------|---------|-------|
| SITE_NO | 4-digit int | 1841 | Trap location identifier |
| COLL_NO | Code | T4GV 508 | Trap 4, Gravid, collection #508 |
| SPECIES | Compound | `40 Cx.qf 20F; 4 Ae.ab 10F` | Count + species + female pool limit |
| POOL | 1-2 digit int | 2 | Number of pools collected |
| NO_MOSQ_POOLED | Int or compound | `39 Cx.qf / 4 Ae.ab` | Mosquitoes per pool |
| VIAL_NO | 4-digit(s) | `8758 / 8759` | Sample vial numbers |
| MSI | 1 digit | 1 | Mosquito Salivary Index |
| STATUS | Short code | N | N = negative, P = positive |
| NUM_POOLS | 1-2 digit int | 1 | Total pools from this site |
| AREA | 3-digit int | 216 | Harris County zone code |
| CITY | 2-4 letters | CB | Area abbreviation |
| ADDRESS | Text | 614 KERNOHAN (419-H) | Street + map grid code |

Common species abbreviations: `Cx.qf` = *Culex quinquefasciatus*, `Ae.ab` = *Aedes albopictus*,
`Ps.co` = *Psorophora columbiae*, `Ps.fx` = *Psorophora ferox*. `F` = female pool limit.

---

## Per-Column Accuracy (Hybrid Mode, CPU)

| Column | Accuracy | Notes |
|--------|----------|-------|
| SITE_NO | 86% (6/7) | Strong |
| AREA | 86% (6/7) | Strong |
| CITY | 71% (5/7) | Good |
| POOL | 71% (5/7) | Good |
| NO_MOSQ_POOLED | 43% (3/7) | Compound cells are hard |
| ADDRESS | 29% (2/7) | Noise stripping helps |
| COLL_NO | 29% (2/7) | Digit misreads on suffix |
| VIAL_NO | 14% (1/7) | Cell truncation issue |
| SPECIES | 0% (0/7) | **Requires GPU** |

---

## GPU Upgrade Path

The SPECIES field (0% accuracy on CPU) requires a vision-language model that can
understand multi-species compound abbreviations. On a machine with a GPU:

```bash
ollama pull llama3.2-vision
python pipeline.py example_mosquito_record_test.pdf --mode whole-page --model llama3.2-vision
```

This is expected to significantly improve SPECIES, COLL_NO, and VIAL_NO accuracy.

---

## Output Files

| File | Description |
|------|-------------|
| `mosquito_surveillance_data.csv` | Extracted data, UTF-8 with BOM (opens correctly in Excel) |
| `mosquito_surveillance_data.xlsx` | Formatted workbook with agency header, dark blue column headers |
| `_temp/page_0.png` | 300 DPI render of the PDF page |
| `_temp/cells/` | Individual cell crops used for debugging |
