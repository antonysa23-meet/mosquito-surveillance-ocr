# Local Models

Models installed for this pipeline. All are managed by Ollama and can be removed at any time.

## Installed

| Model | Size | Purpose | Remove |
|-------|------|---------|--------|
| `llava:latest` | 4.7 GB | Vision LLM — whole-page or cell-by-cell OCR (needs GPU for speed) | `ollama rm llava` |
| `llama3.2-vision:latest` | 7.9 GB | Better vision LLM — significantly more accurate on handwritten forms | `ollama rm llama3.2-vision` |

## Remove everything

```bash
ollama rm llava
ollama rm llama3.2-vision
```

Or uninstall Ollama entirely via Windows Settings > Apps.

## Usage

```bash
# Use llama3.2-vision (best accuracy, needs GPU for speed)
python pipeline.py example_mosquito_record_test.pdf --mode whole-page --model llama3.2-vision

# Use llava (smaller, also needs GPU)
python pipeline.py example_mosquito_record_test.pdf --mode whole-page --model llava

# EasyOCR — no Ollama needed, works on CPU
python pipeline.py example_mosquito_record_test.pdf --mode easyocr
```
