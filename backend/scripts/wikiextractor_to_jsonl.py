import json
from pathlib import Path


INPUT_DIR = Path("../../data/raw/enwiki_extracted")
OUTPUT_DIR = Path("../../data/raw/enwiki_extracted.jsonl")


def make_url(title: str, lang: str = "en") -> str:
    # URL simplificada
    # Nota: Wikipedia utiliza espacios en las URLs.
    return f"https://{lang}.wikipedia.org/wiki/" + title.replace(" ", "_")


with OUTPUT_DIR.open("w", encoding="utf-8") as out:
    for path in INPUT_DIR.rglob("*"):
        if not path.is_file():
            continue

        # Los ficheros de WikiExtractor suelen no tener extensión o ser "wiki_*"
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                doc_id = str(obj.get("id", ""))
                title = obj.get("title", "")
                text = obj.get("text", "")

                if not doc_id or not text:
                    continue

                out_obj = {
                    "doc_id": doc_id,
                    "title": title,
                    "text": text,
                    "url": make_url(title, "en"),
                }
                out.write(json.dumps(out_obj, ensure_ascii=False) + "\n")

print(f"OK: generado {OUTPUT_DIR}")
