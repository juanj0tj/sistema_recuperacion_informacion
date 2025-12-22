from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple
from collections import Counter, defaultdict
import math
import json
from pathlib import Path


@dataclass
class IndexArtifacts:
    inverted_index: Dict[str, List[Tuple[str, float]]]
    doc_store: Dict[str, dict]
    df: Dict[str, int]
    N: int


def build_tfidf_index(docs: List[dict]) -> IndexArtifacts:
    N = len(docs)
    df = Counter()
    tfs = {}
    doc_store = {}

    for d in docs:
        doc_id = str(d["doc_id"])
        terms = d["terms"]  # Ya preprocesados
        c = Counter(terms)
        tfs[doc_id] = c
        for term in c.keys():
            df[term] += 1
        doc_store[doc_id] = {k: d.get(k) for k in ["title", "url", "snippet", "text"]}

    idf = {t: (math.log((N + 1) / (df[t] + 1)) + 1.0) for t in df.keys()}

    inverted = defaultdict(list)
    for doc_id, c in tfs.items():
        doc_len = sum(c.values()) or 1
        for term, freq in c.items():
            tf = freq / doc_len
            tfidf = tf * idf[term]
            inverted[term].append((doc_id, float(tfidf)))

    # Ordenar postings por peso descendente
    for term in inverted.keys():
        inverted[term].sort(key=lambda x: x[1], reverse=True)

    return IndexArtifacts(dict(inverted), dict(doc_store), dict(df), N)


def save_index(art: IndexArtifacts, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "index.json"
    payload = {
        "N": art.N,
        "df": art.df,
        "doc_store": art.doc_store,
        "inverted_index": art.inverted_index,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False))
    return path
