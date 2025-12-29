from typing import List, Dict, Tuple, Optional
import json
import math
import sqlite3
from pathlib import Path
from collections import defaultdict


class SearchEngine:
    def __init__(self, index_path: Path):
        self.index_path = index_path
        self._postings_f = None
        self._doc_store_f = None
        self._doc_db = None
        self._load()

    def _load(self):
        if self.index_path.name != "index.meta.json":
            raise ValueError("Solo se soporta index.meta.json (formato block).")
        meta = json.loads(self.index_path.read_text())
        if meta.get("format") != "block":
            raise ValueError("Formato de indice no soportado.")
        self._load_block(meta)

    def _load_block(self, meta: dict):
        base = self.index_path.parent
        self.N = meta["N"]
        self._postings_path = base / meta["postings_path"]
        self._terms_index = json.loads((base / meta["terms_index_path"]).read_text())
        self._doc_store_path = base / meta["doc_store_path"]
        self._doc_index_type = meta.get("doc_index_type", "json")
        if self._doc_index_type == "sqlite":
            self._doc_db = sqlite3.connect(base / meta["doc_index_path"])
        else:
            self._doc_index = json.loads((base / meta["doc_index_path"]).read_text())
        self._postings_f = self._postings_path.open("rb")
        self._doc_store_f = self._doc_store_path.open("rb")

    def search(
        self, query_terms: List[str], top_k: int = 10
    ) -> List[Tuple[str, float]]:
        scores = defaultdict(float)
        for t in query_terms:
            postings = self._read_postings(t)
            if not postings:
                continue
            df = len(postings)
            idf = math.log((self.N + 1) / (df + 1)) + 1.0
            for doc_id, tf in postings:
                scores[doc_id] += float(tf) * idf
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        return ranked

    def get_doc_meta(self, doc_id: str) -> Dict[str, Optional[str]]:
        if self._doc_store_f is None:
            return {}
        offset = None
        if self._doc_index_type == "sqlite":
            if self._doc_db is None:
                return {}
            row = self._doc_db.execute(
                "SELECT offset FROM doc_index WHERE doc_id = ?",
                (str(doc_id),),
            ).fetchone()
            if row:
                offset = row[0]
        else:
            offset = self._doc_index.get(str(doc_id))
        if offset is None:
            return {}
        self._doc_store_f.seek(offset)
        line = self._doc_store_f.readline()
        if not line:
            return {}
        try:
            meta = json.loads(line)
            if isinstance(meta, dict):
                meta.pop("doc_id", None)
            return meta
        except json.JSONDecodeError:
            return {}

    def _read_postings(self, term: str):
        if self._postings_f is None:
            return []
        entry = self._terms_index.get(term)
        if not entry:
            return []
        offset, length = entry
        self._postings_f.seek(offset)
        line = self._postings_f.read(length)
        if not line:
            return []
        try:
            _, postings_json = line.rstrip(b"\n").split(b"\t", 1)
        except ValueError:
            return []
        return json.loads(postings_json)

    def __del__(self):
        if self._postings_f is not None:
            try:
                self._postings_f.close()
            except OSError:
                pass
        if self._doc_store_f is not None:
            try:
                self._doc_store_f.close()
            except OSError:
                pass
        if self._doc_db is not None:
            try:
                self._doc_db.close()
            except OSError:
                pass
