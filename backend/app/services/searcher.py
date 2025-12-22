from typing import List, Dict, Tuple
import json
from pathlib import Path
from collections import defaultdict


class SearchEngine:
    def __init__(self, index_path: Path):
        self.index_path = index_path
        self._load()

    def _load(self):
        data = json.loads(self.index_path.read_text())
        self.N = data["N"]
        self.df = data["df"]
        self.doc_store = data["doc_store"]
        self.inverted = data["inverted_index"]

    def search(
        self, query_terms: List[str], top_k: int = 10
    ) -> List[Tuple[str, float]]:
        scores = defaultdict(float)
        for t in query_terms:
            postings = self.inverted.get(t, [])
            for doc_id, w in postings:
                scores[doc_id] += float(w)
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        return ranked
