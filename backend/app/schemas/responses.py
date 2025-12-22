from pydantic import BaseModel
from typing import List, Optional, Any


class NormalizedTextResponse(BaseModel):
    normalized: str


class TokensResponse(BaseModel):
    tokens: List[str]


class WeightedTermsResponse(BaseModel):
    weighted_terms: List[dict]


class SelectedTermsResponse(BaseModel):
    selected_terms: List[str]


class IndexBuildResponse(BaseModel):
    ok: bool
    indexed_docs: int
    vocab_size: int
    index_path: Optional[str] = None
    extra: Optional[Any] = None


class SearchResult(BaseModel):
    doc_id: str
    score: float
    title: Optional[str] = None
    snippet: Optional[str] = None
    url: Optional[str] = None


class SearchResponse(BaseModel):
    query: str
    results: List[SearchResult]
