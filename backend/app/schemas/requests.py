from pydantic import BaseModel, Field
from typing import List, Optional


class TextRequest(BaseModel):
    document: str = Field(..., min_length=1)


class TokensRequest(BaseModel):
    tokens: List[str]


class WeightTermsRequest(BaseModel):
    terms: List[str]


class SelectTermsRequest(BaseModel):
    weighted_terms: List[dict]


class IndexRequest(BaseModel):
    corpus_path: Optional[str] = None
