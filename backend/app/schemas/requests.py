from pydantic import BaseModel, Field
from typing import List, Optional


class TextRequest(BaseModel):
    document: str = Field(..., min_length=1)


class TokensRequest(BaseModel):
    tokens: List[str]

class IndexRequest(BaseModel):
    corpus_path: Optional[str] = None
