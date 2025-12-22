from fastapi import APIRouter, HTTPException
from app.schemas.requests import (
    TextRequest,
    TokensRequest,
    WeightTermsRequest,
    SelectTermsRequest,
    IndexRequest,
)
from app.schemas.responses import (
    NormalizedTextResponse,
    TokensResponse,
    WeightedTermsResponse,
    SelectedTermsResponse,
    IndexBuildResponse,
    SearchResponse,
    SearchResult,
)
from app.core.config import settings
from app.services import preprocess
from app.services.indexer import build_tfidf_index, save_index
from app.services.searcher import SearchEngine
from app.storage.paths import ensure_dirs, index_file_path
from app.storage.jsonl import read_jsonl
from pathlib import Path


router = APIRouter()


@router.post("/lexical_analysis", response_model=NormalizedTextResponse)
def lexical_analysis(req: TextRequest):
    normalized = preprocess.lexical_analysis(req.document)
    return {"normalized": normalized}


@router.post("/tokenize", response_model=TokensResponse)
def tokenize(req: TextRequest):
    normalized = preprocess.lexical_analysis(req.document)
    tokens = preprocess.tokenize(normalized)
    return {"tokens": tokens}


@router.post("/remove_stopwords", response_model=TokensResponse)
def remove_stopwords(req: TokensRequest):
    tokens = preprocess.remove_stopwords(req.tokens, language=settings.LANGUAGE)
    return {"tokens": tokens}


@router.post("/lemmatize", response_model=TokensResponse)
def lemmatize(req: TokensRequest):
    tokens = preprocess.lematize_or_stem(req.tokens, language=settings.LANGUAGE)
    return {"tokens": tokens}


@router.post("/weight_terms", response_model=WeightedTermsResponse)
def weight_terms(req: WeightTermsRequest):
    # stub: pesos dummy (en el indexado real se calcula TF-IDF)
    weighted = [{"term": t, "weight": 1.0} for t in req.terms]
    return {"weighted_terms": weighted}


@router.post("/select_terms", response_model=SelectedTermsResponse)
def select_terms(req: SelectTermsRequest):
    # stub: selecciona top por weight
    sorted_terms = sorted(
        req.weighted_terms, key=lambda x: x.get("weight", 0), reverse=True
    )
    selected = [x["term"] for x in sorted_terms[:50] if "term" in x]
    return {"selected_terms": selected}


@router.post("/index", response_model=IndexBuildResponse)
def index_documents(req: IndexRequest):
    """
    Indexación desde un corpus JSONL:
    Cada línea: {"doc_id": "...", "title": "...", "text": "...", "url": "..."}
    """
    ensure_dirs()
    corpus_path = (
        Path(req.corpus_path)
        if req.corpus_path
        else (settings.RAW_DIR / "corpus.jsonl")
    )
    if not corpus_path.exists():
        raise HTTPException(status_core=404, detail=f"No existe corpus: {corpus_path}")

    docs = []
    for d in read_jsonl(corpus_path):
        text = preprocess.lexical_analysis(d.get("text", ""))
        toks = preprocess.tokenize(text)
        toks = preprocess.remove_stopwords(toks, language=settings.LANGUAGE)
        toks = preprocess.filter_meaningful(toks, min_len=settings.MIN_TOKEN_LEN)
        toks = preprocess.lematize_or_stem(toks, language=settings.LANGUAGE)

        docs.append(
            {
                "doc_id": d.get("doc_id"),
                "title": d.get("title"),
                "url": d.get("url"),
                "snippet": (d.get("text", "")[:240] if d.get("text") else None),
                "text": d.get("text"),
                "terms": toks,
            }
        )

    art = build_tfidf_index(docs)
    out = save_index(art, settings.INDEX_DIR)

    return {
        "ok": True,
        "indexed_docs": art.N,
        "vocab_size": len(art.df),
        "index_path": str(out),
        "extra": {"corpus_path": str(corpus_path)},
    }


@router.get("/search", response_model=SearchResponse)
def search(query: str):
    idx_path = index_file_path()
    if not idx_path.exists():
        raise HTTPException(
            status_code=400, detail="Índice no encontrado. Ejecuta POST /index primero."
        )

    q = preprocess.lexical_analysis(query)
    toks = preprocess.tokenize(q)
    toks = preprocess.remove_stopwords(toks, language=settings.LANGUAGE)
    toks = preprocess.filter_meaningful(toks, min_len=settings.MIN_TOKEN_LEN)
    toks = preprocess.lematize_or_stem(toks, language=settings.LANGUAGE)

    engine = SearchEngine(idx_path)
    ranked = engine.search(toks, top_k=settings.TOP_K)

    results = []
    for doc_id, score in ranked:
        meta = engine.doc_store.get(doc_id, {})
        results.append(
            SearchResult(
                doc_id=doc_id,
                score=float(score),
                title=meta.get("title"),
                snippet=meta.get("snippet"),
                url=meta.get("url"),
            )
        )

    return {"query": query, "results": results}
