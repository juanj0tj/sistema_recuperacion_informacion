from fastapi import APIRouter, HTTPException
from typing import Optional
from app.schemas.requests import TextRequest, TokensRequest, IndexRequest
from app.schemas.responses import (
    NormalizedTextResponse,
    TokensResponse,
    IndexBuildResponse,
    SearchResponse,
    SearchResult,
)
from app.core.config import settings
from app.services import preprocess
from app.services.indexer import spimi_worker, finalize_spimi
from app.services.searcher import SearchEngine
from app.services.langdetect import detect_language, SUPPORTED
from app.storage.paths import ensure_dirs, index_file_path
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
import logging
import time


router = APIRouter()
logger = logging.getLogger("ri.index")
logging.basicConfig(level=logging.INFO)


def _iter_batch_offsets(path: Path, batch_size: int):
    with path.open("rb") as f:
        batch_start = f.tell()
        count = 0
        while True:
            line = f.readline()
            if not line:
                break
            count += 1
            if count >= batch_size:
                batch_end = f.tell()
                yield (batch_start, batch_end)
                batch_start = batch_end
                count = 0
        if count:
            yield (batch_start, f.tell())


def _build_executor() -> ProcessPoolExecutor:
    kwargs = {"max_workers": settings.INDEX_WORKERS}
    max_tasks = getattr(settings, "INDEX_MAX_TASKS_PER_CHILD", 0)
    if max_tasks and max_tasks > 0:
        kwargs["max_tasks_per_child"] = max_tasks
    try:
        return ProcessPoolExecutor(**kwargs)
    except TypeError:
        if "max_tasks_per_child" in kwargs:
            logger.warning(
                "max_tasks_per_child no soportado por esta version de Python; ignorando"
            )
            kwargs.pop("max_tasks_per_child", None)
            return ProcessPoolExecutor(**kwargs)
        raise


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
    tokens = preprocess.remove_stopwords(req.tokens, language=settings.DEFAULT_LANGUAGE)
    return {"tokens": tokens}


@router.post("/lemmatize", response_model=TokensResponse)
def lemmatize(req: TokensRequest):
    tokens = preprocess.lematize_or_stem(req.tokens, language=settings.DEFAULT_LANGUAGE)
    return {"tokens": tokens}


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
        raise HTTPException(status_code=404, detail=f"No existe corpus: {corpus_path}")

    LOG_EVERY = 50_000
    start = time.time()

    block_paths = []
    doc_store_paths = []
    total_docs = 0
    next_log = LOG_EVERY

    corpus_path_str = str(corpus_path)
    batch_iter = enumerate(
        _iter_batch_offsets(corpus_path, settings.INDEX_BLOCK_DOCS),
        start=1,
    )
    max_in_flight = settings.INDEX_MAX_IN_FLIGHT
    if max_in_flight < 1:
        max_in_flight = max(1, settings.INDEX_WORKERS * 2)

    with _build_executor() as executor:
        in_flight = set()
        while True:
            while len(in_flight) < max_in_flight:
                try:
                    batch_id, batch = next(batch_iter)
                except StopIteration:
                    break
                in_flight.add(
                    executor.submit(
                        spimi_worker,
                        (
                            (corpus_path_str, batch[0], batch[1]),
                            str(settings.INDEX_DIR),
                            batch_id,
                        ),
                    )
                )

            if not in_flight:
                break

            done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
            for fut in done:
                result = fut.result()
                total_docs += result["docs"]
                block_paths.append(Path(result["block_path"]))
                doc_store_paths.append(Path(result["doc_store_path"]))

                if total_docs >= next_log:
                    elapsed = time.time() - start
                    rate = total_docs / elapsed if elapsed > 0 else 0.0
                    logger.info(
                        "Indexando... %d documentos (%.1f docs/s)",
                        total_docs,
                        rate,
                    )
                    next_log += LOG_EVERY

    logger.info("Lectura + preprocesado completados. Total documentos=%d", total_docs)

    result = finalize_spimi(
        block_paths, doc_store_paths, settings.INDEX_DIR, total_docs
    )
    out = result.index_path

    elapsed_total = time.time() - start
    logger.info(
        "Indexación finalizada: N=%d vocab=%d tiempo=%.1fs",
        result.N,
        result.vocab_size,
        elapsed_total,
    )

    return {
        "ok": True,
        "indexed_docs": result.N,
        "vocab_size": result.vocab_size,
        "index_path": str(out),
        "extra": {"corpus_path": str(corpus_path)},
    }


@router.get("/search", response_model=SearchResponse)
def search(query: str, default_language: Optional[str] = None):
    idx_path = index_file_path()
    if not idx_path.exists():
        raise HTTPException(
            status_code=400, detail="Índice no encontrado. Ejecuta POST /index primero."
        )

    q = preprocess.lexical_analysis(query)
    qlang, qconf = detect_language(q)

    if default_language:
        default_language = default_language.strip().lower()
        if default_language not in SUPPORTED:
            raise HTTPException(status_code=422, detail="Idioma no soportado.")

    if qlang == "unknown":
        # fallback
        qlang = default_language or settings.DEFAULT_QUERY_LANGUAGE

    toks = preprocess.tokenize(q)
    toks = preprocess.remove_stopwords(toks, language=qlang)
    toks = preprocess.filter_meaningful(toks, min_len=settings.MIN_TOKEN_LEN)
    toks = preprocess.lemmatize_or_stem(toks, language=qlang)

    engine = SearchEngine(idx_path)
    ranked = engine.search(toks, top_k=settings.TOP_K)

    results = []
    for doc_uid, score in ranked:
        meta = engine.get_doc_meta(doc_uid)
        doc_id = meta.get("doc_id")
        if not doc_id:
            doc_id = doc_uid
        results.append(
            SearchResult(
                doc_id=str(doc_id),
                score=float(score),
                title=meta.get("title"),
                snippet=meta.get("snippet"),
                url=meta.get("url"),
            )
        )

    return {"query": query, "results": results}
