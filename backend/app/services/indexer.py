from __future__ import annotations
from dataclasses import dataclass
from typing import List
from collections import Counter, defaultdict
import heapq
import json
import sqlite3
import shutil
from pathlib import Path
from urllib.parse import urlparse
from app.core.config import settings
from app.services import preprocess
from app.services.langdetect import detect_language


POSTINGS_NAME = "index.postings"
TERMS_INDEX_NAME = "index.terms.json"
DOC_STORE_NAME = "doc_store.jsonl"
DOC_STORE_PARTS_DIRNAME = "doc_store_parts"
DOC_INDEX_SQLITE_NAME = "doc_store.sqlite"
META_NAME = "index.meta.json"
BLOCK_DIRNAME = "blocks"
TF_DECIMALS = 6


@dataclass
class BlockIndexResult:
    N: int
    vocab_size: int
    index_path: Path


def _make_doc_uid(doc_id: str, url: str | None, namespace: str | None = None) -> str:
    doc_id = doc_id or ""
    if not namespace and url:
        try:
            host = urlparse(url).hostname
        except ValueError:
            host = None
        namespace = host or None
    if namespace:
        if doc_id:
            return f"{namespace}:{doc_id}"
        if url:
            return f"{namespace}:{url}"
    return doc_id or (url or "")


def _read_block_line(handle):
    line = handle.readline()
    if not line:
        return None, None
    term_bytes, postings_bytes = line.rstrip(b"\n").split(b"\t", 1)
    term = term_bytes.decode("utf-8")
    postings = json.loads(postings_bytes)
    return term, postings


def _iter_jsonl_range(path: Path, start: int, end: int):
    with path.open("rb") as f:
        f.seek(start)
        while f.tell() < end:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _iter_batch_docs(batch):
    if (
        isinstance(batch, tuple)
        and len(batch) == 3
        and isinstance(batch[0], (str, Path))
    ):
        corpus_path, start, end = batch
        yield from _iter_jsonl_range(Path(corpus_path), int(start), int(end))
        return
    yield from batch


def spimi_worker(args: tuple) -> dict:
    batch, out_dir, batch_id = args
    out_dir = Path(out_dir)
    block_dir = out_dir / BLOCK_DIRNAME
    doc_store_dir = out_dir / DOC_STORE_PARTS_DIRNAME
    block_dir.mkdir(parents=True, exist_ok=True)
    doc_store_dir.mkdir(parents=True, exist_ok=True)

    inverted = defaultdict(list)
    doc_store_path = doc_store_dir / f"doc_store_{batch_id:06d}.jsonl"
    docs_count = 0

    with doc_store_path.open("wb") as ds_f:
        for d in _iter_batch_docs(batch):
            raw_text = d.get("text", "") or ""
            normalized = preprocess.lexical_analysis(raw_text)

            lang, _conf = detect_language(normalized)

            toks = preprocess.tokenize(normalized)
            toks = preprocess.remove_stopwords(toks, language=lang)
            toks = preprocess.filter_meaningful(toks, min_len=settings.MIN_TOKEN_LEN)
            toks = preprocess.lemmatize_or_stem(toks, language=lang)

            raw_doc_id = d.get("doc_id")
            raw_doc_id = "" if raw_doc_id is None else str(raw_doc_id)
            doc_uid = _make_doc_uid(
                raw_doc_id,
                d.get("url"),
                d.get("source") or d.get("lang"),
            )
            c = Counter(toks)
            doc_len = sum(c.values()) or 1
            for term, freq in c.items():
                tf = round(freq / doc_len, TF_DECIMALS)
                inverted[term].append((doc_uid, float(tf)))

            meta = {
                "doc_id": raw_doc_id,
                "doc_uid": doc_uid,
                "title": d.get("title"),
                "url": d.get("url"),
                "snippet": (raw_text[:240] if raw_text else None),
            }
            line = (
                json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                )
                + b"\n"
            )
            ds_f.write(line)
            docs_count += 1

    block_path = block_dir / f"block_{batch_id:06d}.jsonl"
    with block_path.open("wb") as f:
        for term in sorted(inverted.keys()):
            postings = inverted[term]
            line = (
                f"{term}\t"
                + json.dumps(postings, ensure_ascii=False, separators=(",", ":"))
                + "\n"
            )
            f.write(line.encode("utf-8"))

    return {
        "block_path": str(block_path),
        "doc_store_path": str(doc_store_path),
        "docs": docs_count,
    }


def finalize_spimi(
    block_paths: List[Path], doc_store_paths: List[Path], out_dir: Path, total_docs: int
) -> BlockIndexResult:
    out_dir.mkdir(parents=True, exist_ok=True)
    sorted_blocks = sorted(block_paths)
    sorted_doc_parts = sorted(doc_store_paths)

    doc_store_path = _merge_doc_store_parts(sorted_doc_parts, out_dir)
    doc_index_path = _build_doc_index_sqlite(doc_store_path, out_dir)
    vocab_size = _merge_blocks_spimi(sorted_blocks, out_dir, total_docs)
    meta_path = _write_spimi_meta(out_dir, total_docs, vocab_size, doc_index_path)

    if not settings.INDEX_KEEP_BLOCKS:
        for path in sorted_blocks:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        try:
            (out_dir / BLOCK_DIRNAME).rmdir()
        except OSError:
            pass

        for path in sorted_doc_parts:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        try:
            (out_dir / DOC_STORE_PARTS_DIRNAME).rmdir()
        except OSError:
            pass

    return BlockIndexResult(total_docs, vocab_size, meta_path)


def _merge_doc_store_parts(doc_store_paths: List[Path], out_dir: Path) -> Path:
    out_path = out_dir / DOC_STORE_NAME
    with out_path.open("wb") as out_f:
        for path in doc_store_paths:
            with path.open("rb") as f:
                shutil.copyfileobj(f, out_f)
    return out_path


def _build_doc_index_sqlite(doc_store_path: Path, out_dir: Path) -> Path:
    sqlite_path = out_dir / DOC_INDEX_SQLITE_NAME
    if sqlite_path.exists():
        sqlite_path.unlink()

    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=OFF")
    cur.execute("PRAGMA synchronous=OFF")
    cur.execute("PRAGMA temp_store=MEMORY")
    cur.execute("CREATE TABLE doc_index (doc_id TEXT PRIMARY KEY, offset INTEGER)")

    batch = []
    with doc_store_path.open("rb") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            doc_key = obj.get("doc_uid") or obj.get("doc_id")
            if doc_key is None:
                continue
            batch.append((str(doc_key), int(offset)))
            if len(batch) >= 5000:
                cur.executemany(
                    "INSERT OR REPLACE INTO doc_index (doc_id, offset) VALUES (?, ?)",
                    batch,
                )
                conn.commit()
                batch.clear()

    if batch:
        cur.executemany(
            "INSERT OR REPLACE INTO doc_index (doc_id, offset) VALUES (?, ?)",
            batch,
        )
        conn.commit()

    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_id ON doc_index(doc_id)")
    conn.commit()
    conn.close()
    return sqlite_path


def _merge_blocks_spimi(block_paths: List[Path], out_dir: Path, total_docs: int) -> int:
    df_counts = Counter()
    for path in block_paths:
        with path.open("rb") as f:
            for line in f:
                term_bytes, postings_bytes = line.rstrip(b"\n").split(b"\t", 1)
                term = term_bytes.decode("utf-8")
                postings = json.loads(postings_bytes)
                df_counts[term] += len(postings)

    min_df = getattr(settings, "MIN_DF", 1)
    max_df_ratio = getattr(settings, "MAX_DF_RATIO", 1.0)
    max_df = int(max_df_ratio * total_docs) if total_docs > 0 else 0
    if max_df < min_df:
        max_df = min_df

    postings_path = out_dir / POSTINGS_NAME
    terms_index_path = out_dir / TERMS_INDEX_NAME
    terms_index = {}
    handles = [path.open("rb") for path in block_paths]
    heap = []

    try:
        for i, handle in enumerate(handles):
            term, postings = _read_block_line(handle)
            if term is not None:
                heapq.heappush(heap, (term, i, postings))

        current_term = None
        current_allowed = False
        first_posting = True
        current_offset = 0

        with postings_path.open("wb") as out_f:
            while heap:
                term, i, postings = heapq.heappop(heap)
                if term != current_term:
                    if current_term is not None and current_allowed:
                        out_f.write(b"]\n")
                        length = out_f.tell() - current_offset
                        terms_index[current_term] = [current_offset, length]

                    current_term = term
                    term_df = df_counts.get(term, 0)
                    current_allowed = min_df <= term_df <= max_df
                    if current_allowed:
                        current_offset = out_f.tell()
                        out_f.write(f"{term}\t[".encode("utf-8"))
                        first_posting = True

                if current_allowed:
                    for doc_id, tf in postings:
                        if not first_posting:
                            out_f.write(b",")
                        entry = json.dumps(
                            [doc_id, tf],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ).encode("utf-8")
                        out_f.write(entry)
                        first_posting = False

                next_term, next_postings = _read_block_line(handles[i])
                if next_term is not None:
                    heapq.heappush(heap, (next_term, i, next_postings))

            if current_term is not None and current_allowed:
                out_f.write(b"]\n")
                length = out_f.tell() - current_offset
                terms_index[current_term] = [current_offset, length]
    finally:
        for handle in handles:
            handle.close()

    terms_index_path.write_text(
        json.dumps(terms_index, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    return len(terms_index)


def _write_spimi_meta(
    out_dir: Path, total_docs: int, vocab_size: int, doc_index_path: Path
) -> Path:
    meta_path = out_dir / META_NAME
    meta = {
        "format": "block",
        "N": total_docs,
        "vocab_size": vocab_size,
        "postings_path": POSTINGS_NAME,
        "terms_index_path": TERMS_INDEX_NAME,
        "doc_store_path": DOC_STORE_NAME,
        "doc_index_path": doc_index_path.name,
        "doc_index_type": "sqlite",
    }
    meta_path.write_text(
        json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    return meta_path
