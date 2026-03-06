from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
import re
import socket
import ssl
import sqlite3
import subprocess
import tempfile
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BASE_INDEX_URL = "https://www.diputados.gob.mx/LeyesBiblio/index.htm"
BASE_SITE_URL = "https://www.diputados.gob.mx/LeyesBiblio/"
GOBIERNOS_URL = "https://www.diputados.gob.mx/LeyesBiblio/gobiernos.htm"
ACTUAL_ULTIMA_URL = "https://www.diputados.gob.mx/LeyesBiblio/actual/ultima.htm"
USER_AGENT = "LegalOpsRAG/1.1 (+local-mvp)"

PRIORITY_LAW_KEYS = {
    "ccf",
    "ccom",
    "cfpc",
    "cnpcf",
    "lfpdppp",
    "lgpdppso",
    "laassp",
    "lapp",
}

STATE_ALIASES = {
    "aguascalientes": ["aguascalientes"],
    "baja california": ["baja california"],
    "baja california sur": ["baja california sur"],
    "campeche": ["campeche"],
    "chiapas": ["chiapas"],
    "chihuahua": ["chihuahua"],
    "ciudad de mexico": ["ciudad de mexico", "cdmx", "distrito federal"],
    "coahuila": ["coahuila", "coahuila de zaragoza"],
    "colima": ["colima"],
    "durango": ["durango"],
    "guanajuato": ["guanajuato"],
    "guerrero": ["guerrero"],
    "hidalgo": ["hidalgo"],
    "jalisco": ["jalisco"],
    "mexico": ["estado de mexico", "edomex"],
    "michoacan": ["michoacan"],
    "morelos": ["morelos"],
    "nayarit": ["nayarit"],
    "nuevo leon": ["nuevo leon"],
    "oaxaca": ["oaxaca"],
    "puebla": ["puebla"],
    "queretaro": ["queretaro"],
    "quintana roo": ["quintana roo"],
    "san luis potosi": ["san luis potosi"],
    "sinaloa": ["sinaloa"],
    "sonora": ["sonora"],
    "tabasco": ["tabasco"],
    "tamaulipas": ["tamaulipas"],
    "tlaxcala": ["tlaxcala"],
    "veracruz": ["veracruz"],
    "yucatan": ["yucatan"],
    "zacatecas": ["zacatecas"],
}

LEGAL_URL_HINTS = [
    "ley",
    "leyes",
    "legisl",
    "codigo",
    "constitucion",
    "norma",
    "marco",
    "reglamento",
    "estatuto",
    "decreto",
    "jurid",
]

BINARY_EXTENSIONS = {".doc", ".docx", ".pdf", ".txt", ".rtf"}
SKIP_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".css",
    ".js",
    ".ico",
    ".zip",
    ".rar",
    ".mp4",
    ".mp3",
    ".woff",
    ".woff2",
}

SKIP_DOMAINS = {
    "facebook.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "instagram.com",
    "whatsapp.com",
}

DOMAIN_JURISDICTION_OVERRIDES = {
    "cbcs.gob.mx": "baja california sur",
    "congresobc.gob.mx": "baja california",
    "legislativoedomex.gob.mx": "mexico",
    "congresoags.gob.mx": "aguascalientes",
    "congresocoahuila.gob.mx": "coahuila",
    "congresocol.gob.mx": "colima",
    "congresocdmx.gob.mx": "ciudad de mexico",
}


@dataclass
class LawReference:
    law_key: str
    law_name: str
    ref_url: str
    doc_url: str


@dataclass
class StateEntryPoint:
    jurisdiction: str
    entry_url: str
    source_kind: str
    label: str


class LegalRAG:
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.corpus_dir = self.base_dir / "data" / "legal_corpus"
        self.raw_doc_dir = self.corpus_dir / "raw_doc"
        self.raw_txt_dir = self.corpus_dir / "raw_txt"
        self.state_raw_doc_dir = self.corpus_dir / "state_raw_doc"
        self.state_raw_txt_dir = self.corpus_dir / "state_raw_txt"
        self.db_path = self.corpus_dir / "legal_rag.sqlite"
        self.meta_path = self.corpus_dir / "meta.json"

        self.corpus_dir.mkdir(parents=True, exist_ok=True)
        self.raw_doc_dir.mkdir(parents=True, exist_ok=True)
        self.raw_txt_dir.mkdir(parents=True, exist_ok=True)
        self.state_raw_doc_dir.mkdir(parents=True, exist_ok=True)
        self.state_raw_txt_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_db()

    def status(self) -> dict[str, Any]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        source_count = cur.execute("SELECT COUNT(*) AS c FROM sources").fetchone()["c"]
        chunk_count = cur.execute("SELECT COUNT(*) AS c FROM chunks").fetchone()["c"]
        latest = cur.execute("SELECT MAX(indexed_at) AS t FROM sources").fetchone()["t"]

        scope_rows = cur.execute(
            "SELECT scope, COUNT(*) AS c FROM sources GROUP BY scope ORDER BY scope"
        ).fetchall()
        jurisdiction_rows = cur.execute(
            "SELECT jurisdiction, COUNT(*) AS c FROM sources WHERE scope='state' GROUP BY jurisdiction ORDER BY jurisdiction"
        ).fetchall()
        conn.close()

        meta = {}
        if self.meta_path.exists():
            meta = json.loads(self.meta_path.read_text(encoding="utf-8"))

        return {
            "sources": source_count,
            "chunks": chunk_count,
            "latest_indexed_at": latest,
            "by_scope": {row["scope"]: row["c"] for row in scope_rows},
            "by_state_jurisdiction": {row["jurisdiction"]: row["c"] for row in jurisdiction_rows},
            "meta": meta,
            "db_path": str(self.db_path),
        }

    def rebuild_from_diputados(
        self,
        *,
        limit: int | None = None,
        delay_seconds: float = 0.15,
    ) -> dict[str, Any]:
        law_refs = self._discover_law_references()
        if limit is not None:
            law_refs = law_refs[: max(0, limit)]

        stats = {
            "scope": "federal",
            "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "discovered": len(law_refs),
            "indexed": 0,
            "skipped": 0,
            "errors": [],
        }

        for idx, law in enumerate(law_refs, start=1):
            try:
                self._index_one_federal_law(law)
                stats["indexed"] += 1
            except Exception as exc:  # pragma: no cover
                stats["errors"].append({"law_key": law.law_key, "error": str(exc)})
            time.sleep(delay_seconds)
            if idx % 25 == 0:
                self._save_meta({"last_progress_federal": idx, "discovered_federal": len(law_refs)})

        stats["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        stats["ok"] = len(stats["errors"]) == 0
        self._save_meta({"last_rebuild_federal": stats, "index_url": BASE_INDEX_URL})
        return stats

    def rebuild_federal_subset(
        self,
        law_keys: set[str],
        *,
        delay_seconds: float = 0.08,
    ) -> dict[str, Any]:
        refs = self._discover_law_references()
        selected = [ref for ref in refs if ref.law_key in law_keys]

        stats = {
            "scope": "federal_subset",
            "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "requested": len(law_keys),
            "discovered": len(selected),
            "indexed": 0,
            "errors": [],
        }

        for law in selected:
            try:
                self._index_one_federal_law(law)
                stats["indexed"] += 1
            except Exception as exc:  # pragma: no cover
                stats["errors"].append({"law_key": law.law_key, "error": str(exc)})
            time.sleep(delay_seconds)

        stats["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        stats["ok"] = len(stats["errors"]) == 0
        self._save_meta({"last_rebuild_federal_subset": stats})
        return stats

    def rebuild_state_laws_from_gobiernos(
        self,
        *,
        limit_states: int | None = None,
        include_cdmx: bool = True,
        per_entry_max_pages: int = 30,
        per_page_link_cap: int = 16,
        delay_seconds: float = 0.1,
    ) -> dict[str, Any]:
        entrypoints = self._discover_state_entrypoints()

        if limit_states is not None:
            state_names = sorted({e.jurisdiction for e in entrypoints if e.jurisdiction != "ciudad de mexico"})
            keep_states = set(state_names[: max(0, limit_states)])
            entrypoints = [
                e
                for e in entrypoints
                if (e.jurisdiction in keep_states) or (include_cdmx and e.jurisdiction == "ciudad de mexico")
            ]
        elif not include_cdmx:
            entrypoints = [e for e in entrypoints if e.jurisdiction != "ciudad de mexico"]

        stats = {
            "scope": "state",
            "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "entrypoints": len(entrypoints),
            "processed": 0,
            "indexed_sources": 0,
            "indexed_chunks": 0,
            "errors": [],
        }

        for idx, entry in enumerate(entrypoints, start=1):
            try:
                partial = self._crawl_state_entrypoint(
                    entry,
                    max_pages=per_entry_max_pages,
                    per_page_link_cap=per_page_link_cap,
                )
                stats["indexed_sources"] += partial["indexed_sources"]
                stats["indexed_chunks"] += partial["indexed_chunks"]
                stats["processed"] += 1
                if partial["errors"]:
                    stats["errors"].extend(partial["errors"])
            except Exception as exc:  # pragma: no cover
                stats["errors"].append(
                    {
                        "jurisdiction": entry.jurisdiction,
                        "entry_url": entry.entry_url,
                        "error": str(exc),
                    }
                )
            time.sleep(delay_seconds)
            if idx % 10 == 0:
                self._save_meta({"last_progress_state": idx, "discovered_state": len(entrypoints)})

        stats["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        stats["ok"] = len(stats["errors"]) == 0
        self._save_meta({"last_rebuild_state": stats, "gobiernos_url": GOBIERNOS_URL})
        return stats

    def check_federal_updates(
        self,
        *,
        refresh_on_change: bool = True,
        force: bool = False,
    ) -> dict[str, Any]:
        html_content = self._http_get_text(ACTUAL_ULTIMA_URL)
        current_hash = hashlib.sha256(html_content.encode("utf-8", errors="ignore")).hexdigest()
        update_keys = self._extract_update_law_keys(html_content)

        meta = self._load_meta()
        previous_hash = meta.get("federal_updates_hash")
        changed = force or (current_hash != previous_hash)

        sync_result: dict[str, Any] | None = None
        if refresh_on_change and changed:
            if update_keys:
                sync_result = self.rebuild_federal_subset(update_keys)
            else:
                sync_result = {
                    "scope": "federal_subset",
                    "requested": 0,
                    "indexed": 0,
                    "errors": [],
                    "ok": True,
                }

        result = {
            "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "updates_page": ACTUAL_ULTIMA_URL,
            "hash_changed": changed,
            "updated_law_keys": sorted(update_keys),
            "updated_law_count": len(update_keys),
            "sync_result": sync_result,
        }

        self._save_meta(
            {
                "federal_updates_checked_at": result["checked_at"],
                "federal_updates_hash": current_hash,
                "federal_updates_keys": sorted(update_keys),
                "federal_updates_last_result": result,
            }
        )
        return result

    def search(
        self,
        query: str,
        top_k: int = 5,
        *,
        scope: str | None = None,
        jurisdiction: str | None = None,
    ) -> list[dict[str, Any]]:
        clean = self._fts_query(query)
        if not clean:
            return []

        normalized_query = _normalize_text(query)
        query_tokens = set(re.findall(r"[a-zA-Z0-9]{4,}", normalized_query))

        where_clauses = ["chunks_fts MATCH ?"]
        params: list[Any] = [clean]

        if scope:
            where_clauses.append("s.scope = ?")
            params.append(scope)
        if jurisdiction:
            where_clauses.append("s.jurisdiction = ?")
            params.append(_normalize_text(jurisdiction))

        limit_raw = max(top_k * 8, 25)
        params.append(limit_raw)

        sql = f"""
            SELECT
                c.chunk_id,
                c.article_label,
                c.content,
                s.law_key,
                s.law_name,
                s.ref_url,
                s.doc_url,
                s.last_reform,
                s.scope,
                s.jurisdiction,
                s.source_kind,
                bm25(chunks_fts) AS rank_score
            FROM chunks_fts
            JOIN chunks c ON c.chunk_id = chunks_fts.rowid
            JOIN sources s ON s.source_id = c.source_id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY rank_score
            LIMIT ?
        """

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        rows = cur.execute(sql, tuple(params)).fetchall()
        conn.close()

        rescored = []
        for row in rows:
            snippet = _compact(row["content"], 420)
            text_norm = _normalize_text(row["content"] or "")
            overlap = sum(1 for token in query_tokens if token in text_norm)

            priority_boost = 2.0 if row["law_key"] in PRIORITY_LAW_KEYS else 0.0
            jurisdiction_boost = 0.0
            jurisdiction_norm = _normalize_text(row["jurisdiction"] or "")
            if jurisdiction_norm and any(part in normalized_query for part in jurisdiction_norm.split()):
                jurisdiction_boost = 1.0

            bm25_score = float(row["rank_score"]) if row["rank_score"] is not None else 0.0
            hybrid_score = (-bm25_score) + overlap * 0.8 + priority_boost + jurisdiction_boost

            rescored.append(
                {
                    "law_key": row["law_key"],
                    "law_name": row["law_name"],
                    "article_label": row["article_label"],
                    "snippet": snippet,
                    "ref_url": row["ref_url"],
                    "doc_url": row["doc_url"],
                    "last_reform": row["last_reform"],
                    "scope": row["scope"],
                    "jurisdiction": row["jurisdiction"],
                    "source_kind": row["source_kind"],
                    "rank_score": bm25_score,
                    "hybrid_score": round(hybrid_score, 4),
                }
            )

        rescored.sort(key=lambda x: x["hybrid_score"], reverse=True)
        return rescored[:top_k]

    def _index_one_federal_law(self, law: LawReference) -> None:
        doc_name = f"{law.law_key}.doc"
        local_doc = self.raw_doc_dir / doc_name
        local_txt = self.raw_txt_dir / f"{law.law_key}.txt"

        if not local_doc.exists():
            binary = self._http_get_bytes(law.doc_url)
            local_doc.write_bytes(binary)

        txt_content = self._convert_doc_to_text(local_doc)
        local_txt.write_text(txt_content, encoding="utf-8")

        title = self._extract_title(txt_content) or law.law_name
        last_reform = self._extract_last_reform(txt_content)
        file_hash = hashlib.sha256(txt_content.encode("utf-8")).hexdigest()
        chunks = self._chunk_law_text(txt_content)
        if not chunks:
            return

        self._upsert_source_and_chunks(
            source_key=law.law_key,
            law_name=title,
            ref_url=law.ref_url,
            doc_url=law.doc_url,
            text_path=str(local_txt),
            content_hash=file_hash,
            last_reform=last_reform,
            scope="federal",
            jurisdiction="mexico_federal",
            source_kind="federal_law_doc",
            chunks=chunks,
        )

    def _crawl_state_entrypoint(
        self,
        entry: StateEntryPoint,
        *,
        max_pages: int,
        per_page_link_cap: int,
    ) -> dict[str, Any]:
        queue: deque[str] = deque([self._normalize_url(entry.entry_url)])
        visited: set[str] = set()
        indexed_sources = 0
        indexed_chunks = 0
        errors: list[dict[str, Any]] = []

        root_domain = _normalize_domain(urllib.parse.urlparse(entry.entry_url).netloc)

        while queue and len(visited) < max_pages:
            current_url = self._normalize_url(queue.popleft())
            if current_url in visited:
                continue
            visited.add(current_url)

            try:
                payload, content_type, final_url = self._http_get_response(current_url)
            except Exception as exc:  # pragma: no cover
                errors.append({"url": current_url, "error": str(exc)})
                continue

            final_url = self._normalize_url(final_url)
            visited.add(final_url)

            kind = self._detect_resource_kind(final_url, content_type)
            text_content = ""
            title = ""

            if kind == "html":
                html_text = self._decode_bytes(payload)
                text_content = self._html_to_text(html_text)
                title = self._extract_html_title(html_text) or f"{entry.jurisdiction} - página legal"

                for link_url in self._candidate_links_from_html(html_text, final_url, root_domain, per_page_link_cap):
                    if link_url not in visited:
                        queue.append(link_url)
            elif kind in {"doc", "docx", "pdf", "txt"}:
                text_content = self._extract_text_from_binary(payload, kind)
                filename = Path(urllib.parse.urlparse(final_url).path).name or "documento"
                title = f"{entry.jurisdiction}: {filename}"
            else:
                continue

            if not self._is_legalish(text_content):
                continue

            chunks = self._chunk_law_text(text_content)
            if not chunks:
                continue

            text_hash = hashlib.sha256(text_content.encode("utf-8", errors="ignore")).hexdigest()
            source_key = (
                f"state::{_normalize_text(entry.jurisdiction)}::"
                f"{hashlib.sha1(final_url.encode('utf-8')).hexdigest()[:20]}"
            )

            text_path = ""
            if kind in {"doc", "docx", "pdf", "txt"}:
                local_name = f"{source_key.replace(':', '_')}.txt"
                local_path = self.state_raw_txt_dir / local_name
                local_path.write_text(text_content, encoding="utf-8")
                text_path = str(local_path)

            self._upsert_source_and_chunks(
                source_key=source_key,
                law_name=_compact(title, 220),
                ref_url=entry.entry_url,
                doc_url=final_url,
                text_path=text_path,
                content_hash=text_hash,
                last_reform=None,
                scope="state",
                jurisdiction=_normalize_text(entry.jurisdiction),
                source_kind=entry.source_kind,
                chunks=chunks,
            )
            indexed_sources += 1
            indexed_chunks += len(chunks)

        return {
            "entry_url": entry.entry_url,
            "jurisdiction": entry.jurisdiction,
            "visited_pages": len(visited),
            "indexed_sources": indexed_sources,
            "indexed_chunks": indexed_chunks,
            "errors": errors,
        }

    def _discover_law_references(self) -> list[LawReference]:
        html_content = self._http_get_text(BASE_INDEX_URL)
        hrefs = re.findall(r'href="([^"]+)"', html_content, flags=re.IGNORECASE)

        references: list[LawReference] = []
        for idx, href in enumerate(hrefs):
            normalized = href.strip()
            if not normalized.lower().startswith("doc/") or not normalized.lower().endswith(".doc"):
                continue
            law_key = Path(normalized).stem.lower()

            ref_link = ""
            for back in range(idx - 1, max(-1, idx - 8), -1):
                candidate = hrefs[back].strip()
                if candidate.lower().startswith("ref/") and candidate.lower().endswith(".htm"):
                    ref_link = candidate
                    break

            law_name = self._law_name_from_key(law_key)
            if ref_link:
                law_name = Path(ref_link).stem.upper()

            references.append(
                LawReference(
                    law_key=law_key,
                    law_name=law_name,
                    ref_url=urllib.parse.urljoin(BASE_SITE_URL, ref_link) if ref_link else "",
                    doc_url=urllib.parse.urljoin(BASE_SITE_URL, normalized),
                )
            )

        dedup: dict[str, LawReference] = {}
        for ref in references:
            dedup[ref.law_key] = ref
        return sorted(dedup.values(), key=lambda x: x.law_key)

    def _discover_state_entrypoints(self) -> list[StateEntryPoint]:
        html_content = self._http_get_text(GOBIERNOS_URL)
        entries: list[StateEntryPoint] = []

        for match in re.finditer(r'(?is)<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>', html_content):
            href = match.group(1).strip()
            text = _strip_tags(match.group(2)).strip()
            text_norm = _normalize_text(text)
            if not href:
                continue

            abs_url = urllib.parse.urljoin(GOBIERNOS_URL, href)
            if not abs_url.lower().startswith(("http://", "https://")):
                continue

            context_window = html_content[max(0, match.start() - 1200) : min(len(html_content), match.end() + 220)]
            context_norm = _normalize_text(_strip_tags(context_window))

            if "leyes del estado" in text_norm:
                domain_norm = _normalize_domain(urllib.parse.urlparse(abs_url).netloc)
                jurisdiction = DOMAIN_JURISDICTION_OVERRIDES.get(domain_norm)
                if not jurisdiction:
                    jurisdiction = self._infer_state_from_context(context_norm)
                if jurisdiction and jurisdiction != "ciudad de mexico":
                    entries.append(
                        StateEntryPoint(
                            jurisdiction=jurisdiction,
                            entry_url=abs_url,
                            source_kind="state_congreso_leyes",
                            label=f"{jurisdiction} - leyes del estado",
                        )
                    )
                continue

            domain_norm = _normalize_domain(urllib.parse.urlparse(abs_url).netloc)
            if "congresocdmx.gob.mx" in domain_norm and "marco-legal" in abs_url.lower():
                entries.append(
                    StateEntryPoint(
                        jurisdiction="ciudad de mexico",
                        entry_url=abs_url,
                        source_kind="cdmx_congreso_leyes",
                        label="CDMX - sitio del congreso",
                    )
                )
                continue

            if "consejeria.cdmx.gob.mx" in domain_norm and "/leyes/" in abs_url.lower():
                entries.append(
                    StateEntryPoint(
                        jurisdiction="ciudad de mexico",
                        entry_url=abs_url,
                        source_kind="cdmx_gobierno_leyes",
                        label="CDMX - sitio del gobierno",
                    )
                )
                continue

        dedup: dict[str, StateEntryPoint] = {}
        for item in entries:
            key = f"{item.jurisdiction}::{item.entry_url}::{item.source_kind}"
            dedup[key] = item
        return sorted(dedup.values(), key=lambda x: (x.jurisdiction, x.source_kind, x.entry_url))

    def _infer_state_from_context(self, context_norm: str) -> str | None:
        best_state = None
        best_pos = -1
        for canonical, aliases in STATE_ALIASES.items():
            for alias in aliases:
                pos = context_norm.rfind(alias)
                if pos > best_pos:
                    best_state = canonical
                    best_pos = pos
        return best_state

    def _extract_update_law_keys(self, updates_html: str) -> set[str]:
        keys = set()
        for match in re.finditer(r'href="(?:\.\./)?ref/([a-zA-Z0-9_]+)\.htm"', updates_html, flags=re.IGNORECASE):
            keys.add(match.group(1).lower())
        return keys

    def _upsert_source_and_chunks(
        self,
        *,
        source_key: str,
        law_name: str,
        ref_url: str,
        doc_url: str,
        text_path: str,
        content_hash: str,
        last_reform: str | None,
        scope: str,
        jurisdiction: str,
        source_kind: str,
        chunks: list[dict[str, str]],
    ) -> None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO sources (
                law_key, law_name, ref_url, doc_url, text_path, content_hash,
                last_reform, indexed_at, scope, jurisdiction, source_kind
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(law_key) DO UPDATE SET
                law_name=excluded.law_name,
                ref_url=excluded.ref_url,
                doc_url=excluded.doc_url,
                text_path=excluded.text_path,
                content_hash=excluded.content_hash,
                last_reform=excluded.last_reform,
                indexed_at=excluded.indexed_at,
                scope=excluded.scope,
                jurisdiction=excluded.jurisdiction,
                source_kind=excluded.source_kind
            """,
            (
                source_key,
                law_name,
                ref_url,
                doc_url,
                text_path,
                content_hash,
                last_reform,
                dt.datetime.now(dt.timezone.utc).isoformat(),
                scope,
                jurisdiction,
                source_kind,
            ),
        )

        source_id = cur.execute("SELECT source_id FROM sources WHERE law_key = ?", (source_key,)).fetchone()[0]
        cur.execute("DELETE FROM chunks WHERE source_id = ?", (source_id,))

        for order, chunk in enumerate(chunks):
            cur.execute(
                """
                INSERT INTO chunks (source_id, chunk_order, article_label, content)
                VALUES (?, ?, ?, ?)
                """,
                (source_id, order, chunk.get("article_label", "Fragmento"), chunk.get("content", "")),
            )

        conn.commit()
        conn.close()

    def _candidate_links_from_html(
        self,
        html_text: str,
        base_url: str,
        root_domain: str,
        cap: int,
    ) -> list[str]:
        candidates: list[tuple[int, str]] = []
        for href, anchor_text in _extract_anchor_links(html_text):
            absolute = urllib.parse.urljoin(base_url, href)
            absolute = self._normalize_url(absolute.split("#", 1)[0])
            if not absolute:
                continue
            if not absolute.lower().startswith(("http://", "https://")):
                continue
            if not self._is_allowed_link(absolute, root_domain):
                continue

            score = self._link_score(absolute, anchor_text)
            if score <= 0:
                continue
            candidates.append((score, absolute))

        candidates.sort(key=lambda x: x[0], reverse=True)
        ordered: list[str] = []
        seen: set[str] = set()
        for _, url in candidates:
            if url in seen:
                continue
            seen.add(url)
            ordered.append(url)
            if len(ordered) >= cap:
                break
        return ordered

    def _is_allowed_link(self, url: str, root_domain: str) -> bool:
        parsed = urllib.parse.urlparse(url)
        domain = _normalize_domain(parsed.netloc)
        if not domain:
            return False

        if any(domain.endswith(skip) for skip in SKIP_DOMAINS):
            return False
        if root_domain and not (domain == root_domain or domain.endswith("." + root_domain)):
            return False

        path = parsed.path.lower()
        ext = Path(path).suffix
        if ext in SKIP_EXTENSIONS:
            return False
        return True

    def _link_score(self, url: str, anchor_text: str) -> int:
        payload = _normalize_text(url + " " + anchor_text)
        score = 0
        for hint in LEGAL_URL_HINTS:
            if hint in payload:
                score += 2
        if any(ext in url.lower() for ext in [".doc", ".docx", ".pdf", ".txt", ".rtf"]):
            score += 3
        if "gaceta" in payload:
            score -= 2
        if "boletin" in payload:
            score -= 1
        return score

    def _detect_resource_kind(self, final_url: str, content_type: str) -> str:
        path = urllib.parse.urlparse(final_url).path.lower()
        ext = Path(path).suffix

        if ext in {".html", ".htm", ""}:
            if "text/html" in content_type or ext in {".html", ".htm", ""}:
                return "html"
        if ext in {".doc", ".docx", ".pdf", ".txt", ".rtf"}:
            return ext.replace(".", "")

        ctype = content_type.lower()
        if "application/msword" in ctype:
            return "doc"
        if "application/vnd.openxmlformats-officedocument.wordprocessingml.document" in ctype:
            return "docx"
        if "application/pdf" in ctype:
            return "pdf"
        if "text/plain" in ctype:
            return "txt"
        if "text/html" in ctype:
            return "html"
        return "other"

    def _extract_text_from_binary(self, payload: bytes, kind: str) -> str:
        if kind == "txt":
            return self._decode_bytes(payload)

        suffix = f".{kind}"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(payload)
            tmp_path = Path(tmp.name)

        try:
            if kind in {"doc", "docx", "rtf"}:
                return self._convert_doc_to_text(tmp_path)
            if kind == "pdf":
                return self._extract_pdf_text_fallback(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)
        return ""

    def _extract_pdf_text_fallback(self, pdf_path: Path) -> str:
        proc = subprocess.run(["strings", "-n", "6", str(pdf_path)], capture_output=True, text=True, check=False)
        lines = []
        for line in proc.stdout.splitlines():
            clean = _normalize_spacing(line)
            if len(clean) < 10:
                continue
            if re.search(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]", clean):
                lines.append(clean)
        return "\n".join(lines)

    def _convert_doc_to_text(self, doc_path: Path) -> str:
        cmd = ["textutil", "-convert", "txt", "-stdout", str(doc_path)]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            stderr = proc.stderr.strip() or "error desconocido en textutil"
            raise RuntimeError(f"No se pudo convertir DOC a texto: {stderr}")
        return proc.stdout

    def _html_to_text(self, html_text: str) -> str:
        cleaned = re.sub(r"(?is)<script.*?>.*?</script>", " ", html_text)
        cleaned = re.sub(r"(?is)<style.*?>.*?</style>", " ", cleaned)
        cleaned = _strip_tags(cleaned)
        cleaned = html.unescape(cleaned)
        return _normalize_spacing(cleaned)

    def _extract_html_title(self, html_text: str) -> str:
        match = re.search(r"(?is)<title>(.*?)</title>", html_text)
        if not match:
            return ""
        return _compact(_normalize_spacing(_strip_tags(match.group(1))), 220)

    def _is_legalish(self, text: str) -> bool:
        if not text:
            return False
        compact = _normalize_spacing(text)
        if len(compact) < 220:
            return False
        norm = _normalize_text(compact)
        signal_terms = ["articulo", "capitulo", "ley", "codigo", "constitucion", "reglamento", "decreto"]
        hits = sum(1 for term in signal_terms if term in norm)
        return hits >= 2

    def _chunk_law_text(self, text: str) -> list[dict[str, str]]:
        cleaned = text.replace("\r", "\n")
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        if not cleaned:
            return []

        article_pattern = re.compile(
            r"(?im)^(art[ií]culo\s+\d+[a-zA-Zºo\.\-]*)\.\s*(.*)$",
            flags=re.MULTILINE,
        )
        matches = list(article_pattern.finditer(cleaned))
        chunks: list[dict[str, str]] = []

        if matches:
            for i, match in enumerate(matches):
                start = match.start()
                end = matches[i + 1].start() if i + 1 < len(matches) else len(cleaned)
                chunk_text = cleaned[start:end].strip()
                if len(chunk_text) < 80:
                    continue
                article_label = _compact(match.group(1), 80)
                chunks.append({"article_label": article_label, "content": _compact(chunk_text, 2600)})
        else:
            window = 1700
            overlap = 260
            pos = 0
            idx = 1
            while pos < len(cleaned):
                piece = cleaned[pos : pos + window]
                if len(piece.strip()) > 80:
                    chunks.append({"article_label": f"Fragmento {idx}", "content": piece.strip()})
                idx += 1
                pos += window - overlap

        return chunks

    def _extract_title(self, text: str) -> str:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.isupper() and "LEY" in line:
                return _compact(line, 220)
        first = _compact(text.splitlines()[0] if text.splitlines() else "", 220)
        return first

    def _extract_last_reform(self, text: str) -> str | None:
        match = re.search(r"Última reforma publicada DOF\s+([0-9\-]+)", text, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r"Texto vigente.*?DOF\s+([0-9\-]+)", text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1)
        return None

    def _law_name_from_key(self, law_key: str) -> str:
        return law_key.upper()

    def _http_get_text(self, url: str) -> str:
        with self._open_url(url, timeout=25) as resp:
            data = resp.read()
        return self._decode_bytes(data)

    def _http_get_bytes(self, url: str) -> bytes:
        with self._open_url(url, timeout=35) as resp:
            return resp.read()

    def _http_get_response(self, url: str) -> tuple[bytes, str, str]:
        with self._open_url(url, timeout=10) as resp:
            payload = resp.read()
            content_type = resp.headers.get("Content-Type", "")
            final_url = resp.geturl()
        return payload, content_type, final_url

    def _open_url(self, url: str, *, timeout: float) -> Any:
        current = self._normalize_url(url)
        use_unverified_context = False
        timeout_retries = 0

        for _ in range(6):
            req = urllib.request.Request(current, headers={"User-Agent": USER_AGENT})
            context = ssl._create_unverified_context() if use_unverified_context else None

            try:
                return urllib.request.urlopen(req, timeout=timeout, context=context)
            except urllib.error.HTTPError as exc:
                if exc.code in {301, 302, 303, 307, 308}:
                    location = exc.headers.get("Location", "").strip()
                    if location:
                        current = self._normalize_url(urllib.parse.urljoin(current, location))
                        continue
                    if not current.endswith("/"):
                        current = current + "/"
                        continue
                raise
            except urllib.error.URLError as exc:
                if self._is_ssl_cert_error(exc) and not use_unverified_context:
                    use_unverified_context = True
                    continue
                if self._is_timeout_error(exc) and timeout_retries < 1:
                    timeout_retries += 1
                    continue
                raise
            except (socket.timeout, TimeoutError):
                if timeout_retries < 1:
                    timeout_retries += 1
                    continue
                raise

        raise RuntimeError(f"Demasiadas redirecciones: {url}")

    def _normalize_url(self, url: str) -> str:
        candidate = (url or "").strip()
        if not candidate:
            return candidate

        parts = urllib.parse.urlsplit(candidate)
        if not parts.scheme:
            return candidate

        scheme = parts.scheme.lower()
        netloc = parts.netloc
        try:
            host_port = parts.netloc.split("@")[-1]
            userinfo = ""
            if "@" in parts.netloc:
                userinfo = parts.netloc.rsplit("@", 1)[0] + "@"
            if ":" in host_port:
                host, port = host_port.rsplit(":", 1)
                host = host.encode("idna").decode("ascii")
                netloc = f"{userinfo}{host}:{port}"
            else:
                host = host_port.encode("idna").decode("ascii")
                netloc = f"{userinfo}{host}"
        except Exception:
            netloc = parts.netloc

        path = urllib.parse.quote(parts.path, safe="/%:@-._~!$&'()*+,;=")
        query = urllib.parse.quote(parts.query, safe="=&%:@-._~!$'()*+,;/?")
        return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))

    def _is_ssl_cert_error(self, exc: urllib.error.URLError) -> bool:
        reason = getattr(exc, "reason", None)
        if isinstance(reason, ssl.SSLCertVerificationError):
            return True
        if isinstance(reason, ssl.SSLError) and "CERTIFICATE_VERIFY_FAILED" in str(reason):
            return True
        return False

    def _is_timeout_error(self, exc: urllib.error.URLError) -> bool:
        reason = getattr(exc, "reason", None)
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return True
        return "timed out" in str(exc).lower()

    def _decode_bytes(self, data: bytes) -> str:
        for enc in ("utf-8", "latin-1"):
            try:
                return data.decode(enc)
            except UnicodeDecodeError:
                continue
        return data.decode("utf-8", errors="ignore")

    def _fts_query(self, query: str) -> str:
        normalized = _normalize_text(query)
        tokens = re.findall(r"[a-zA-Z0-9]{3,}", normalized)
        if not tokens:
            return ""
        unique = []
        seen = set()
        for token in tokens:
            if token in seen:
                continue
            seen.add(token)
            unique.append(token)
        return " OR ".join(unique[:12])

    def _ensure_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS sources (
                source_id INTEGER PRIMARY KEY AUTOINCREMENT,
                law_key TEXT NOT NULL UNIQUE,
                law_name TEXT NOT NULL,
                ref_url TEXT,
                doc_url TEXT,
                text_path TEXT,
                content_hash TEXT,
                last_reform TEXT,
                indexed_at TEXT,
                scope TEXT DEFAULT 'federal',
                jurisdiction TEXT DEFAULT 'mexico_federal',
                source_kind TEXT DEFAULT 'federal_law_doc'
            );

            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                chunk_order INTEGER NOT NULL,
                article_label TEXT,
                content TEXT NOT NULL,
                FOREIGN KEY(source_id) REFERENCES sources(source_id) ON DELETE CASCADE
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
                content,
                article_label,
                law_name,
                law_key,
                tokenize = 'unicode61 remove_diacritics 2'
            );
            """
        )

        existing = {row[1] for row in cur.execute("PRAGMA table_info(sources)").fetchall()}
        if "scope" not in existing:
            cur.execute("ALTER TABLE sources ADD COLUMN scope TEXT DEFAULT 'federal'")
        if "jurisdiction" not in existing:
            cur.execute("ALTER TABLE sources ADD COLUMN jurisdiction TEXT DEFAULT 'mexico_federal'")
        if "source_kind" not in existing:
            cur.execute("ALTER TABLE sources ADD COLUMN source_kind TEXT DEFAULT 'federal_law_doc'")

        cur.execute("UPDATE sources SET scope = COALESCE(scope, 'federal')")
        cur.execute("UPDATE sources SET jurisdiction = COALESCE(jurisdiction, 'mexico_federal')")
        cur.execute("UPDATE sources SET source_kind = COALESCE(source_kind, 'federal_law_doc')")

        cur.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
              INSERT INTO chunks_fts(rowid, content, article_label, law_name, law_key)
              SELECT
                new.chunk_id,
                new.content,
                new.article_label,
                (SELECT law_name FROM sources WHERE source_id = new.source_id),
                (SELECT law_key FROM sources WHERE source_id = new.source_id);
            END;

            CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
              DELETE FROM chunks_fts WHERE rowid = old.chunk_id;
            END;

            CREATE TRIGGER IF NOT EXISTS chunks_au AFTER UPDATE ON chunks BEGIN
              DELETE FROM chunks_fts WHERE rowid = old.chunk_id;
              INSERT INTO chunks_fts(rowid, content, article_label, law_name, law_key)
              SELECT
                new.chunk_id,
                new.content,
                new.article_label,
                (SELECT law_name FROM sources WHERE source_id = new.source_id),
                (SELECT law_key FROM sources WHERE source_id = new.source_id);
            END;
            """
        )

        conn.commit()
        conn.close()

    def _load_meta(self) -> dict[str, Any]:
        if self.meta_path.exists():
            return json.loads(self.meta_path.read_text(encoding="utf-8"))
        return {}

    def _save_meta(self, meta_update: dict[str, Any]) -> None:
        meta = self._load_meta()
        meta.update(meta_update)
        self.meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")



def _extract_anchor_links(html_text: str) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    for match in re.finditer(r'(?is)<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>', html_text):
        href = match.group(1).strip()
        text = _strip_tags(match.group(2)).strip()
        links.append((href, text))
    return links



def _strip_tags(value: str) -> str:
    return re.sub(r"(?is)<[^>]+>", " ", value or "")



def _normalize_spacing(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()



def _normalize_text(text: str) -> str:
    lowered = (text or "").lower()
    normalized = unicodedata.normalize("NFD", lowered)
    without_marks = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", without_marks).strip()



def _normalize_domain(domain: str) -> str:
    if not domain:
        return ""
    return domain.lower().strip().lstrip("www.")



def _compact(text: str, limit: int) -> str:
    clean = _normalize_spacing(text)
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "…"
