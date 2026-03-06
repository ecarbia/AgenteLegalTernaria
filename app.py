from __future__ import annotations

import datetime as dt
import io
import json
import re
import textwrap
import threading
import unicodedata
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request, send_file
from werkzeug.utils import secure_filename

from legalops_engine import (
    analyze_contract,
    answer_question,
    assess_clause_risk,
    load_json,
    parse_document_text,
    save_json,
)
from legal_rag import LegalRAG
from llm_bridge import answer_question_with_gemini, enrich_analysis_with_gemini, get_llm_status
from llm_bridge import (
    draft_dialogue_reply_with_gemini,
    generate_executive_summary_with_gemini,
    plan_research_actions_with_gemini,
    rewrite_clause_with_gemini,
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
UPLOADS_DIR = DATA_DIR / "uploads"
TEXT_DIR = DATA_DIR / "text"
ANALYSIS_DIR = DATA_DIR / "analysis"
FEEDBACK_DIR = DATA_DIR / "feedback"
CLAUSE_EDITS_DIR = DATA_DIR / "clause_edits"
CONVERSATIONS_DIR = DATA_DIR / "conversations"
FEEDBACK_LOG_PATH = FEEDBACK_DIR / "feedback_log.jsonl"
DOCUMENTS_INDEX_PATH = DATA_DIR / "documents_index.json"

ALLOWED_EXTENSIONS = {"txt", "docx", "pdf"}
ALLOWED_FEEDBACK_SIGNALS = {"incumplimiento", "duda", "vo_bo"}
RISK_PRIORITY = {"low": 1, "medium": 2, "high": 3, "critical": 4}
RISK_SCORE_HINT = {"low": 20, "medium": 45, "high": 70, "critical": 88}

DISCLAIMER = (
    "Analisis tecnico automatizado. No constituye asesoria legal definitiva; "
    "requiere validacion de abogado responsable."
)

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
)

LEGAL_RAG = LegalRAG(BASE_DIR)
RAG_STATE: dict[str, Any] = {
    "running": False,
    "mode": None,
    "last_run": None,
    "last_error": None,
    "last_stats": None,
}


def ensure_storage() -> None:
    for path in (UPLOADS_DIR, TEXT_DIR, ANALYSIS_DIR, FEEDBACK_DIR, CLAUSE_EDITS_DIR, CONVERSATIONS_DIR):
        path.mkdir(parents=True, exist_ok=True)
    if not DOCUMENTS_INDEX_PATH.exists():
        save_json(DOCUMENTS_INDEX_PATH, {"documents": []})
    if not FEEDBACK_LOG_PATH.exists():
        FEEDBACK_LOG_PATH.write_text("", encoding="utf-8")


def load_documents_index() -> dict[str, Any]:
    ensure_storage()
    return load_json(DOCUMENTS_INDEX_PATH)


def save_documents_index(index: dict[str, Any]) -> None:
    save_json(DOCUMENTS_INDEX_PATH, index)


def infer_source_type(filename: str) -> str:
    suffix = Path(filename).suffix.lower().replace(".", "")
    return suffix


def current_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def parse_bool(raw: Any) -> bool:
    value = normalize_for_match(str(raw or ""))
    return value in {"1", "true", "si", "yes", "on"}


def normalize_signal(value: str) -> str:
    token = normalize_for_match(value or "")
    token = re.sub(r"[^a-z0-9]+", "", token)
    if token in {"vobo", "vob", "aprobado"}:
        return "vo_bo"
    if token in {"incumplimiento", "incumple"}:
        return "incumplimiento"
    if token in {"duda", "dudas"}:
        return "duda"
    return ""


def normalize_risk_level(value: str) -> str:
    level = normalize_for_match(value or "")
    return level if level in RISK_PRIORITY else ""


def get_document(document_id: str) -> dict[str, Any] | None:
    index = load_documents_index()
    for item in index["documents"]:
        if item["document_id"] == document_id:
            return item
    return None


def analysis_path(document_id: str) -> Path:
    return ANALYSIS_DIR / f"{document_id}.json"


def load_analysis_if_exists(document_id: str) -> dict[str, Any] | None:
    path = analysis_path(document_id)
    if not path.exists():
        return None
    return load_json(path)


def clause_edits_path(document_id: str) -> Path:
    return CLAUSE_EDITS_DIR / f"{document_id}.json"


def load_clause_edits(document_id: str) -> dict[str, Any]:
    path = clause_edits_path(document_id)
    if not path.exists():
        return {"document_id": document_id, "updated_at": None, "by_clause": {}}
    try:
        payload = load_json(path)
    except Exception:
        return {"document_id": document_id, "updated_at": None, "by_clause": {}}
    if not isinstance(payload, dict):
        return {"document_id": document_id, "updated_at": None, "by_clause": {}}
    payload.setdefault("document_id", document_id)
    payload.setdefault("updated_at", None)
    payload.setdefault("by_clause", {})
    return payload


def save_clause_edits(document_id: str, payload: dict[str, Any]) -> None:
    payload["document_id"] = document_id
    payload["updated_at"] = current_utc_iso()
    save_json(clause_edits_path(document_id), payload)


def conversation_path(document_id: str) -> Path:
    return CONVERSATIONS_DIR / f"{document_id}.json"


def load_conversation_state(document_id: str) -> dict[str, Any]:
    path = conversation_path(document_id)
    if not path.exists():
        return {
            "document_id": document_id,
            "objective": "",
            "summary": "",
            "history": [],
            "updated_at": None,
        }
    try:
        payload = load_json(path)
    except Exception:
        return {
            "document_id": document_id,
            "objective": "",
            "summary": "",
            "history": [],
            "updated_at": None,
        }
    if not isinstance(payload, dict):
        return {
            "document_id": document_id,
            "objective": "",
            "summary": "",
            "history": [],
            "updated_at": None,
        }
    history = payload.get("history", [])
    if not isinstance(history, list):
        history = []
    safe_history = []
    for turn in history[-40:]:
        if not isinstance(turn, dict):
            continue
        role = (turn.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        text = str(turn.get("text") or "").strip()
        if not text:
            continue
        safe_history.append(
            {
                "role": role,
                "text": text[:1800],
                "created_at": turn.get("created_at") or current_utc_iso(),
            }
        )
    return {
        "document_id": document_id,
        "objective": str(payload.get("objective") or "").strip()[:220],
        "summary": str(payload.get("summary") or "").strip()[:2200],
        "history": safe_history,
        "updated_at": payload.get("updated_at"),
    }


def save_conversation_state(document_id: str, state: dict[str, Any]) -> None:
    payload = {
        "document_id": document_id,
        "objective": str(state.get("objective") or "").strip()[:220],
        "summary": str(state.get("summary") or "").strip()[:2200],
        "history": list(state.get("history", []))[-40:],
        "updated_at": current_utc_iso(),
    }
    save_json(conversation_path(document_id), payload)


def infer_user_tone(question: str) -> str:
    q = normalize_for_match(question or "")
    frustration = {"pesimo", "malo", "no funciona", "error", "mal", "urgente"}
    urgency = {"urgente", "hoy", "ya", "inmediato", "asap"}
    collaborative = {"ayudame", "podemos", "vamos", "quiero"}
    if any(token in q for token in frustration):
        return "frustrado"
    if any(token in q for token in urgency):
        return "urgente"
    if any(token in q for token in collaborative):
        return "colaborativo"
    return "neutral"


def infer_conversation_objective(question: str, current_objective: str = "") -> str:
    q = normalize_for_match(question or "")
    if any(token in q for token in {"dictamen", "dictaminar", "conclusion"}):
        return "Preparar dictamen del contrato."
    if any(token in q for token in {"ley", "jurisd", "foro", "norma", "articulo"}):
        return "Resolver dudas de ley aplicable y fundamento normativo."
    if any(token in q for token in {"riesgo", "incumpl", "duda", "vobo", "vo bo"}):
        return "Priorizar y resolver hallazgos de riesgo contractual."
    if any(token in q for token in {"editar", "redact", "clausula", "reescribir"}):
        return "Ajustar redaccion de clausulas para negociacion y Vo.Bo."
    if current_objective:
        return current_objective
    return "Analizar riesgos y acelerar dictamen del contrato."


def compact_conversation_state(state: dict[str, Any]) -> dict[str, Any]:
    history = list(state.get("history", []))
    if len(history) <= 16:
        state["history"] = history
        return state

    dropped = history[:-16]
    kept = history[-16:]
    digest_parts = []
    for turn in dropped[-8:]:
        role = "U" if turn.get("role") == "user" else "A"
        digest_parts.append(f"{role}: {str(turn.get('text') or '')[:120]}")
    digest = " | ".join(digest_parts).strip()
    prior = str(state.get("summary") or "").strip()
    merged = f"{prior} | {digest}".strip(" |")
    state["summary"] = merged[-2200:]
    state["history"] = kept
    return state


def append_conversation_turn(state: dict[str, Any], *, role: str, text: str) -> dict[str, Any]:
    if role not in {"user", "assistant"}:
        return state
    clean = (text or "").strip()
    if not clean:
        return state
    history = list(state.get("history", []))
    history.append({"role": role, "text": clean[:1800], "created_at": current_utc_iso()})
    state["history"] = history
    return compact_conversation_state(state)


def load_feedback_entries(document_id: str | None = None) -> list[dict[str, Any]]:
    ensure_storage()
    if not FEEDBACK_LOG_PATH.exists():
        return []

    entries: list[dict[str, Any]] = []
    for line in FEEDBACK_LOG_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if document_id and item.get("document_id") != document_id:
            continue
        entries.append(item)

    entries.sort(key=lambda x: x.get("created_at", ""))
    return entries


def append_feedback_entry(entry: dict[str, Any]) -> None:
    ensure_storage()
    with FEEDBACK_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def summarize_feedback(entries: list[dict[str, Any]]) -> dict[str, Any]:
    signals = Counter()
    by_clause: dict[str, Counter[str]] = {}

    for item in entries:
        signal = normalize_signal(item.get("signal", ""))
        if signal:
            signals[signal] += 1
        clause_type = (item.get("clause_type") or "").strip()
        if clause_type:
            by_clause.setdefault(clause_type, Counter())[signal or "otro"] += 1

    latest = entries[-1] if entries else None
    return {
        "total": len(entries),
        "signals": {
            "incumplimiento": signals.get("incumplimiento", 0),
            "duda": signals.get("duda", 0),
            "vo_bo": signals.get("vo_bo", 0),
        },
        "latest": latest,
        "by_clause": {key: dict(counter) for key, counter in by_clause.items()},
    }


def latest_feedback_by_clause(entries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    ordered = sorted(entries, key=lambda item: item.get("created_at", ""))
    latest: dict[str, dict[str, Any]] = {}
    for item in ordered:
        clause_type = (item.get("clause_type") or "").strip() or "general"
        latest[clause_type] = item
    return latest


def resolve_clause_queue_status(clause: dict[str, Any], latest_feedback: dict[str, Any] | None) -> str:
    if latest_feedback:
        signal = normalize_signal(latest_feedback.get("signal", ""))
        if signal == "vo_bo":
            return "Vo.Bo."
        if signal == "duda":
            return "Duda"
        if signal == "incumplimiento":
            return "Requiere revision"

    risk_level = normalize_risk_level((clause.get("risk") or {}).get("level", "")) or "medium"
    status = (clause.get("status") or "").strip().lower()
    if risk_level in {"high", "critical"} or status in {"partial", "not_found"}:
        return "Requiere revision"
    if risk_level == "medium":
        return "Duda"
    return "Vo.Bo."


def recompute_overall_risk(clauses: list[dict[str, Any]]) -> dict[str, Any]:
    if not clauses:
        return {"level": "medium", "score": 50, "critical_flags": ["Sin clausulas evaluadas."]}

    level = "low"
    max_score = 0
    total = 0
    flags: list[str] = []

    for clause in clauses:
        clause_label = clause.get("clause_label") or clause.get("clause_type") or "Clausula"
        risk = clause.get("risk", {})
        risk_level = normalize_risk_level(risk.get("level", "")) or "medium"
        score = int(risk.get("score", 50))
        total += score
        max_score = max(max_score, score)

        if RISK_PRIORITY[risk_level] > RISK_PRIORITY[level]:
            level = risk_level
        if risk_level in {"high", "critical"}:
            flags.append(f"{clause_label}: {risk.get('rationale', 'Revisar redaccion contractual.')}")

    avg_score = round(total / max(1, len(clauses)), 2)
    return {"level": level, "score": max(avg_score, float(max_score)), "critical_flags": flags[:10]}


def apply_feedback_learning(analysis: dict[str, Any], document_id: str) -> dict[str, Any]:
    all_entries = load_feedback_entries()
    doc_entries = [entry for entry in all_entries if entry.get("document_id") == document_id]

    overrides_by_clause: dict[str, Counter[str]] = {}
    doc_signals_by_clause: dict[str, Counter[str]] = {}

    for entry in all_entries:
        clause_type = (entry.get("clause_type") or "").strip()
        corrected_risk = normalize_risk_level(entry.get("corrected_risk", ""))
        if clause_type and corrected_risk:
            overrides_by_clause.setdefault(clause_type, Counter())[corrected_risk] += 1

    for entry in doc_entries:
        clause_type = (entry.get("clause_type") or "").strip()
        signal = normalize_signal(entry.get("signal", ""))
        if clause_type and signal:
            doc_signals_by_clause.setdefault(clause_type, Counter())[signal] += 1

    adjustments: list[dict[str, Any]] = []
    for clause in analysis.get("clauses", []):
        clause_type = (clause.get("clause_type") or "").strip()
        risk = clause.setdefault("risk", {})
        current_level = normalize_risk_level(risk.get("level", "")) or "medium"
        original_level = current_level

        global_override = overrides_by_clause.get(clause_type, Counter())
        if global_override and sum(global_override.values()) >= 2:
            learned_level = global_override.most_common(1)[0][0]
            if RISK_PRIORITY[learned_level] != RISK_PRIORITY[current_level]:
                current_level = learned_level
                risk["level"] = learned_level
                risk["score"] = max(int(risk.get("score", 0)), RISK_SCORE_HINT[learned_level])
                risk["rationale"] = (
                    f"{risk.get('rationale', '').strip()} Ajuste por patron historico de feedback legal."
                ).strip()

        doc_signals = doc_signals_by_clause.get(clause_type, Counter())
        if doc_signals.get("incumplimiento", 0) > 0 and RISK_PRIORITY[current_level] < RISK_PRIORITY["high"]:
            current_level = "high"
            risk["level"] = "high"
            risk["score"] = max(int(risk.get("score", 0)), RISK_SCORE_HINT["high"])
            risk["rationale"] = (
                f"{risk.get('rationale', '').strip()} Incrementado por feedback de incumplimiento."
            ).strip()
        elif (
            doc_signals.get("vo_bo", 0) > 0
            and doc_signals.get("incumplimiento", 0) == 0
            and RISK_PRIORITY[current_level] <= RISK_PRIORITY["medium"]
        ):
            current_level = "low"
            risk["level"] = "low"
            risk["score"] = min(int(risk.get("score", RISK_SCORE_HINT["low"])), RISK_SCORE_HINT["low"])
            risk["rationale"] = (
                f"{risk.get('rationale', '').strip()} Atenuado por Vo.Bo. del abogado."
            ).strip()

        clause["feedback_signals"] = dict(doc_signals)
        if current_level != original_level:
            adjustments.append(
                {
                    "clause_type": clause_type,
                    "from_level": original_level,
                    "to_level": current_level,
                }
            )

    analysis["overall_risk"] = recompute_overall_risk(analysis.get("clauses", []))
    analysis["lawyer_feedback"] = summarize_feedback(doc_entries)
    analysis["learning_trace"] = {
        "global_feedback_count": len(all_entries),
        "document_feedback_count": len(doc_entries),
        "adjustments": adjustments[:20],
    }
    return analysis


def infer_contract_status(analysis: dict[str, Any] | None, feedback_summary: dict[str, Any]) -> str:
    if analysis is None:
        return "Sin analisis"

    latest_signal = normalize_signal((feedback_summary.get("latest") or {}).get("signal", ""))
    if latest_signal == "incumplimiento":
        return "Incumplimiento"
    if latest_signal == "duda":
        return "Duda"
    if latest_signal == "vo_bo":
        return "Vo.Bo."

    risk_level = (analysis.get("overall_risk") or {}).get("level", "medium")
    if risk_level in {"high", "critical"}:
        return "Requiere revision"

    if analysis.get("human_review", {}).get("required"):
        return "Revision humana"
    return "Listo"


def build_contract_rows(client_name_filter: str | None = None) -> list[dict[str, Any]]:
    index = load_documents_index()
    rows: list[dict[str, Any]] = []
    filter_norm = normalize_for_match(client_name_filter or "")

    for metadata in index.get("documents", []):
        client_name = (metadata.get("client_name") or "Sin cliente").strip() or "Sin cliente"
        if filter_norm and normalize_for_match(client_name) != filter_norm:
            continue
        document_id = metadata["document_id"]
        analysis = load_analysis_if_exists(document_id)
        feedback_entries = load_feedback_entries(document_id)
        feedback_summary = summarize_feedback(feedback_entries)
        status = infer_contract_status(analysis, feedback_summary)

        overall = (analysis or {}).get("overall_risk", {})
        level = normalize_risk_level(overall.get("level", "")) or "medium"
        score = overall.get("score", 0)
        updated_at = metadata.get("analyzed_at") or metadata.get("uploaded_at")

        rows.append(
            {
                "document_id": document_id,
                "document_name": metadata.get("document_name", "Contrato"),
                "project_name": metadata.get("project_name") or metadata.get("document_name", "Contrato"),
                "client_name": client_name,
                "contract_type": metadata.get("contract_type", ""),
                "counterparty": metadata.get("counterparty", ""),
                "expected_sign_date": metadata.get("expected_sign_date", ""),
                "objective": metadata.get("objective", ""),
                "include_historical": bool(metadata.get("include_historical", False)),
                "analysis_available": analysis is not None,
                "risk_level": level,
                "risk_score": score,
                "status": status,
                "human_review_required": bool((analysis or {}).get("human_review", {}).get("required")),
                "feedback_summary": feedback_summary,
                "updated_at": updated_at,
                "uploaded_at": metadata.get("uploaded_at"),
                "analyzed_at": metadata.get("analyzed_at"),
            }
        )

    rows.sort(
        key=lambda row: (
            RISK_PRIORITY.get(row["risk_level"], 0),
            row.get("updated_at", ""),
        ),
        reverse=True,
    )
    return rows


def build_review_queue_rows(
    *,
    client_name_filter: str | None = None,
    status_filter: str | None = None,
) -> list[dict[str, Any]]:
    rows = build_contract_rows(client_name_filter=client_name_filter)
    normalized_status_filter = normalize_for_match(status_filter or "")
    queue: list[dict[str, Any]] = []

    for row in rows:
        document_id = row["document_id"]
        analysis = load_analysis_if_exists(document_id)
        if not analysis:
            continue

        feedback_entries = load_feedback_entries(document_id)
        latest_by_clause = latest_feedback_by_clause(feedback_entries)
        edits_payload = load_clause_edits(document_id)
        edits_by_clause = edits_payload.get("by_clause", {}) if isinstance(edits_payload, dict) else {}

        for clause in analysis.get("clauses", []):
            clause_type = (clause.get("clause_type") or "").strip()
            if not clause_type:
                continue
            latest_feedback = latest_by_clause.get(clause_type)
            queue_status = resolve_clause_queue_status(clause, latest_feedback)
            if normalized_status_filter and normalize_for_match(queue_status) != normalized_status_filter:
                continue

            manual_edit = edits_by_clause.get(clause_type, {}) if isinstance(edits_by_clause, dict) else {}
            queue.append(
                {
                    "document_id": document_id,
                    "document_name": row.get("document_name"),
                    "project_name": row.get("project_name"),
                    "client_name": row.get("client_name", "Sin cliente"),
                    "counterparty": row.get("counterparty", ""),
                    "clause_type": clause_type,
                    "clause_label": clause.get("clause_label") or clause_type,
                    "risk_level": normalize_risk_level((clause.get("risk") or {}).get("level", "")) or "medium",
                    "queue_status": queue_status,
                    "updated_at": row.get("updated_at"),
                    "latest_feedback_at": (latest_feedback or {}).get("created_at"),
                    "latest_feedback_note": (latest_feedback or {}).get("note", ""),
                    "has_manual_edit": bool(manual_edit),
                    "manual_edit_updated_at": manual_edit.get("updated_at"),
                }
            )

    status_priority = {"Requiere revision": 3, "Duda": 2, "Vo.Bo.": 1}
    queue.sort(
        key=lambda item: (
            status_priority.get(item.get("queue_status", ""), 0),
            RISK_PRIORITY.get(item.get("risk_level", "medium"), 0),
            item.get("latest_feedback_at") or item.get("updated_at") or "",
        ),
        reverse=True,
    )
    return queue


def build_dashboard_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    high_risk = sum(1 for row in rows if row["risk_level"] in {"high", "critical"})
    pending = sum(1 for row in rows if row["status"] in {"Sin analisis", "Requiere revision", "Duda", "Revision humana"})
    vo_bo = sum(1 for row in rows if row["status"] == "Vo.Bo.")
    with_analysis = sum(1 for row in rows if row["analysis_available"])
    return {
        "total_contracts": total,
        "high_risk": high_risk,
        "pending_review": pending,
        "vo_bo": vo_bo,
        "analyzed_contracts": with_analysis,
    }


def build_dictamen_payload(
    metadata: dict[str, Any],
    analysis: dict[str, Any],
    feedback_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    clauses = analysis.get("clauses", [])
    overall = analysis.get("overall_risk", {})

    incumplimientos: list[dict[str, Any]] = []
    dudas: list[dict[str, Any]] = []
    vo_bo: list[str] = []
    recommendations: list[str] = []

    for clause in clauses:
        label = clause.get("clause_label") or clause.get("clause_type") or "Clausula"
        status = clause.get("status")
        risk = clause.get("risk", {})
        risk_level = normalize_risk_level(risk.get("level", "")) or "medium"
        evidence = (clause.get("evidence") or [{}])[0]
        evidence_ref = {
            "section": evidence.get("section", "No identificada"),
            "snippet": evidence.get("snippet", "")[:260],
        }

        if risk_level in {"high", "critical"} or status == "not_found":
            incumplimientos.append(
                {
                    "clause_label": label,
                    "detail": risk.get("rationale", "Revisar redaccion."),
                    "evidence": evidence_ref,
                }
            )
        elif status in {"partial", "not_found"} or risk_level == "medium":
            dudas.append(
                {
                    "clause_label": label,
                    "detail": risk.get("rationale", "Pendiente validacion."),
                    "evidence": evidence_ref,
                }
            )

        recommendation = (risk.get("recommendation_initial") or "").strip()
        if recommendation and recommendation not in recommendations:
            recommendations.append(recommendation)

    for entry in feedback_entries:
        signal = normalize_signal(entry.get("signal", ""))
        note = (entry.get("note") or "").strip()
        clause_type = (entry.get("clause_type") or "").strip()
        title = clause_type or "General"

        if signal == "incumplimiento" and note:
            incumplimientos.append(
                {
                    "clause_label": title,
                    "detail": f"Feedback abogado: {note}",
                    "evidence": {"section": "Feedback legal", "snippet": note[:260]},
                }
            )
        elif signal == "duda" and note:
            dudas.append(
                {
                    "clause_label": title,
                    "detail": f"Feedback abogado: {note}",
                    "evidence": {"section": "Feedback legal", "snippet": note[:260]},
                }
            )
        elif signal == "vo_bo":
            vo_bo.append(note or f"Vo.Bo. registrado en {title}.")

    llm_hallazgos = analysis.get("llm_hallazgos", {})
    for text in llm_hallazgos.get("incumplimientos", []):
        incumplimientos.append(
            {
                "clause_label": "llm_review",
                "detail": text,
                "evidence": {"section": "LLM review", "snippet": text[:260]},
            }
        )
    for text in llm_hallazgos.get("dudas", []):
        dudas.append(
            {
                "clause_label": "llm_review",
                "detail": text,
                "evidence": {"section": "LLM review", "snippet": text[:260]},
            }
        )
    for text in llm_hallazgos.get("vobo", []):
        vo_bo.append(text)

    if not vo_bo and overall.get("level") in {"low", "medium"} and not incumplimientos:
        vo_bo.append("Sin observaciones criticas para continuidad, sujeto a validacion final.")

    conclusion = "Dictamen condicionado"
    if incumplimientos or overall.get("level") == "critical":
        conclusion = "No favorable sin ajustes"
    elif overall.get("level") == "high":
        conclusion = "Condicionado a negociacion"
    elif vo_bo and not dudas:
        conclusion = "Vo.Bo. tecnico preliminar"

    return {
        "document_id": metadata.get("document_id"),
        "document_name": metadata.get("document_name"),
        "project_name": metadata.get("project_name") or metadata.get("document_name"),
        "contract_type": metadata.get("contract_type", ""),
        "counterparty": metadata.get("counterparty", ""),
        "expected_sign_date": metadata.get("expected_sign_date", ""),
        "generated_at": current_utc_iso(),
        "overall_risk": overall,
        "resumen": {
            "partes": (analysis.get("summary") or {}).get("parties", []),
            "objeto": (analysis.get("summary") or {}).get("object", "No identificado"),
            "jurisdiccion": (analysis.get("summary") or {}).get("jurisdiction", "No identificado"),
        },
        "incumplimientos": incumplimientos[:20],
        "dudas": dudas[:20],
        "vo_bo": vo_bo[:20],
        "recomendaciones": recommendations[:10],
        "conclusion": conclusion,
        "disclaimer": DISCLAIMER,
    }


def blend_feedback_into_answer(answer: str, feedback_summary: dict[str, Any]) -> str:
    signals = feedback_summary.get("signals", {})
    total = int(feedback_summary.get("total", 0))
    if total <= 0:
        return answer

    intro = (
        "Tomando en cuenta feedback legal previo "
        f"(incumplimientos: {signals.get('incumplimiento', 0)}, "
        f"dudas: {signals.get('duda', 0)}, Vo.Bo.: {signals.get('vo_bo', 0)}), "
    )
    return intro + answer


def build_default_llm_meta() -> dict[str, Any]:
    status = get_llm_status()
    status["used"] = False
    status["error"] = None
    status["generated_at"] = current_utc_iso()
    return status


def build_conversation_context(
    conversation_state: dict[str, Any],
    *,
    question: str,
    metadata: dict[str, Any],
    analysis: dict[str, Any],
) -> dict[str, Any]:
    recent_history = []
    for turn in conversation_state.get("history", [])[-8:]:
        recent_history.append(
            {
                "role": turn.get("role"),
                "text": str(turn.get("text") or "")[:260],
            }
        )

    contract_context = analysis.get("contract_context", {})
    return {
        "objective": conversation_state.get("objective") or "",
        "history_summary": conversation_state.get("summary") or "",
        "recent_history": recent_history,
        "user_tone": infer_user_tone(question),
        "project_name": contract_context.get("project_name") or metadata.get("project_name") or metadata.get("document_name"),
        "client_name": contract_context.get("client_name") or metadata.get("client_name") or "",
    }


def executive_summary_needs_refresh(text: str) -> bool:
    clean = (text or "").strip()
    if len(clean) < 90:
        return True
    probe = normalize_for_match(clean)
    bad_patterns = [
        "regula la relacion entre en la ciudad de mexico",
        "su objeto principal es: objeto del contrato",
        "hallazgo(s) critico(s)",
        "sobre vigencia/plazo, se observa",
    ]
    return any(pattern in probe for pattern in bad_patterns)


def is_small_talk_question(question: str) -> bool:
    q = normalize_for_match(question or "")
    q = re.sub(r"[^a-z0-9\s]+", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    if not q:
        return True

    greetings = {
        "hola",
        "buenas",
        "buen dia",
        "buenos dias",
        "buenas tardes",
        "buenas noches",
        "que tal",
        "hi",
        "hello",
        "hey",
        "como estas",
        "que haces",
        "tu que haces",
        "quien eres",
        "que puedes hacer",
        "ayuda",
    }
    if q in greetings:
        return True

    tokens = q.split()
    if len(tokens) <= 2 and any(token in {"hola", "buenas", "hey", "hi", "hello"} for token in tokens):
        return True
    legal_tokens = {
        "contrato",
        "clausula",
        "riesgo",
        "ley",
        "jurisdiccion",
        "dictamen",
        "incumplimiento",
        "indemnizacion",
        "pago",
    }
    if len(tokens) <= 5 and not any(token in legal_tokens for token in tokens):
        return True
    return False


def build_small_talk_response(
    metadata: dict[str, Any],
    analysis: dict[str, Any],
    *,
    question: str,
    conversation_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    q = normalize_for_match(question or "")
    contract_name = metadata.get("project_name") or metadata.get("document_name") or "el contrato"
    overall = analysis.get("overall_risk", {}) if isinstance(analysis, dict) else {}
    level = normalize_risk_level(overall.get("level", "")) or "medium"

    history = list((conversation_state or {}).get("history", []))
    first_touch = len(history) <= 1
    greeting_tokens = {"hola", "buenas", "hey", "hello", "hi", "que tal", "como estas", "tu que haces"}
    greeting = q in greeting_tokens
    onboarding_tokens = {"que hago", "que hago aqui", "como funciona", "por donde", "que sigue", "como empiezo", "ayuda"}
    onboarding = any(token in q for token in onboarding_tokens)

    if onboarding:
        answer = (
            f"Aqui puedes revisar {contract_name} de inicio a fin. "
            "Primero te explico riesgos por clausula, despues marcamos cada hallazgo como Requiere revision, Duda o Vo.Bo., "
            "y al final generamos dictamen con soporte normativo. "
            "Si quieres, empezamos ahora con un resumen ejecutivo del contrato."
        )
        next_actions = [
            "Genera resumen ejecutivo del contrato actual.",
            "Prioriza hallazgos por impacto economico y juridico.",
        ]
    elif greeting and first_touch:
        answer = (
            f"Hola. Ya tengo abierto {contract_name}. "
            "Estoy listo para ayudarte con analisis de riesgo, ley aplicable y dictamen."
        )
        next_actions = [
            "Resume riesgos por clausula con prioridad.",
            "Genera dictamen preliminar del contrato.",
        ]
    elif greeting:
        answer = (
            f"Hola de nuevo. Seguimos con {contract_name}. "
            "Dime que quieres resolver en este turno y voy directo al punto."
        )
        next_actions = [
            "Revisar una clausula especifica.",
            "Preparar dictamen breve para envio interno.",
        ]
    elif first_touch:
        answer = (
            f"Listo, ya tengo abierto {contract_name}. "
            "Puedo ayudarte a analizar riesgos, validar normativa aplicable y preparar dictamen. "
            "Dime en que parte quieres entrar primero."
        )
        next_actions = [
            "Resume riesgos por clausula con prioridad.",
            "Genera dictamen preliminar del contrato.",
        ]
    else:
        answer = (
            "Seguimos sobre este contrato. "
            "Si me dices el objetivo de este turno, te doy una respuesta puntual y accionable."
        )
        next_actions = [
            "Revisar una clausula especifica.",
            "Preparar dictamen breve para envio interno.",
        ]

    return {
        "answer": answer,
        "confidence": 0.9,
        "risk_estimate": {
            "level": "low",
            "impacto_probable": "Sin nueva consulta específica; no se actualiza dictamen en este mensaje.",
            "recomendacion_inicial": "Pregunta por una cláusula concreta (ej. indemnización, pagos o terminación).",
        },
        "chat_mode": "small_talk",
        "citations": [],
        "missing_evidence": False,
        "human_review_required": False,
        "next_actions": next_actions,
        "disclaimer": DISCLAIMER,
    }


def _import_reportlab() -> Any:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
    except Exception as exc:  # pragma: no cover - operativo
        raise RuntimeError("reportlab no disponible") from exc
    return A4, canvas


def _escape_pdf_text(text: str) -> str:
    return (
        text.replace("\\", "\\\\")
        .replace("(", "\\(")
        .replace(")", "\\)")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _build_plain_pdf(lines: list[str]) -> io.BytesIO:
    lines = [line[:180] for line in lines if line is not None]
    if not lines:
        lines = ["Sin contenido para exportar."]

    page_size = 52
    pages = [lines[idx : idx + page_size] for idx in range(0, len(lines), page_size)]
    page_count = len(pages)

    catalog_obj = 1
    pages_obj = 2
    first_page_obj = 3
    font_obj = first_page_obj + page_count * 2
    total_objs = font_obj

    objects: dict[int, bytes] = {}
    kids_refs = []
    for idx, page_lines in enumerate(pages):
        page_obj = first_page_obj + idx * 2
        content_obj = page_obj + 1
        kids_refs.append(f"{page_obj} 0 R")

        stream_lines = ["BT", "/F1 10 Tf", "36 806 Td"]
        for line_index, line in enumerate(page_lines):
            escaped = _escape_pdf_text(line)
            if line_index == 0:
                stream_lines.append(f"({escaped}) Tj")
            else:
                stream_lines.append(f"0 -14 Td ({escaped}) Tj")
        stream_lines.append("ET")
        stream = "\n".join(stream_lines).encode("latin-1", errors="replace")

        objects[content_obj] = (
            f"<< /Length {len(stream)} >>\nstream\n".encode("ascii")
            + stream
            + b"\nendstream"
        )
        objects[page_obj] = (
            f"<< /Type /Page /Parent {pages_obj} 0 R /MediaBox [0 0 595 842] "
            f"/Resources << /Font << /F1 {font_obj} 0 R >> >> /Contents {content_obj} 0 R >>"
        ).encode("ascii")

    objects[catalog_obj] = f"<< /Type /Catalog /Pages {pages_obj} 0 R >>".encode("ascii")
    objects[pages_obj] = (
        f"<< /Type /Pages /Kids [{' '.join(kids_refs)}] /Count {page_count} >>"
    ).encode("ascii")
    objects[font_obj] = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"

    output = bytearray()
    output.extend(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")

    offsets = [0] * (total_objs + 1)
    for obj_num in range(1, total_objs + 1):
        offsets[obj_num] = len(output)
        output.extend(f"{obj_num} 0 obj\n".encode("ascii"))
        output.extend(objects[obj_num])
        output.extend(b"\nendobj\n")

    xref_offset = len(output)
    output.extend(f"xref\n0 {total_objs + 1}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for obj_num in range(1, total_objs + 1):
        output.extend(f"{offsets[obj_num]:010d} 00000 n \n".encode("ascii"))

    output.extend(
        (
            "trailer\n"
            f"<< /Size {total_objs + 1} /Root {catalog_obj} 0 R >>\n"
            "startxref\n"
            f"{xref_offset}\n"
            "%%EOF\n"
        ).encode("ascii")
    )

    stream = io.BytesIO(bytes(output))
    stream.seek(0)
    return stream


def build_consolidated_pdf(summary: dict[str, Any], rows: list[dict[str, Any]]) -> io.BytesIO:
    try:
        A4, canvas = _import_reportlab()
    except RuntimeError:
        lines = [
            "Matriz Consolidada y Semaforizada de Contratos",
            f"Generado: {current_utc_iso()}",
            "",
            f"Contratos totales: {summary.get('total_contracts', 0)}",
            f"Alto riesgo: {summary.get('high_risk', 0)}",
            f"Pendientes revision: {summary.get('pending_review', 0)}",
            f"Vo.Bo.: {summary.get('vo_bo', 0)}",
            "",
            "Proyecto | Contraparte | Riesgo | Estado | Ultima actualizacion",
        ]
        for row in rows:
            lines.append(
                f"{row.get('project_name', 'Contrato')} | {row.get('counterparty', 'N/A')} | "
                f"{(row.get('risk_level') or 'medium').upper()} ({row.get('risk_score', 0)}) | "
                f"{row.get('status', 'N/A')} | {row.get('updated_at', 'N/A')}"
            )
        return _build_plain_pdf(lines)

    buff = io.BytesIO()
    page_w, page_h = A4
    pdf = canvas.Canvas(buff, pagesize=A4)

    def write_line(text: str, y_value: float, *, x_value: float = 36, size: int = 9) -> float:
        pdf.setFont("Helvetica", size)
        chunks = textwrap.wrap(text, width=125) or [""]
        for chunk in chunks:
            nonlocal_page = y_value
            if nonlocal_page < 42:
                pdf.showPage()
                pdf.setFont("Helvetica", size)
                nonlocal_page = page_h - 36
            pdf.drawString(x_value, nonlocal_page, chunk)
            y_value = nonlocal_page - 12
        return y_value

    y = page_h - 40
    pdf.setFont("Helvetica-Bold", 15)
    pdf.drawString(36, y, "Matriz Consolidada y Semaforizada de Contratos")
    y -= 16
    pdf.setFont("Helvetica", 9)
    pdf.drawString(36, y, f"Generado: {current_utc_iso()}")
    y -= 18

    for line in [
        f"Contratos totales: {summary.get('total_contracts', 0)}",
        f"Alto riesgo: {summary.get('high_risk', 0)}",
        f"Pendientes revision: {summary.get('pending_review', 0)}",
        f"Vo.Bo.: {summary.get('vo_bo', 0)}",
    ]:
        y = write_line(line, y)

    y -= 6
    y = write_line("Proyecto | Contraparte | Riesgo | Estado | Ultima actualizacion", y, size=10)
    y -= 6

    for row in rows:
        risk = (row.get("risk_level") or "medium").upper()
        line = (
            f"{row.get('project_name', 'Contrato')} | "
            f"{row.get('counterparty', 'N/A')} | "
            f"{risk} ({row.get('risk_score', 0)}) | "
            f"{row.get('status', 'N/A')} | "
            f"{row.get('updated_at', 'N/A')}"
        )
        y = write_line(line, y)

    pdf.save()
    buff.seek(0)
    return buff


def build_dictamen_pdf(dictamen: dict[str, Any]) -> io.BytesIO:
    try:
        A4, canvas = _import_reportlab()
    except RuntimeError:
        lines = [
            "Dictamen Individual de Contrato",
            f"Generado: {dictamen.get('generated_at', current_utc_iso())}",
            "",
            f"Proyecto: {dictamen.get('project_name', 'N/A')}",
            f"Contrato: {dictamen.get('document_name', 'N/A')}",
            f"Tipo: {dictamen.get('contract_type', 'N/A')}",
            f"Contraparte: {dictamen.get('counterparty', 'N/A')}",
            f"Riesgo global: {(dictamen.get('overall_risk') or {}).get('level', 'N/A')}",
            f"Conclusion: {dictamen.get('conclusion', 'N/A')}",
            "",
            "Incumplimientos:",
        ]
        for item in dictamen.get("incumplimientos", [])[:16]:
            evidence = item.get("evidence", {})
            lines.append(f"- {item.get('clause_label', 'Clausula')}: {item.get('detail', '')}")
            lines.append(
                f"  Evidencia ({evidence.get('section', 'No identificada')}): {evidence.get('snippet', '')}"
            )
        lines.append("")
        lines.append("Dudas:")
        for item in dictamen.get("dudas", [])[:16]:
            lines.append(f"- {item.get('clause_label', 'Clausula')}: {item.get('detail', '')}")
        lines.append("")
        lines.append("Vo.Bo.:")
        for note in dictamen.get("vo_bo", [])[:12]:
            lines.append(f"- {note}")
        lines.append("")
        lines.append("Recomendaciones:")
        for rec in dictamen.get("recomendaciones", [])[:16]:
            lines.append(f"- {rec}")
        lines.append("")
        lines.append(dictamen.get("disclaimer", DISCLAIMER))
        return _build_plain_pdf(lines)

    buff = io.BytesIO()
    page_w, page_h = A4
    pdf = canvas.Canvas(buff, pagesize=A4)

    def write_line(text: str, y_value: float, *, x_value: float = 36, size: int = 9) -> float:
        pdf.setFont("Helvetica", size)
        chunks = textwrap.wrap(text, width=125) or [""]
        for chunk in chunks:
            nonlocal_page = y_value
            if nonlocal_page < 42:
                pdf.showPage()
                pdf.setFont("Helvetica", size)
                nonlocal_page = page_h - 36
            pdf.drawString(x_value, nonlocal_page, chunk)
            y_value = nonlocal_page - 12
        return y_value

    y = page_h - 40
    pdf.setFont("Helvetica-Bold", 15)
    pdf.drawString(36, y, "Dictamen Individual de Contrato")
    y -= 16
    pdf.setFont("Helvetica", 9)
    pdf.drawString(36, y, f"Generado: {dictamen.get('generated_at', current_utc_iso())}")
    y -= 18

    metadata_lines = [
        f"Proyecto: {dictamen.get('project_name', 'N/A')}",
        f"Contrato: {dictamen.get('document_name', 'N/A')}",
        f"Tipo: {dictamen.get('contract_type', 'N/A')}",
        f"Contraparte: {dictamen.get('counterparty', 'N/A')}",
        f"Riesgo global: {(dictamen.get('overall_risk') or {}).get('level', 'N/A')}",
        f"Conclusion: {dictamen.get('conclusion', 'N/A')}",
    ]
    for line in metadata_lines:
        y = write_line(line, y)

    y -= 8
    y = write_line("Resumen contractual:", y, size=10)
    resumen = dictamen.get("resumen", {})
    y = write_line(f"Objeto: {resumen.get('objeto', 'N/A')}", y, x_value=46)
    y = write_line(f"Jurisdiccion: {resumen.get('jurisdiccion', 'N/A')}", y, x_value=46)
    parties = resumen.get("partes", [])
    if parties:
        for party in parties[:3]:
            y = write_line(f"Parte: {party}", y, x_value=46)

    y -= 8
    y = write_line("Incumplimientos detectados:", y, size=10)
    for item in dictamen.get("incumplimientos", [])[:12]:
        y = write_line(f"- {item.get('clause_label', 'Clausula')}: {item.get('detail', '')}", y, x_value=46)
        evidence = item.get("evidence", {})
        y = write_line(
            f"  Evidencia ({evidence.get('section', 'No identificada')}): {evidence.get('snippet', '')}",
            y,
            x_value=56,
            size=8,
        )

    y -= 8
    y = write_line("Dudas y pendientes:", y, size=10)
    for item in dictamen.get("dudas", [])[:12]:
        y = write_line(f"- {item.get('clause_label', 'Clausula')}: {item.get('detail', '')}", y, x_value=46)

    y -= 8
    y = write_line("Vo.Bo. registrados:", y, size=10)
    for note in dictamen.get("vo_bo", [])[:8]:
        y = write_line(f"- {note}", y, x_value=46)

    y -= 8
    y = write_line("Recomendaciones:", y, size=10)
    for rec in dictamen.get("recomendaciones", [])[:12]:
        y = write_line(f"- {rec}", y, x_value=46)

    y -= 8
    y = write_line(dictamen.get("disclaimer", DISCLAIMER), y, size=8)

    pdf.save()
    buff.seek(0)
    return buff


def _rag_build_federal_job(limit: int | None) -> None:
    RAG_STATE["running"] = True
    RAG_STATE["mode"] = "federal"
    RAG_STATE["last_error"] = None
    try:
        stats = LEGAL_RAG.rebuild_from_diputados(limit=limit)
        RAG_STATE["last_stats"] = stats
        RAG_STATE["last_run"] = current_utc_iso()
    except Exception as exc:  # pragma: no cover - operativo
        RAG_STATE["last_error"] = str(exc)
    finally:
        RAG_STATE["running"] = False
        RAG_STATE["mode"] = None


def _rag_build_state_job(
    limit_states: int | None,
    max_pages: int,
    include_cdmx: bool,
) -> None:
    RAG_STATE["running"] = True
    RAG_STATE["mode"] = "state"
    RAG_STATE["last_error"] = None
    try:
        stats = LEGAL_RAG.rebuild_state_laws_from_gobiernos(
            limit_states=limit_states,
            include_cdmx=include_cdmx,
            per_entry_max_pages=max_pages,
        )
        RAG_STATE["last_stats"] = stats
        RAG_STATE["last_run"] = current_utc_iso()
    except Exception as exc:  # pragma: no cover - operativo
        RAG_STATE["last_error"] = str(exc)
    finally:
        RAG_STATE["running"] = False
        RAG_STATE["mode"] = None


def _safe_check_federal_updates() -> dict[str, Any] | None:
    try:
        return LEGAL_RAG.check_federal_updates(refresh_on_change=True)
    except Exception as exc:  # pragma: no cover - operativo
        return {"error": str(exc), "checked_at": current_utc_iso()}


def run_analysis_for_document(metadata: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    document_id = metadata["document_id"]
    text_path = Path(metadata["text_path"])
    if not text_path.exists():
        raise RuntimeError("Texto del documento no disponible.")

    raw_text = text_path.read_text(encoding="utf-8")
    updates_check = _safe_check_federal_updates()

    analysis = analyze_contract(
        document_id=metadata["document_id"],
        document_name=metadata["document_name"],
        source_type=metadata["source_type"],
        repository_path=metadata["repository_path"],
        raw_text=raw_text,
    )
    analysis["contract_context"] = {
        "project_name": metadata.get("project_name") or metadata.get("document_name"),
        "client_name": metadata.get("client_name") or "Sin cliente",
        "contract_type": metadata.get("contract_type", ""),
        "counterparty": metadata.get("counterparty", ""),
        "expected_sign_date": metadata.get("expected_sign_date", ""),
        "objective": metadata.get("objective", ""),
        "include_historical": bool(metadata.get("include_historical", False)),
    }
    analysis["legal_grounding"] = build_legal_grounding_for_analysis(analysis)
    analysis["federal_updates_check"] = updates_check
    analysis, llm_meta = enrich_analysis_with_gemini(analysis)
    executive_summary, executive_meta = generate_executive_summary_with_gemini(analysis)
    if executive_summary:
        analysis["executive_summary"] = executive_summary
    analysis["llm_review"] = {
        "risk_enrichment": llm_meta,
        "executive_summary": executive_meta,
    }
    analysis = apply_feedback_learning(analysis, document_id)
    save_json(analysis_path(document_id), analysis)
    return analysis, updates_check


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/api/health")
def health() -> Any:
    return jsonify({"status": "ok", "timestamp": current_utc_iso()})


@app.get("/api/llm/status")
def llm_status() -> Any:
    return jsonify(get_llm_status())


@app.get("/api/dashboard")
def dashboard() -> Any:
    client_name = (request.args.get("client_name") or "").strip()
    rows = build_contract_rows(client_name_filter=client_name or None)
    clients = sorted(
        {
            (row.get("client_name") or "").strip()
            for row in build_contract_rows()
            if (row.get("client_name") or "").strip()
        }
    )
    return jsonify(
        {
            "generated_at": current_utc_iso(),
            "summary": build_dashboard_summary(rows),
            "contracts": rows,
            "clients": clients,
            "filters": {"client_name": client_name or ""},
        }
    )


@app.get("/api/documents")
def list_documents() -> Any:
    index = load_documents_index()
    documents = []
    for item in index["documents"]:
        enriched = dict(item)
        enriched["analysis_available"] = analysis_path(item["document_id"]).exists()
        documents.append(enriched)
    documents.sort(key=lambda d: d.get("uploaded_at", ""), reverse=True)
    return jsonify({"documents": documents})


@app.get("/api/rag/status")
def rag_status() -> Any:
    status = LEGAL_RAG.status()
    status["job"] = RAG_STATE
    return jsonify(status)


@app.post("/api/rag/rebuild")
def rag_rebuild() -> Any:
    payload = request.get_json(silent=True) or {}
    limit = payload.get("limit")
    if limit is not None:
        try:
            limit = int(limit)
        except (ValueError, TypeError):
            return jsonify({"error": "limit debe ser entero."}), 400
        if limit <= 0:
            return jsonify({"error": "limit debe ser > 0."}), 400
    else:
        limit = None

    if RAG_STATE["running"]:
        return jsonify({"error": "Ya existe una indexacion RAG en proceso."}), 409

    worker = threading.Thread(target=_rag_build_federal_job, args=(limit,), daemon=True)
    worker.start()
    return jsonify({"message": "Indexacion RAG federal iniciada.", "limit": limit}), 202


@app.post("/api/rag/rebuild-state")
def rag_rebuild_state() -> Any:
    payload = request.get_json(silent=True) or {}
    limit_states = payload.get("limit_states")
    max_pages = payload.get("max_pages", 30)
    include_cdmx = bool(payload.get("include_cdmx", True))

    if limit_states is not None:
        try:
            limit_states = int(limit_states)
        except (TypeError, ValueError):
            return jsonify({"error": "limit_states debe ser entero."}), 400
        if limit_states <= 0:
            return jsonify({"error": "limit_states debe ser > 0."}), 400

    try:
        max_pages = int(max_pages)
    except (TypeError, ValueError):
        return jsonify({"error": "max_pages debe ser entero."}), 400
    max_pages = max(8, min(max_pages, 80))

    if RAG_STATE["running"]:
        return jsonify({"error": "Ya existe una indexacion RAG en proceso."}), 409

    worker = threading.Thread(
        target=_rag_build_state_job,
        args=(limit_states, max_pages, include_cdmx),
        daemon=True,
    )
    worker.start()
    return (
        jsonify(
            {
                "message": "Indexacion RAG estatal/CDMX iniciada.",
                "limit_states": limit_states,
                "max_pages": max_pages,
                "include_cdmx": include_cdmx,
            }
        ),
        202,
    )


@app.post("/api/rag/check-federal-updates")
def rag_check_federal_updates() -> Any:
    result = _safe_check_federal_updates()
    return jsonify({"updates_check": result})


@app.post("/api/rag/search")
def rag_search() -> Any:
    payload = request.get_json(silent=True) or {}
    query = (payload.get("query") or "").strip()
    top_k = payload.get("top_k", 5)
    scope = (payload.get("scope") or "").strip().lower() or None
    jurisdiction = (payload.get("jurisdiction") or "").strip() or None
    verify_updates = bool(payload.get("verify_updates", True))

    if not query:
        return jsonify({"error": "query es obligatoria."}), 400
    try:
        top_k = int(top_k)
    except (TypeError, ValueError):
        return jsonify({"error": "top_k debe ser entero."}), 400
    top_k = max(1, min(top_k, 15))

    updates_check = _safe_check_federal_updates() if verify_updates else None
    hits = LEGAL_RAG.search(query, top_k=top_k, scope=scope, jurisdiction=jurisdiction)
    return jsonify({"query": query, "results": hits, "updates_check": updates_check})


@app.post("/api/documents")
def upload_document() -> Any:
    ensure_storage()
    if "file" not in request.files:
        return jsonify({"error": "No se envio archivo en el campo 'file'."}), 400

    uploaded_file = request.files["file"]
    if uploaded_file.filename is None or uploaded_file.filename.strip() == "":
        return jsonify({"error": "Selecciona un archivo valido."}), 400

    filename = secure_filename(uploaded_file.filename)
    source_type = infer_source_type(filename)
    if source_type not in ALLOWED_EXTENSIONS:
        return jsonify({"error": "Formato no soportado. Usa TXT, DOCX o PDF."}), 400

    project_name = (request.form.get("project_name") or "").strip()
    client_name = (request.form.get("client_name") or "").strip()
    contract_type = (request.form.get("contract_type") or "").strip()
    counterparty = (request.form.get("counterparty") or "").strip()
    expected_sign_date = (request.form.get("expected_sign_date") or "").strip()
    objective = (request.form.get("objective") or "").strip()
    include_historical = parse_bool(request.form.get("include_historical"))

    document_id = f"doc_{uuid.uuid4().hex[:12]}"
    stored_filename = f"{document_id}.{source_type}"
    upload_path = UPLOADS_DIR / stored_filename
    uploaded_file.save(str(upload_path))

    try:
        extracted_text = parse_document_text(upload_path, source_type=source_type)
    except Exception as exc:  # pragma: no cover - ruta de fallo operativo
        upload_path.unlink(missing_ok=True)
        return jsonify({"error": f"No se pudo procesar el archivo: {exc}"}), 400

    if not extracted_text.strip():
        upload_path.unlink(missing_ok=True)
        return jsonify({"error": "No se pudo extraer texto utilizable del documento."}), 400

    text_path = TEXT_DIR / f"{document_id}.txt"
    text_path.write_text(extracted_text, encoding="utf-8")

    metadata = {
        "document_id": document_id,
        "document_name": filename,
        "project_name": project_name or filename,
        "client_name": client_name or "Sin cliente",
        "contract_type": contract_type,
        "counterparty": counterparty,
        "expected_sign_date": expected_sign_date,
        "objective": objective,
        "include_historical": include_historical,
        "source_type": source_type,
        "repository_path": str(upload_path),
        "text_path": str(text_path),
        "uploaded_at": current_utc_iso(),
    }

    index = load_documents_index()
    index["documents"].append(metadata)
    save_documents_index(index)
    return jsonify({"document": metadata}), 201


@app.post("/api/analyze/<document_id>")
def analyze_document(document_id: str) -> Any:
    metadata = get_document(document_id)
    if metadata is None:
        return jsonify({"error": "Documento no encontrado."}), 404

    try:
        analysis, updates_check = run_analysis_for_document(metadata)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    index = load_documents_index()
    for item in index["documents"]:
        if item["document_id"] == document_id:
            item["analyzed_at"] = current_utc_iso()
            break
    save_documents_index(index)

    return jsonify({"analysis": analysis, "updates_check": updates_check})


@app.post("/api/analyze-all")
def analyze_all_documents() -> Any:
    index = load_documents_index()
    docs = index.get("documents", [])
    if not docs:
        return jsonify({"processed": 0, "ok": 0, "errors": 0, "results": []})

    results = []
    ok = 0
    errors = 0
    for item in docs:
        try:
            analysis, _ = run_analysis_for_document(item)
            item["analyzed_at"] = current_utc_iso()
            ok += 1
            results.append(
                {
                    "document_id": item["document_id"],
                    "document_name": item.get("document_name"),
                    "ok": True,
                    "risk_level": (analysis.get("overall_risk") or {}).get("level", "medium"),
                }
            )
        except Exception as exc:
            errors += 1
            results.append(
                {
                    "document_id": item.get("document_id"),
                    "document_name": item.get("document_name"),
                    "ok": False,
                    "error": str(exc),
                }
            )

    save_documents_index(index)
    return jsonify({"processed": len(results), "ok": ok, "errors": errors, "results": results})


@app.get("/api/analysis/<document_id>")
def get_analysis(document_id: str) -> Any:
    path = analysis_path(document_id)
    if not path.exists():
        return jsonify({"error": "Analisis no disponible para este documento."}), 404
    analysis = load_json(path)
    refresh_summary = parse_bool(request.args.get("refresh_summary"))
    current_summary = (analysis.get("executive_summary") or "").strip()
    if refresh_summary or executive_summary_needs_refresh(current_summary):
        summary_text, summary_meta = generate_executive_summary_with_gemini(analysis)
        if summary_text:
            analysis["executive_summary"] = summary_text
            llm_review = analysis.get("llm_review")
            if not isinstance(llm_review, dict):
                llm_review = {}
            llm_review["executive_summary"] = summary_meta
            analysis["llm_review"] = llm_review
            save_json(path, analysis)
    return jsonify({"analysis": analysis})


@app.get("/api/feedback/<document_id>")
def get_feedback(document_id: str) -> Any:
    metadata = get_document(document_id)
    if metadata is None:
        return jsonify({"error": "Documento no encontrado."}), 404
    entries = load_feedback_entries(document_id)
    return jsonify({"document_id": document_id, "summary": summarize_feedback(entries), "entries": entries[-80:]})


@app.get("/api/review/queue")
def get_review_queue() -> Any:
    client_name = (request.args.get("client_name") or "").strip() or None
    status = (request.args.get("status") or "").strip() or None
    page_raw = request.args.get("page", 1)
    per_page_raw = request.args.get("per_page", 10)

    try:
        page = int(page_raw)
    except (TypeError, ValueError):
        return jsonify({"error": "page debe ser entero."}), 400
    try:
        per_page = int(per_page_raw)
    except (TypeError, ValueError):
        return jsonify({"error": "per_page debe ser entero."}), 400

    page = max(1, page)
    if per_page not in {10, 50, 100}:
        return jsonify({"error": "per_page solo permite 10, 50 o 100."}), 400

    rows = build_review_queue_rows(client_name_filter=client_name, status_filter=status)
    total = len(rows)
    start = (page - 1) * per_page
    end = start + per_page
    paged = rows[start:end]
    clients = sorted(
        {
            (row.get("client_name") or "").strip()
            for row in build_contract_rows()
            if (row.get("client_name") or "").strip()
        }
    )

    return jsonify(
        {
            "items": paged,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": max(1, (total + per_page - 1) // per_page),
            },
            "filters": {"client_name": client_name or "", "status": status or ""},
            "clients": clients,
        }
    )


@app.post("/api/feedback")
def post_feedback() -> Any:
    payload = request.get_json(silent=True) or {}
    document_id = (payload.get("document_id") or "").strip()
    if not document_id:
        return jsonify({"error": "document_id es obligatorio."}), 400
    metadata = get_document(document_id)
    if metadata is None:
        return jsonify({"error": "Documento no encontrado."}), 404

    signal = normalize_signal(payload.get("signal", ""))
    if signal not in ALLOWED_FEEDBACK_SIGNALS:
        return jsonify({"error": "signal invalido. Usa: incumplimiento, duda, vo_bo."}), 400

    corrected_risk = normalize_risk_level(payload.get("corrected_risk", ""))
    clause_type = (payload.get("clause_type") or "").strip()
    note = (payload.get("note") or "").strip()
    reviewer = (payload.get("reviewer") or "abogado").strip()

    entry = {
        "feedback_id": f"fb_{uuid.uuid4().hex[:12]}",
        "document_id": document_id,
        "document_name": metadata.get("document_name"),
        "clause_type": clause_type,
        "signal": signal,
        "corrected_risk": corrected_risk or None,
        "note": note,
        "reviewer": reviewer,
        "created_at": current_utc_iso(),
    }
    append_feedback_entry(entry)

    existing_analysis = load_analysis_if_exists(document_id)
    if existing_analysis is not None:
        updated = apply_feedback_learning(existing_analysis, document_id)
        save_json(analysis_path(document_id), updated)

    entries = load_feedback_entries(document_id)
    return jsonify({"message": "Feedback registrado.", "entry": entry, "summary": summarize_feedback(entries)}), 201


@app.post("/api/clause-edit")
def post_clause_edit() -> Any:
    payload = request.get_json(silent=True) or {}
    document_id = (payload.get("document_id") or "").strip()
    clause_type = (payload.get("clause_type") or "").strip()
    revised_text = (payload.get("revised_text") or "").strip()
    note = (payload.get("note") or "").strip()
    reviewer = (payload.get("reviewer") or "abogado_sr").strip()

    if not document_id:
        return jsonify({"error": "document_id es obligatorio."}), 400
    if not clause_type:
        return jsonify({"error": "clause_type es obligatorio."}), 400
    if len(revised_text) < 30:
        return jsonify({"error": "revised_text debe contener al menos 30 caracteres."}), 400

    metadata = get_document(document_id)
    if metadata is None:
        return jsonify({"error": "Documento no encontrado."}), 404

    analysis = load_analysis_if_exists(document_id)
    if analysis is None:
        return jsonify({"error": "Analiza el documento antes de editar clausulas."}), 400

    clause_found = False
    for clause in analysis.get("clauses", []):
        if (clause.get("clause_type") or "").strip() != clause_type:
            continue
        clause_found = True
        clause["extracted_text"] = revised_text
        clause["status"] = "found"
        clause["manual_edit"] = {
            "updated_at": current_utc_iso(),
            "reviewer": reviewer,
            "note": note,
        }
        try:
            clause["risk"] = assess_clause_risk(clause_type, clause.get("status", "found"), revised_text)
        except Exception:
            pass
        break

    if not clause_found:
        return jsonify({"error": "clause_type no encontrada en el analisis."}), 404

    analysis["overall_risk"] = recompute_overall_risk(analysis.get("clauses", []))
    analysis["last_manual_edit_at"] = current_utc_iso()
    save_json(analysis_path(document_id), analysis)

    edits_payload = load_clause_edits(document_id)
    edits_payload.setdefault("by_clause", {})
    edits_payload["by_clause"][clause_type] = {
        "revised_text": revised_text,
        "note": note,
        "reviewer": reviewer,
        "updated_at": current_utc_iso(),
    }
    save_clause_edits(document_id, edits_payload)

    return jsonify(
        {
            "message": "Clausula actualizada para revision legal.",
            "document_id": document_id,
            "clause_type": clause_type,
            "updated_at": current_utc_iso(),
        }
    )


@app.post("/api/clause-rewrite-suggest")
def suggest_clause_rewrite() -> Any:
    payload = request.get_json(silent=True) or {}
    document_id = (payload.get("document_id") or "").strip()
    clause_type = (payload.get("clause_type") or "").strip()

    if not document_id:
        return jsonify({"error": "document_id es obligatorio."}), 400
    if not clause_type:
        return jsonify({"error": "clause_type es obligatorio."}), 400

    analysis = load_analysis_if_exists(document_id)
    if analysis is None:
        return jsonify({"error": "Analiza el documento antes de reescribir clausulas."}), 400

    clause = None
    for item in analysis.get("clauses", []):
        if (item.get("clause_type") or "").strip() == clause_type:
            clause = item
            break
    if clause is None:
        return jsonify({"error": "clause_type no encontrada en el analisis."}), 404

    refs = []
    for grounding in analysis.get("legal_grounding", []):
        if grounding.get("clause_type") != clause_type:
            continue
        refs = grounding.get("references", [])[:3]
        break

    rewritten, meta = rewrite_clause_with_gemini(
        clause_label=clause.get("clause_label", clause_type),
        clause_type=clause_type,
        current_text=clause.get("extracted_text", ""),
        recommendation=(clause.get("risk") or {}).get("recommendation_initial", ""),
        legal_refs=refs,
    )
    if not rewritten:
        error = meta.get("error") or "No se pudo generar sugerencia de redaccion."
        return jsonify({"error": error, "llm": meta}), 502

    return jsonify(
        {
            "document_id": document_id,
            "clause_type": clause_type,
            "suggested_text": rewritten,
            "llm": meta,
        }
    )


@app.get("/api/dictamen/<document_id>")
def get_dictamen(document_id: str) -> Any:
    metadata = get_document(document_id)
    if metadata is None:
        return jsonify({"error": "Documento no encontrado."}), 404

    analysis = load_analysis_if_exists(document_id)
    if analysis is None:
        return jsonify({"error": "Analiza el documento antes de emitir dictamen."}), 400

    feedback_entries = load_feedback_entries(document_id)
    dictamen = build_dictamen_payload(metadata, analysis, feedback_entries)
    return jsonify({"dictamen": dictamen})


@app.get("/api/export/consolidated.pdf")
def export_consolidated_pdf() -> Any:
    rows = build_contract_rows()
    summary = build_dashboard_summary(rows)
    try:
        pdf_stream = build_consolidated_pdf(summary, rows)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500

    file_name = f"matriz_consolidada_{dt.date.today().isoformat()}.pdf"
    return send_file(
        pdf_stream,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=file_name,
    )


@app.get("/api/export/dictamen/<document_id>.pdf")
def export_dictamen_pdf(document_id: str) -> Any:
    metadata = get_document(document_id)
    if metadata is None:
        return jsonify({"error": "Documento no encontrado."}), 404

    analysis = load_analysis_if_exists(document_id)
    if analysis is None:
        return jsonify({"error": "Analiza el documento antes de exportar dictamen."}), 400

    feedback_entries = load_feedback_entries(document_id)
    dictamen = build_dictamen_payload(metadata, analysis, feedback_entries)

    try:
        pdf_stream = build_dictamen_pdf(dictamen)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500

    file_name = f"dictamen_{document_id}_{dt.date.today().isoformat()}.pdf"
    return send_file(
        pdf_stream,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=file_name,
    )


@app.post("/api/questions")
def ask_question() -> Any:
    payload = request.get_json(silent=True) or {}
    document_id = (payload.get("document_id") or "").strip()
    question = (payload.get("question") or "").strip()

    if not document_id:
        return jsonify({"error": "document_id es obligatorio."}), 400
    if not question:
        return jsonify({"error": "question es obligatoria."}), 400

    metadata = get_document(document_id)
    if metadata is None:
        return jsonify({"error": "Documento no encontrado."}), 404

    path = analysis_path(document_id)
    if not path.exists():
        raw_text = Path(metadata["text_path"]).read_text(encoding="utf-8")
        analysis = analyze_contract(
            document_id=metadata["document_id"],
            document_name=metadata["document_name"],
            source_type=metadata["source_type"],
            repository_path=metadata["repository_path"],
            raw_text=raw_text,
        )
    else:
        analysis = load_json(path)

    analysis = apply_feedback_learning(analysis, document_id)
    save_json(path, analysis)

    updates_check = _safe_check_federal_updates()
    feedback_entries = load_feedback_entries(document_id)
    feedback_summary = summarize_feedback(feedback_entries)
    conversation_state = load_conversation_state(document_id)
    conversation_state["objective"] = infer_conversation_objective(
        question,
        conversation_state.get("objective", ""),
    )
    conversation_context = build_conversation_context(
        conversation_state,
        question=question,
        metadata=metadata,
        analysis=analysis,
    )

    if is_small_talk_question(question):
        response = build_small_talk_response(
            metadata,
            analysis,
            question=question,
            conversation_state=conversation_state,
        )
        noop_meta = build_default_llm_meta()
        response["legal_citations"] = []
        response["legal_fichas"] = []
        response["research_plan"] = {}
        response["feedback_summary"] = feedback_summary
        response["llm_response"] = {"planner": noop_meta, "dialogue": noop_meta}
        response["updates_check"] = updates_check
        response["document_id"] = document_id
        response["document_name"] = metadata["document_name"]
        response["disclaimer"] = DISCLAIMER
        conversation_state = append_conversation_turn(conversation_state, role="user", text=question)
        conversation_state = append_conversation_turn(
            conversation_state,
            role="assistant",
            text=response.get("answer", ""),
        )
        save_conversation_state(document_id, conversation_state)
        return jsonify(response)

    law_question = is_law_applicable_intent(question)
    response = answer_question(analysis, question)
    legal_hits: list[dict[str, Any]] = []
    legal_fichas: list[dict[str, Any]] = []

    research_plan, planner_meta = plan_research_actions_with_gemini(
        question=question,
        analysis=analysis,
        feedback_summary=feedback_summary,
        conversation_context=conversation_context,
    )
    if research_plan and research_plan.get("needs_research"):
        legal_hits = retrieve_legal_hits_from_tasks(
            research_plan.get("research_tasks", []),
            question=question,
            analysis=analysis,
            top_k_total=6,
        )

    if not legal_hits and should_inject_legal_context(question):
        legal_query = rewrite_legal_query(question)
        jurisdiction_hint = infer_jurisdiction_context(question, analysis)
        legal_hits = retrieve_legal_hits(legal_query, jurisdiction_hint, top_k=4)

    if law_question and not legal_hits:
        legal_hits = retrieve_legal_hits(
            "jurisdicción competencia contractual ley aplicable código civil federal código de comercio",
            infer_jurisdiction_context(question, analysis),
            top_k=4,
        )

    if research_plan is None:
        heuristic_tasks = []
        if legal_hits and should_inject_legal_context(question):
            heuristic_tasks.append(
                {
                    "query": rewrite_legal_query(question)[:180],
                    "purpose": "Sustentar analisis y dictamen con evidencia normativa.",
                    "clause_type": "",
                    "scope": "mixed",
                    "jurisdiction_hint": infer_jurisdiction_context(question, analysis) or "",
                }
            )
        research_plan = {
            "conversation_mode": "analisis",
            "needs_research": bool(legal_hits),
            "research_tasks": heuristic_tasks,
            "assistant_intent": "Guiar analisis de riesgos y dictamen.",
        }

    if legal_hits:
        legal_fichas = build_legal_fichas(
            legal_hits,
            (research_plan or {}).get("research_tasks", []),
        )

    llm_answer, llm_meta = draft_dialogue_reply_with_gemini(
        question=question,
        analysis=analysis,
        legal_fichas=legal_fichas,
        feedback_summary=feedback_summary,
        research_plan=research_plan,
        conversation_context=conversation_context,
    )
    if not llm_answer:
        legacy_answer, legacy_meta = answer_question_with_gemini(
            question=question,
            analysis=analysis,
            legal_hits=legal_hits,
            feedback_summary=feedback_summary,
            conversation_context=conversation_context,
        )
        if legacy_answer:
            llm_answer = legacy_answer
            llm_meta = legacy_meta

    if llm_answer and law_question and not is_law_focused_answer(llm_answer.get("answer", "")):
        llm_answer = None

    if llm_answer:
        response["answer"] = llm_answer.get("answer", response["answer"])
        response["confidence"] = llm_answer.get("confidence", response.get("confidence", 0.65))
        response["risk_estimate"] = llm_answer.get("risk_estimate", response.get("risk_estimate", {}))
        response["missing_evidence"] = llm_answer.get("missing_evidence", response.get("missing_evidence", False))
        response["human_review_required"] = llm_answer.get(
            "human_review_required",
            response.get("human_review_required", True),
        )
        if isinstance(llm_answer.get("next_actions"), list):
            response["next_actions"] = llm_answer.get("next_actions", [])[:3]

    response["legal_citations"] = legal_hits
    response["legal_fichas"] = legal_fichas
    response["research_plan"] = research_plan or {}
    if should_blend_feedback_in_answer(question):
        response["answer"] = blend_feedback_into_answer(response["answer"], feedback_summary)
    if legal_hits and should_inject_legal_context(question) and not llm_answer and not law_question:
        response["answer"] = blend_contract_and_legal_answer(response["answer"], legal_hits)
    response["feedback_summary"] = feedback_summary
    response["llm_response"] = {
        "planner": planner_meta,
        "dialogue": llm_meta,
    }
    response["updates_check"] = updates_check
    response["document_id"] = document_id
    response["document_name"] = metadata["document_name"]
    response["disclaimer"] = DISCLAIMER
    conversation_state = append_conversation_turn(conversation_state, role="user", text=question)
    conversation_state = append_conversation_turn(
        conversation_state,
        role="assistant",
        text=response.get("answer", ""),
    )
    save_conversation_state(document_id, conversation_state)
    return jsonify(response)


def build_legal_grounding_for_analysis(analysis: dict[str, Any]) -> list[dict[str, Any]]:
    queries = {
        "parties": "capacidad de las partes y consentimiento contrato",
        "object": "objeto del contrato validez obligaciones",
        "term": "vigencia plazo contractual renovacion",
        "payments": "pago contraprestacion mora contractual",
        "termination": "terminacion rescision incumplimiento contrato",
        "liability": "responsabilidad civil contractual limitacion daños",
        "indemnification": "indemnizacion daños perjuicios defensa",
        "confidentiality": "confidencialidad secreto proteccion datos",
        "intellectual_property": "propiedad intelectual derechos de autor licencia",
        "jurisdiction": "jurisdiccion competencia ley aplicable",
        "compliance": "cumplimiento normativo anticorrupcion proteccion datos",
    }
    grounding = []
    summary_jurisdiction = analysis.get("summary", {}).get("jurisdiction", "")
    for clause in analysis.get("clauses", []):
        clause_type = clause.get("clause_type")
        query = queries.get(clause_type)
        if not query:
            continue
        jurisdiction_hint = infer_jurisdiction_context(summary_jurisdiction, analysis)
        hits = retrieve_legal_hits(query, jurisdiction_hint, top_k=2)
        grounding.append(
            {
                "clause_type": clause_type,
                "references": hits,
            }
        )
    return grounding


def retrieve_legal_hits(
    query: str,
    jurisdiction_hint: str | None,
    *,
    top_k: int,
) -> list[dict[str, Any]]:
    if jurisdiction_hint:
        state_hits = LEGAL_RAG.search(
            query,
            top_k=max(1, top_k // 2),
            scope="state",
            jurisdiction=jurisdiction_hint,
        )
        federal_hits = LEGAL_RAG.search(
            query,
            top_k=max(1, top_k - len(state_hits)),
            scope="federal",
        )
        merged = dedup_hits(state_hits + federal_hits)
        return sanitize_legal_hits(merged, top_k=top_k)
    direct = LEGAL_RAG.search(query, top_k=top_k * 2)
    return sanitize_legal_hits(direct, top_k=top_k)


def retrieve_legal_hits_from_tasks(
    tasks: list[dict[str, Any]],
    *,
    question: str,
    analysis: dict[str, Any],
    top_k_total: int = 6,
) -> list[dict[str, Any]]:
    if not tasks:
        return []

    collected: list[dict[str, Any]] = []
    default_hint = infer_jurisdiction_context(question, analysis)
    for task in tasks[:3]:
        query = (task.get("query") or "").strip()
        if len(query) < 4:
            continue
        scope = normalize_for_match(task.get("scope", "mixed"))
        if scope not in {"federal", "state", "mixed"}:
            scope = "mixed"
        jurisdiction_hint = (task.get("jurisdiction_hint") or "").strip() or default_hint

        if scope == "federal":
            hits = LEGAL_RAG.search(query, top_k=2, scope="federal")
            collected.extend(sanitize_legal_hits(hits, top_k=2))
        elif scope == "state":
            if jurisdiction_hint:
                hits = LEGAL_RAG.search(query, top_k=2, scope="state", jurisdiction=jurisdiction_hint)
            else:
                hits = LEGAL_RAG.search(query, top_k=2, scope="state")
            collected.extend(sanitize_legal_hits(hits, top_k=2))
        else:
            collected.extend(retrieve_legal_hits(query, jurisdiction_hint, top_k=2))

    return dedup_hits(collected)[:top_k_total]


def build_legal_fichas(legal_hits: list[dict[str, Any]], research_tasks: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    tasks = research_tasks or []
    fichas = []
    for idx, hit in enumerate(legal_hits, start=1):
        task = tasks[min(idx - 1, len(tasks) - 1)] if tasks else {}
        fichas.append(
            {
                "ficha_id": f"ficha_{idx:02d}",
                "law_name": hit.get("law_name", "Referencia normativa"),
                "article_label": hit.get("article_label", "Fragmento normativo"),
                "scope": hit.get("scope", "federal"),
                "jurisdiction": hit.get("jurisdiction", ""),
                "purpose": task.get("purpose", "Sustentar analisis y dictamen."),
                "clause_type": task.get("clause_type", ""),
                "snippet": hit.get("snippet", ""),
                "source_url": hit.get("ref_url") or hit.get("doc_url"),
            }
        )
    return fichas


def dedup_hits(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    ordered = []
    for hit in hits:
        key = (hit.get("law_key"), hit.get("article_label"))
        if key in seen:
            continue
        seen.add(key)
        ordered.append(hit)
    return ordered


def clean_text_for_ui(value: Any, *, max_len: int = 220) -> str:
    text = str(value or "")
    text = re.sub(r"[\x00-\x1F\x7F]+", " ", text)
    text = re.sub(r"[^0-9A-Za-zÁÉÍÓÚáéíóúÑñÜü.,;:(){}\[\]/%+\-_#&'\" ]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if max_len and len(text) > max_len:
        text = text[:max_len].rstrip()
    return text


def seems_garbled(text: str) -> bool:
    if not text:
        return True
    allowed_punct = set(".,;:()[]/%+-_#&'\" ")
    weird = 0
    letters = 0
    for ch in text:
        if ch.isalpha():
            letters += 1
            continue
        if ch.isdigit() or ch.isspace() or ch in allowed_punct:
            continue
        if unicodedata.category(ch).startswith("P"):
            continue
        weird += 1

    weird_ratio = weird / max(1, len(text))
    letter_ratio = letters / max(1, len(text))
    return weird_ratio > 0.18 or letter_ratio < 0.14


def sanitize_legal_hit(hit: dict[str, Any]) -> dict[str, Any] | None:
    scope = clean_text_for_ui(hit.get("scope"), max_len=30).lower() or "federal"
    jurisdiction = clean_text_for_ui(hit.get("jurisdiction"), max_len=80).replace("_", " ")
    law_name = clean_text_for_ui(hit.get("law_name"), max_len=180)
    article_label = clean_text_for_ui(hit.get("article_label"), max_len=80)
    snippet = clean_text_for_ui(hit.get("snippet"), max_len=220)

    if seems_garbled(law_name):
        if scope == "state" and jurisdiction:
            law_name = f"Referencia normativa estatal ({jurisdiction})"
        elif scope == "state":
            law_name = "Referencia normativa estatal"
        else:
            law_name = "Referencia normativa federal"

    if seems_garbled(article_label):
        article_label = "Fragmento normativo"
    if seems_garbled(snippet):
        snippet = "Texto legal recuperado; revisar fuente oficial."

    if not law_name and not article_label:
        return None

    cleaned = dict(hit)
    cleaned["law_name"] = law_name
    cleaned["article_label"] = article_label
    cleaned["snippet"] = snippet
    cleaned["scope"] = scope
    cleaned["jurisdiction"] = jurisdiction
    cleaned["ref_url"] = clean_text_for_ui(hit.get("ref_url"), max_len=280)
    cleaned["doc_url"] = clean_text_for_ui(hit.get("doc_url"), max_len=280)
    return cleaned


def sanitize_legal_hits(hits: list[dict[str, Any]], *, top_k: int) -> list[dict[str, Any]]:
    safe_hits: list[dict[str, Any]] = []
    for hit in hits:
        cleaned = sanitize_legal_hit(hit)
        if cleaned is None:
            continue
        safe_hits.append(cleaned)
        if len(safe_hits) >= top_k:
            break
    return safe_hits


def should_inject_legal_context(question: str) -> bool:
    q = normalize_for_match(question or "")
    intents = [
        "ley",
        "articulo",
        "jurisd",
        "norma",
        "codigo",
        "reglamento",
        "cumpl",
        "responsabilidad",
        "indemn",
        "termina",
        "confid",
        "pago",
    ]
    return any(token in q for token in intents)


def should_blend_feedback_in_answer(question: str) -> bool:
    q = normalize_for_match(question or "")
    intents = ["riesgo", "incumpl", "duda", "vobo", "vo bo", "dictamen", "revision", "clausula"]
    return any(token in q for token in intents)


def is_law_applicable_intent(question: str) -> bool:
    q = normalize_for_match(question or "")
    if "ley" in q and ("aplicable" in q or "aplica" in q):
        return True
    intents = ["ley aplicable", "jurisd", "foro", "tribunal", "competencia", "fuero", "marco normativo"]
    return any(token in q for token in intents)


def is_law_focused_answer(answer: str) -> bool:
    probe = normalize_for_match(answer or "")
    expected = [
        "ley",
        "aplicable",
        "norma",
        "jurisd",
        "foro",
        "tribunal",
        "federal",
        "estatal",
        "competencia",
        "codigo",
        "articulo",
    ]
    return any(token in probe for token in expected)


def is_generic_law_label(label: str) -> bool:
    norm = normalize_for_match(label or "")
    if not norm:
        return True
    return "marco legal" in norm or "referencia normativa" in norm


def blend_contract_and_legal_answer(base_answer: str, legal_hits: list[dict[str, Any]]) -> str:
    if not legal_hits:
        return base_answer

    top = None
    for candidate in legal_hits:
        law_name_probe = clean_text_for_ui(candidate.get("law_name"), max_len=160)
        if is_generic_law_label(law_name_probe):
            continue
        top = candidate
        break
    if top is None:
        top = legal_hits[0]

    law_name = clean_text_for_ui(top.get("law_name"), max_len=160) or "Referencia normativa aplicable"
    article = clean_text_for_ui(top.get("article_label"), max_len=80) or "Fragmento normativo"
    answer = base_answer.strip()
    if answer and answer[-1] not in ".!?":
        answer = answer + "."
    return f"{answer} Referencia normativa principal: {law_name} ({article})."


def rewrite_legal_query(question: str) -> str:
    q = normalize_for_match(question)
    if "indemn" in q:
        return "indemnización daños perjuicios responsabilidad civil código civil federal"
    if "confid" in q or "datos" in q:
        return "confidencialidad protección de datos personales ley federal de datos personales"
    if "jurisd" in q or "ley aplicable" in q or "foro" in q:
        return "jurisdicción competencia contractual código civil federal código de comercio"
    if "responsabilidad" in q or "daño" in q:
        return "responsabilidad civil contractual limitación de responsabilidad código civil federal"
    if "termina" in q or "resc" in q:
        return "rescisión terminación por incumplimiento código civil federal contratos"
    if "pago" in q or "factura" in q:
        return "obligaciones de pago mora intereses contratos mercantiles"
    if "propiedad intelectual" in q or "derechos de autor" in q:
        return "propiedad intelectual derechos de autor licencias ley federal del derecho de autor"
    return question


def infer_jurisdiction_context(text: str, analysis: dict[str, Any] | None = None) -> str | None:
    probe = normalize_for_match(text or "")
    if analysis:
        probe = probe + " " + normalize_for_match(analysis.get("summary", {}).get("jurisdiction", ""))

    token_map = {
        "cdmx": "ciudad de mexico",
        "ciudad de mexico": "ciudad de mexico",
        "aguascalientes": "aguascalientes",
        "baja california sur": "baja california sur",
        "baja california": "baja california",
        "campeche": "campeche",
        "chiapas": "chiapas",
        "chihuahua": "chihuahua",
        "coahuila": "coahuila",
        "colima": "colima",
        "durango": "durango",
        "guanajuato": "guanajuato",
        "guerrero": "guerrero",
        "hidalgo": "hidalgo",
        "jalisco": "jalisco",
        "edomex": "mexico",
        "estado de mexico": "mexico",
        "michoacan": "michoacan",
        "morelos": "morelos",
        "nayarit": "nayarit",
        "nuevo leon": "nuevo leon",
        "oaxaca": "oaxaca",
        "puebla": "puebla",
        "queretaro": "queretaro",
        "quintana roo": "quintana roo",
        "san luis potosi": "san luis potosi",
        "sinaloa": "sinaloa",
        "sonora": "sonora",
        "tabasco": "tabasco",
        "tamaulipas": "tamaulipas",
        "tlaxcala": "tlaxcala",
        "veracruz": "veracruz",
        "yucatan": "yucatan",
        "zacatecas": "zacatecas",
    }
    for token, jurisdiction in token_map.items():
        if token in probe:
            return jurisdiction
    return None


def normalize_for_match(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text.lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


if __name__ == "__main__":
    ensure_storage()
    app.run(host="127.0.0.1", port=5050, debug=True)
