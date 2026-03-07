from __future__ import annotations

import datetime as dt
import json
import re
import subprocess
import unicodedata
import zipfile
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

DISCLAIMER = (
    "Analisis tecnico automatizado. No constituye asesoria legal definitiva; "
    "requiere validacion de abogado responsable."
)

RISK_PRIORITY = {"low": 1, "medium": 2, "high": 3, "critical": 4}

CLAUSE_KEYWORDS: dict[str, list[str]] = {
    "parties": ["partes", "comparecen", "contratante", "proveedor", "cliente", "entre"],
    "object": ["objeto", "alcance", "servicios", "entregables"],
    "term": ["vigencia", "duracion", "inicio", "termino", "renovacion"],
    "payments": ["pago", "pagos", "factura", "tarifa", "precio", "mora"],
    "termination": ["terminacion", "rescision", "cancelacion", "preaviso", "incumplimiento"],
    "liability": ["responsabilidad", "limitacion", "danos", "perjuicios", "no sera responsable"],
    "indemnification": ["indemnizacion", "indemnizar", "sacar en paz", "defensa"],
    "confidentiality": ["confidencial", "confidencialidad", "secreto", "divulgacion", "nda"],
    "intellectual_property": [
        "propiedad intelectual",
        "derechos de autor",
        "licencia",
        "titularidad",
        "patente",
    ],
    "jurisdiction": ["jurisdiccion", "competencia", "ley aplicable", "tribunales", "fuero"],
    "compliance": [
        "cumplimiento",
        "regulatorio",
        "anticorrupcion",
        "proteccion de datos",
        "lavado de dinero",
    ],
}

CLAUSE_LABELS = {
    "parties": "Partes",
    "object": "Objeto",
    "term": "Vigencia",
    "payments": "Pagos",
    "termination": "Terminacion",
    "liability": "Responsabilidad",
    "indemnification": "Indemnizacion",
    "confidentiality": "Confidencialidad",
    "intellectual_property": "Propiedad intelectual",
    "jurisdiction": "Jurisdiccion",
    "compliance": "Cumplimiento",
}

RECOMMENDATIONS = {
    "parties": "Confirmar nombres legales completos, representante y facultades.",
    "object": "Delimitar alcance, entregables y criterios de aceptacion.",
    "term": "Definir fecha de inicio/termino y reglas de renovacion.",
    "payments": "Aclarar montos, hitos de pago, mora y condiciones de facturacion.",
    "termination": "Equilibrar causales y periodos de preaviso para ambas partes.",
    "liability": "Definir limite de responsabilidad y exclusiones razonables.",
    "indemnification": "Establecer reciprocidad y mecanismo de defensa ante reclamos.",
    "confidentiality": "Incluir alcance, excepciones y vigencia post-terminacion.",
    "intellectual_property": "Definir titularidad y alcance de licencia/cesion.",
    "jurisdiction": "Definir foro competente y ley aplicable.",
    "compliance": "Alinear obligaciones regulatorias y derecho de auditoria.",
}

CRITICAL_MISSING = {"confidentiality", "indemnification"}
HIGH_MISSING = {"liability", "compliance", "jurisdiction"}

CRITICAL_TERMS = [
    "indemnizacion ilimitada",
    "sin limite de responsabilidad",
    "renuncia total",
    "a discrecion exclusiva",
    "sin derecho de defensa",
]

HIGH_TERMS = [
    "no sera responsable",
    "sin previo aviso",
    "incumplimiento esencial",
    "penalizacion excesiva",
    "unilateralmente",
    "a discrecion",
    "terminacion inmediata",
]

MEDIUM_TERMS = [
    "mora",
    "penalizacion",
    "interes moratorio",
    "suspension",
    "unilateral",
    "renuncia",
    "sin garantia",
]

QUESTION_TO_CLAUSE = {
    "parte": "parties",
    "objeto": "object",
    "vigencia": "term",
    "duracion": "term",
    "pago": "payments",
    "factura": "payments",
    "terminacion": "termination",
    "rescision": "termination",
    "responsabilidad": "liability",
    "indemnizacion": "indemnification",
    "confidencial": "confidentiality",
    "propiedad intelectual": "intellectual_property",
    "jurisdiccion": "jurisdiction",
    "ley aplicable": "jurisdiction",
    "foro": "jurisdiction",
    "tribunal": "jurisdiction",
    "competencia": "jurisdiction",
    "cumplimiento": "compliance",
    "riesgo": "compliance",
}


def parse_document_text(file_path: Path, source_type: str) -> str:
    source = source_type.lower()
    if source == "txt":
        return _read_text_file(file_path)
    if source == "docx":
        return _read_docx(file_path)
    if source == "pdf":
        return _read_pdf(file_path)
    raise ValueError(f"Formato no soportado: {source_type}")


def _read_text_file(file_path: Path) -> str:
    for encoding in ("utf-8", "latin-1"):
        try:
            return file_path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return file_path.read_text(errors="ignore")


def _read_docx(file_path: Path) -> str:
    paragraphs: list[str] = []
    with zipfile.ZipFile(file_path) as archive:
        xml_bytes = archive.read("word/document.xml")
    root = ET.fromstring(xml_bytes)
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    for paragraph in root.findall(".//w:p", ns):
        runs = []
        for node in paragraph.findall(".//w:t", ns):
            if node.text:
                runs.append(node.text)
        text = " ".join(runs).strip()
        if text:
            paragraphs.append(text)
    return "\n".join(paragraphs)


def _read_pdf(file_path: Path) -> str:
    # Fallback sin dependencias externas: intenta recuperar texto con `strings`.
    result = subprocess.run(
        ["strings", "-n", "6", str(file_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    lines = []
    for line in result.stdout.splitlines():
        cleaned = normalize_text(line)
        if len(cleaned) < 8:
            continue
        if re.search(r"[A-Za-z]", cleaned):
            lines.append(cleaned)
    return "\n".join(lines)


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def split_paragraphs(raw_text: str) -> list[dict[str, str]]:
    lines = [line.strip() for line in raw_text.splitlines()]
    sections: list[dict[str, str]] = []
    current_section = "Texto general"
    buffer: list[str] = []

    def flush() -> None:
        if not buffer:
            return
        paragraph_text = normalize_text(" ".join(buffer))
        if paragraph_text:
            sections.append({"section": current_section, "text": paragraph_text})
        buffer.clear()

    for line in lines:
        if not line:
            flush()
            continue
        if is_heading(line):
            flush()
            current_section = line[:120]
            continue
        buffer.append(line)
    flush()

    if not sections:
        compact = normalize_text(raw_text)
        if compact:
            sections.append({"section": "Texto general", "text": compact})
    return sections


def is_heading(line: str) -> bool:
    compact = normalize_text(line)
    if not compact:
        return False
    if len(compact) > 90:
        return False
    upper_ratio = sum(ch.isupper() for ch in compact if ch.isalpha()) / max(
        1, sum(ch.isalpha() for ch in compact)
    )
    if upper_ratio > 0.7:
        return True
    return bool(
        re.match(
            r"^(clausula|clausula\s+\w+|seccion|capitulo|articulo)\b",
            compact.lower(),
        )
    )


def analyze_contract(
    *,
    document_id: str,
    document_name: str,
    source_type: str,
    repository_path: str,
    raw_text: str,
) -> dict[str, Any]:
    paragraphs = split_paragraphs(raw_text)
    clauses = extract_clauses(paragraphs, document_id, document_name)
    summary = build_summary(paragraphs, clauses)
    overall_risk = build_overall_risk(clauses)
    compliance_checks = build_compliance_checks(clauses)
    requires_human = overall_risk["level"] in {"high", "critical"}

    now = dt.datetime.now(dt.timezone.utc).isoformat()
    analysis = {
        "analysis_id": f"analysis_{document_id}",
        "document": {
            "document_id": document_id,
            "document_name": document_name,
            "source_type": source_type,
            "repository_path": repository_path,
            "ingestion_timestamp": now,
        },
        "summary": summary,
        "clauses": clauses,
        "overall_risk": overall_risk,
        "compliance_checks": compliance_checks,
        "human_review": {
            "required": requires_human,
            "reason": "Existe riesgo high/critical." if requires_human else "No obligatorio.",
            "review_status": "pending" if requires_human else "not_required",
        },
        "generated_at": now,
        "disclaimer": DISCLAIMER,
    }
    analysis = attach_executive_summary_llm_first(analysis)
    return analysis


def attach_executive_summary_llm_first(analysis: dict[str, Any]) -> dict[str, Any]:
    summary_text = None
    summary_meta: dict[str, Any] = {}

    try:
        from llm_bridge import generate_executive_summary_with_gemini

        summary_text, summary_meta = generate_executive_summary_with_gemini(analysis)
    except Exception:
        summary_text, summary_meta = None, {"used": False, "error": "summary_generation_failed"}

    summary_block = analysis.setdefault("summary", {})
    if summary_text:
        summary_block["executive"] = summary_text
        summary_block["source"] = "llm"
        summary_block["llm_meta"] = summary_meta
        analysis["executive_summary"] = summary_text
    else:
        fallback = (
            "No se pudo generar el resumen ejecutivo en este intento. "
            "Puedo reintentar de inmediato con más contexto contractual."
        )
        summary_block["executive"] = fallback
        summary_block["source"] = "fallback_minimal"
        summary_block["llm_meta"] = summary_meta
        analysis["executive_summary"] = fallback
    return analysis


def extract_clauses(
    paragraphs: list[dict[str, str]],
    document_id: str,
    document_name: str,
) -> list[dict[str, Any]]:
    clauses: list[dict[str, Any]] = []
    for clause_type, keywords in CLAUSE_KEYWORDS.items():
        matches = rank_matches(paragraphs, keywords)
        if matches:
            status = "found" if len(matches) > 1 else "partial"
            extracted_text = " ".join(item["text"] for item in matches[:2])
            evidence = []
            for item in matches[:3]:
                evidence.append(
                    {
                        "document_id": document_id,
                        "document_name": document_name,
                        "section": item["section"],
                        "snippet": item["text"][:500],
                    }
                )
        else:
            status = "not_found"
            extracted_text = ""
            fallback = paragraphs[0] if paragraphs else {"section": "Texto general", "text": ""}
            evidence = [
                {
                    "document_id": document_id,
                    "document_name": document_name,
                    "section": fallback["section"],
                    "snippet": fallback["text"][:300] or "Sin evidencia textual disponible.",
                }
            ]

        risk = assess_clause_risk(clause_type, status, extracted_text)
        clauses.append(
            {
                "clause_type": clause_type,
                "clause_label": CLAUSE_LABELS[clause_type],
                "status": status,
                "extracted_text": extracted_text,
                "normalized_data": {},
                "evidence": evidence,
                "risk": risk,
            }
        )
    return clauses


def rank_matches(paragraphs: list[dict[str, str]], keywords: list[str]) -> list[dict[str, str]]:
    scored: list[tuple[int, dict[str, str]]] = []
    for item in paragraphs:
        segments = segment_text_for_matching(item["text"])
        for segment in segments:
            text_lc = segment.lower()
            score = sum(1 for kw in keywords if kw in text_lc)
            if score > 0:
                scored.append((score, {"section": item["section"], "text": segment}))

    scored.sort(key=lambda pair: pair[0], reverse=True)

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for _, item in scored:
        key = normalize_text(item["text"])[:420].lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def segment_text_for_matching(text: str, max_segment_len: int = 420) -> list[str]:
    compact = normalize_text(text)
    if not compact:
        return []

    if len(compact) <= max_segment_len:
        return [compact]

    raw_sentences = re.split(r"(?<=[\.;:])\s+", compact)
    sentences = [normalize_text(sentence) for sentence in raw_sentences if normalize_text(sentence)]
    if not sentences:
        return [compact[:max_segment_len]]

    segments: list[str] = []
    buffer = ""
    for sentence in sentences:
        if len(sentence) > max_segment_len:
            if buffer:
                segments.append(buffer)
                buffer = ""
            for i in range(0, len(sentence), max_segment_len):
                piece = sentence[i : i + max_segment_len]
                if piece:
                    segments.append(piece)
            continue

        if not buffer:
            buffer = sentence
            continue

        candidate = f"{buffer} {sentence}"
        if len(candidate) <= max_segment_len:
            buffer = candidate
        else:
            segments.append(buffer)
            buffer = sentence

    if buffer:
        segments.append(buffer)

    # Ventana deslizante ligera para preservar contexto local entre segmentos.
    if len(segments) > 1:
        with_context = list(segments)
        for idx in range(len(segments) - 1):
            joined = f"{segments[idx]} {segments[idx + 1]}"
            with_context.append(joined[:max_segment_len])
        segments = with_context

    return segments[:140]


def assess_clause_risk(clause_type: str, status: str, extracted_text: str) -> dict[str, Any]:
    text_lc = extracted_text.lower()

    if status == "not_found":
        if clause_type in CRITICAL_MISSING:
            return risk_payload(
                "critical",
                82,
                "No se encontro la clausula y es critica para control de contingencias.",
                "Alta exposicion legal/economica por vacio contractual.",
                RECOMMENDATIONS[clause_type],
                0.88,
            )
        if clause_type in HIGH_MISSING:
            return risk_payload(
                "high",
                68,
                "No se encontro la clausula y puede generar incertidumbre material.",
                "Riesgo operativo y de exigibilidad contractual.",
                RECOMMENDATIONS[clause_type],
                0.81,
            )
        return risk_payload(
            "medium",
            42,
            "No se encontro evidencia suficiente de la clausula.",
            "Riesgo por informacion incompleta.",
            RECOMMENDATIONS[clause_type],
            0.7,
        )

    if any(term in text_lc for term in CRITICAL_TERMS):
        return risk_payload(
            "critical",
            85,
            "La redaccion contiene terminos de alto impacto contractual.",
            "Exposicion severa por obligaciones desbalanceadas o ilimitadas.",
            RECOMMENDATIONS[clause_type],
            0.86,
        )

    high_hits = sum(1 for term in HIGH_TERMS if term in text_lc)
    medium_hits = sum(1 for term in MEDIUM_TERMS if term in text_lc)

    if high_hits >= 1:
        return risk_payload(
            "high",
            66,
            "Se detectaron terminos con potencial de desequilibrio contractual.",
            "Posibles controversias y contingencias economicas.",
            RECOMMENDATIONS[clause_type],
            0.79,
        )

    if medium_hits >= 2 or status == "partial":
        return risk_payload(
            "medium",
            46,
            "Se identificaron senales moderadas que requieren revision legal.",
            "Riesgo medio por ambiguedad o condiciones no del todo equilibradas.",
            RECOMMENDATIONS[clause_type],
            0.72,
        )

    return risk_payload(
        "low",
        18,
        "Clausula localizada sin senales fuertes de riesgo inmediato.",
        "Riesgo acotado sujeto a revision juridica final.",
        RECOMMENDATIONS[clause_type],
        0.74,
    )


def risk_payload(
    level: str,
    score: int,
    rationale: str,
    probable_impact: str,
    recommendation_initial: str,
    confidence: float,
) -> dict[str, Any]:
    return {
        "level": level,
        "score": score,
        "rationale": rationale,
        "probable_impact": probable_impact,
        "recommendation_initial": recommendation_initial,
        "confidence": round(confidence, 2),
    }


def build_summary(paragraphs: list[dict[str, str]], clauses: list[dict[str, Any]]) -> dict[str, Any]:
    full_text = " ".join(item["text"] for item in paragraphs)
    full_lc = full_text.lower()

    parties = extract_parties(paragraphs)
    object_text = extract_first_match(paragraphs, ["objeto", "alcance", "servicio"])
    term_clause = find_clause(clauses, "term")
    jurisdiction_clause = find_clause(clauses, "jurisdiction")

    governing_law = extract_first_phrase(
        jurisdiction_clause.get("extracted_text", ""),
        ["ley aplicable", "legislacion", "codigo civil"],
    )
    if not governing_law and "ley aplicable" in full_lc:
        governing_law = "Ley aplicable identificada en el documento (validar detalle)."

    jurisdiction = extract_first_phrase(
        jurisdiction_clause.get("extracted_text", ""),
        ["jurisdiccion", "tribunales", "fuero"],
    )
    if not jurisdiction and jurisdiction_clause.get("status") == "found":
        jurisdiction = "Jurisdiccion mencionada (validar texto completo)."

    return {
        "parties": parties if parties else ["No identificado"],
        "object": object_text or "No identificado",
        "term": {
            "status": term_clause.get("status", "not_found"),
            "duration_text": term_clause.get("extracted_text", "")[:180],
        },
        "governing_law": governing_law or "No identificado",
        "jurisdiction": jurisdiction or "No identificado",
    }


def extract_parties(paragraphs: list[dict[str, str]]) -> list[str]:
    candidates: list[str] = []
    for paragraph in paragraphs[:25]:
        text = paragraph["text"]
        text_lc = text.lower()
        if "entre" in text_lc and " y " in text_lc:
            simplified = normalize_text(text)
            candidates.append(simplified[:180])
            if len(candidates) >= 2:
                break
    return candidates


def extract_first_match(paragraphs: list[dict[str, str]], keywords: list[str]) -> str:
    for paragraph in paragraphs:
        text_lc = paragraph["text"].lower()
        if any(keyword in text_lc for keyword in keywords):
            return paragraph["text"][:220]
    return ""


def extract_first_phrase(text: str, keywords: list[str]) -> str:
    if not text:
        return ""
    sentences = re.split(r"(?<=[\.;])\s+", text)
    for sentence in sentences:
        sentence_lc = sentence.lower()
        if any(keyword in sentence_lc for keyword in keywords):
            return sentence[:180]
    return ""


def find_clause(clauses: list[dict[str, Any]], clause_type: str) -> dict[str, Any]:
    for clause in clauses:
        if clause["clause_type"] == clause_type:
            return clause
    return {}


def build_overall_risk(clauses: list[dict[str, Any]]) -> dict[str, Any]:
    if not clauses:
        return {"level": "medium", "score": 50, "critical_flags": ["Sin clausulas evaluadas."]}

    max_level = "low"
    max_score = 0
    total_score = 0
    flags: list[str] = []

    for clause in clauses:
        risk = clause["risk"]
        total_score += int(risk.get("score", 0))
        if int(risk.get("score", 0)) > max_score:
            max_score = int(risk.get("score", 0))
        if RISK_PRIORITY[risk["level"]] > RISK_PRIORITY[max_level]:
            max_level = risk["level"]
        if risk["level"] in {"high", "critical"}:
            flags.append(f"{CLAUSE_LABELS[clause['clause_type']]}: {risk['rationale']}")

    avg_score = round(total_score / len(clauses), 2)
    overall_score = max(avg_score, float(max_score))
    return {"level": max_level, "score": overall_score, "critical_flags": flags[:8]}


def build_compliance_checks(clauses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks = []
    for check_id, clause_type, label in [
        ("CHK_CONF", "confidentiality", "Confidencialidad presente"),
        ("CHK_JUR", "jurisdiction", "Jurisdiccion y ley aplicable"),
        ("CHK_COMP", "compliance", "Clausula de cumplimiento"),
    ]:
        clause = find_clause(clauses, clause_type)
        status = "pass" if clause.get("status") == "found" else "warn"
        message = "Evidencia localizada." if status == "pass" else "Requiere validacion manual."
        checks.append(
            {
                "check_id": check_id,
                "status": status,
                "message": f"{label}: {message}",
                "evidence": clause.get("evidence", [])[:1],
            }
        )
    return checks


def answer_question(analysis: dict[str, Any], question: str) -> dict[str, Any]:
    question_lc = question.lower().strip()
    question_norm = normalize_for_match(question_lc)
    clauses = analysis.get("clauses", [])
    if is_law_applicable_question(question_norm):
        return answer_law_applicable_question(analysis, clauses)

    selected_types = infer_clause_targets(question_norm)

    selected = [c for c in clauses if c["clause_type"] in selected_types] if selected_types else []
    if not selected:
        selected = rank_clauses_by_overlap(clauses, question_lc)[:2]

    usable = [cl for cl in selected if cl.get("status") in {"found", "partial"} and cl.get("extracted_text")]
    if not usable:
        return {
            "answer": (
                "No encuentro evidencia suficiente en este contrato para responder con precision. "
                "Si quieres, indícame una clausula concreta y la reviso de inmediato."
            ),
            "confidence": 0.45,
            "risk_estimate": {
                "level": "medium",
                "impacto_probable": "Analisis incompleto por falta de evidencia textual.",
                "recomendacion_inicial": "Solicitar documento completo o anexos faltantes.",
            },
            "citations": collect_citations(selected, max_items=2),
            "missing_evidence": True,
            "human_review_required": True,
            "next_actions": suggest_next_actions(question_norm),
            "disclaimer": DISCLAIMER,
        }

    points = []
    highest = usable[0]["risk"]
    for clause in usable[:3]:
        snippet = normalize_text(clause["extracted_text"])[:160]
        points.append((CLAUSE_LABELS[clause["clause_type"]], snippet))
        if RISK_PRIORITY[clause["risk"]["level"]] > RISK_PRIORITY[highest["level"]]:
            highest = clause["risk"]

    answer_text = compose_natural_answer(question_norm, points)
    citations = collect_citations(usable, max_items=4)

    return {
        "answer": answer_text,
        "confidence": min(0.93, round(0.62 + 0.08 * len(citations), 2)),
        "risk_estimate": {
            "level": highest["level"],
            "impacto_probable": highest["probable_impact"],
            "recomendacion_inicial": highest["recommendation_initial"],
        },
        "citations": citations,
        "missing_evidence": False,
        "human_review_required": highest["level"] in {"high", "critical"},
        "next_actions": suggest_next_actions(question_norm),
        "disclaimer": DISCLAIMER,
    }


def compose_natural_answer(question_lc: str, points: list[tuple[str, str]]) -> str:
    if not points:
        return "No encuentro evidencia suficiente en los documentos disponibles."

    if len(points) == 1:
        label, text = points[0]
        return (
            f"Revisando la clausula de {label.lower()}, se observa: {text}. "
            "Si te sirve, puedo proponer una redaccion alternativa para negociacion."
        )

    risk_like = any(token in question_lc for token in ["riesgo", "incumpl", "duda"])
    dictamen_like = any(token in question_lc for token in ["dictamen", "dictaminar", "vo bo", "vobo"])
    if dictamen_like:
        intro = "Para dictamen preliminar, esto es lo mas relevante del contrato:"
    elif risk_like:
        intro = "En terminos de riesgo contractual, los puntos clave son:"
    else:
        intro = "Con base en el texto contractual, esto responde mejor tu consulta:"
    segments = [f"En {label.lower()}, se observa: {text}." for label, text in points]
    ending = " Si quieres, lo convierto en acciones concretas de revision, duda o Vo.Bo."
    return intro + " " + " ".join(segments) + ending


def suggest_next_actions(question_lc: str) -> list[str]:
    actions: list[str] = []
    if any(token in question_lc for token in ["dictamen", "dictaminar"]):
        actions.append("Puedo generar dictamen preliminar con recomendaciones negociables.")
    if any(token in question_lc for token in ["riesgo", "indemn", "responsabilidad", "termina", "pago"]):
        actions.append("Puedo priorizar riesgos high/critical y su impacto economico.")
    if not actions:
        actions.append("Puedo resumir incumplimientos, dudas y Vo.Bo. del contrato.")
    actions.append("Si el riesgo es alto/critical, valida con abogado responsable antes de decidir.")
    return actions[:3]


def is_law_applicable_question(question_lc: str) -> bool:
    if "ley" in question_lc and ("aplicable" in question_lc or "aplica" in question_lc):
        return True
    intents = ["ley aplicable", "jurisdic", "foro", "tribunal", "competencia", "fuero", "marco normativo"]
    return any(token in question_lc for token in intents)


def answer_law_applicable_question(analysis: dict[str, Any], clauses: list[dict[str, Any]]) -> dict[str, Any]:
    summary = analysis.get("summary", {})
    governing_law = summary.get("governing_law", "No identificado")
    jurisdiction = summary.get("jurisdiction", "No identificado")
    jurisdiction_clause = find_clause(clauses, "jurisdiction")
    citations = collect_citations([jurisdiction_clause] if jurisdiction_clause else clauses[:1], max_items=4)

    grounded_laws: list[str] = []
    for item in analysis.get("legal_grounding", []):
        if item.get("clause_type") != "jurisdiction":
            continue
        for ref in item.get("references", []):
            law_name = normalize_text(ref.get("law_name", ""))
            if not law_name:
                continue
            law_name_norm = normalize_for_match(law_name)
            if "marco legal" in law_name_norm:
                continue
            if law_name not in grounded_laws:
                grounded_laws.append(law_name)

    normalized_gov_law = normalize_for_match(governing_law)
    gov_law_is_placeholder = (
        not governing_law
        or normalized_gov_law == "no identificado"
        or "validar detalle" in normalized_gov_law
        or "identificada en el documento" in normalized_gov_law
    )

    if gov_law_is_placeholder and grounded_laws:
        governing_law = ", ".join(grounded_laws[:3])

    jurisdiction_norm = normalize_for_match(jurisdiction)
    if "federal" in jurisdiction_norm and "ciudad de mexico" in jurisdiction_norm:
        jurisdiction = "Federal + Estatal (Ciudad de Mexico)"

    missing = (not governing_law or governing_law == "No identificado") and jurisdiction == "No identificado"
    if missing:
        answer = (
            "No encuentro evidencia clara de ley aplicable y jurisdiccion en el texto recuperado. "
            "Conviene validar esta clausula con el contrato completo o anexos."
        )
        risk_level = "medium"
        confidence = 0.55
    else:
        answer = (
            f"En el contrato, la ley aplicable identificada es: {governing_law}. "
            f"La jurisdiccion indicada es: {jurisdiction}. "
            "Si quieres, reviso si esa redaccion es equilibrada para ambas partes."
        )
        risk_level = "low"
        confidence = 0.86

    return {
        "answer": answer,
        "confidence": confidence,
        "risk_estimate": {
            "level": risk_level,
            "impacto_probable": "Definir ley y foro evita incertidumbre en caso de controversia.",
            "recomendacion_inicial": "Confirmar que ley aplicable y foro coincidan con la estrategia legal del despacho.",
        },
        "citations": citations,
        "missing_evidence": missing,
        "human_review_required": True,
        "next_actions": [
            "Puedo evaluar si la clausula de ley aplicable es negociable.",
            "Puedo comparar esta clausula contra el estandar del despacho.",
        ],
        "disclaimer": DISCLAIMER,
    }


def infer_clause_targets(question_lc: str) -> list[str]:
    targets = []
    for keyword, clause_type in QUESTION_TO_CLAUSE.items():
        if keyword in question_lc and clause_type not in targets:
            targets.append(clause_type)
    return targets


def normalize_for_match(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text)
    without_marks = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return without_marks.lower()


def rank_clauses_by_overlap(clauses: list[dict[str, Any]], question_lc: str) -> list[dict[str, Any]]:
    tokens = [tok for tok in re.split(r"\W+", question_lc) if len(tok) > 2]
    scored: list[tuple[int, dict[str, Any]]] = []
    for clause in clauses:
        text = (
            (clause.get("extracted_text") or "")
            + " "
            + " ".join(ev.get("snippet", "") for ev in clause.get("evidence", []))
        ).lower()
        score = sum(1 for token in tokens if token in text)
        scored.append((score, clause))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in scored if item[0] > 0] or clauses[:2]


def collect_citations(clauses: list[dict[str, Any]], max_items: int = 4) -> list[dict[str, Any]]:
    citations: list[dict[str, Any]] = []
    for clause in clauses:
        for ev in clause.get("evidence", []):
            citations.append(
                {
                    "document_id": ev.get("document_id"),
                    "document_name": ev.get("document_name"),
                    "section": ev.get("section", "No identificada"),
                    "page": ev.get("page"),
                    "snippet": ev.get("snippet", ""),
                }
            )
            if len(citations) >= max_items:
                return citations
    return citations


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
