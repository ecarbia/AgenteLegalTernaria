from __future__ import annotations

import datetime as dt
import json
import os
import re
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

RISK_LEVELS = {"low", "medium", "high", "critical"}
RISK_PRIORITY = {"low": 1, "medium": 2, "high": 3, "critical": 4}
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
}
SPANISH_STOPWORDS = {
    "que",
    "como",
    "cual",
    "cuales",
    "cuando",
    "donde",
    "quien",
    "quienes",
    "el",
    "la",
    "los",
    "las",
    "de",
    "del",
    "en",
    "y",
    "o",
    "es",
    "son",
    "un",
    "una",
    "por",
    "para",
    "sobre",
    "me",
    "puedes",
    "podrias",
    "favor",
    "contrato",
}
REQUIRED_DIALOG_KEYS = {
    "answer",
    "confidence",
    "risk_estimate",
    "missing_evidence",
    "human_review_required",
}


def normalize_for_match(text: str) -> str:
    normalized = unicodedata.normalize("NFD", (text or "").lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _trim_on_sentence_boundary(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    candidate = text[:max_len]
    # intenta cortar en final de oración
    last_dot = max(candidate.rfind(". "), candidate.rfind("! "), candidate.rfind("? "))
    if last_dot >= int(max_len * 0.6):
        return candidate[: last_dot + 1].rstrip()
    # fallback: cortar por espacio para no romper palabra
    last_space = candidate.rfind(" ")
    if last_space >= int(max_len * 0.6):
        return candidate[:last_space].rstrip() + "…"
    return candidate.rstrip() + "…"


def _clean_llm_text(raw: str, *, max_len: int = 2200) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    if text.startswith("{") and '"answer"' in text:
        parsed = _extract_json_from_text(text)
        candidate = (parsed.get("answer") or "").strip() if isinstance(parsed, dict) else ""
        if candidate:
            text = candidate
    text = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE)
    text = text.replace("```", "")
    text = re.sub(r"[\x00-\x1F\x7F]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = _trim_on_sentence_boundary(text, max_len=max_len)
    return text


def _default_risk_from_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    overall = analysis.get("overall_risk", {}) if isinstance(analysis, dict) else {}
    level = normalize_for_match(overall.get("level", "medium"))
    if level not in RISK_LEVELS:
        level = "medium"
    return {
        "level": level,
        "impacto_probable": "Validar en revision humana con soporte normativo.",
        "recomendacion_inicial": "Escalar a abogado responsable para cierre de dictamen.",
    }


def _summary_looks_weak(text: str) -> bool:
    clean = _clean_llm_text(text, max_len=1800)
    if len(clean) < 140:
        return True
    probe = normalize_for_match(clean)

    # señales de texto plantilla o concatenación mecánica
    weak_patterns = [
        "en vigencia/plazo se observa",
        "en pagos destaca",
        "en jurisdiccion/ley aplicable se identifica",
        "el alcance principal identificado es",
        "no identificado",
    ]
    if any(p in probe for p in weak_patterns):
        return True

    # excesiva fragmentación por dos puntos -> síntoma de pegado
    if clean.count(":") >= 4 and len(clean) < 450:
        return True

    return False


def _normalize_next_action(text: str) -> str:
    action = re.sub(r"\s+", " ", str(text or "")).strip()
    action = re.sub(r"^(puedes\s+pedir|pide)\s*:?\s*", "", action, flags=re.IGNORECASE)
    action = action.strip(" '\"")
    return action[:170]


def _clamp_score(value: Any) -> int:
    try:
        parsed = int(round(float(value)))
    except (TypeError, ValueError):
        return 50
    return max(0, min(100, parsed))


def _clamp_confidence(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.65
    return round(max(0.0, min(1.0, parsed)), 2)


def _extract_json_from_text(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    code_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    if code_match:
        candidate = code_match.group(1).strip()
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        candidate = raw[start : end + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    return {}


class GeminiClient:
    def __init__(self, api_key: str, model: str):
        self.api_key = api_key.strip()
        self.model = model.strip() or "gemini-1.5-flash"

    @classmethod
    def from_env(cls) -> "GeminiClient":
        return cls(
            api_key=os.getenv("GEMINI_API_KEY", ""),
            model=os.getenv("GEMINI_MODEL", "gemini-1.5-flash"),
        )

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    def _endpoint(self) -> str:
        key = urllib.parse.quote_plus(self.api_key)
        return f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={key}"

    def generate_text(
        self,
        *,
        prompt: str,
        temperature: float = 0.15,
        max_output_tokens: int = 1800,
    ) -> str:
        if not self.configured:
            raise RuntimeError("GEMINI_API_KEY no configurada.")

        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": temperature,
                "topP": 0.9,
                "maxOutputTokens": max_output_tokens,
            },
        }
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._endpoint(),
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=50) as resp:
                response_payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Gemini HTTP {exc.code}: {detail[:250]}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"No se pudo conectar con Gemini: {exc}") from exc

        candidates = response_payload.get("candidates", [])
        if not candidates:
            raise RuntimeError("Gemini no devolvio candidatos.")
        parts = (candidates[0].get("content") or {}).get("parts", [])
        text = "\n".join(part.get("text", "") for part in parts if isinstance(part, dict)).strip()
        if not text:
            raise RuntimeError("Gemini devolvio respuesta vacia.")
        return text

    def generate_json(
        self,
        *,
        prompt: str,
        temperature: float = 0.1,
        max_output_tokens: int = 1900,
    ) -> tuple[dict[str, Any], str]:
        raw = self.generate_text(
            prompt=prompt,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
        )
        parsed = _extract_json_from_text(raw)
        return parsed, raw


def get_llm_status() -> dict[str, Any]:
    client = GeminiClient.from_env()
    return {
        "provider": "gemini",
        "configured": client.configured,
        "model": client.model,
        "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def _compact_analysis_for_llm(analysis: dict[str, Any]) -> dict[str, Any]:
    clauses_payload = []
    for clause in analysis.get("clauses", [])[:12]:
        evidence = clause.get("evidence", [])[:2]
        evidence_payload = [
            {
                "section": item.get("section", "No identificada"),
                "snippet": (item.get("snippet") or "")[:240],
            }
            for item in evidence
        ]
        clauses_payload.append(
            {
                "clause_type": clause.get("clause_type"),
                "clause_label": clause.get("clause_label"),
                "status": clause.get("status"),
                "extracted_text": (clause.get("extracted_text") or "")[:560],
                "current_risk": clause.get("risk", {}),
                "evidence": evidence_payload,
            }
        )

    legal_payload = []
    for item in analysis.get("legal_grounding", [])[:8]:
        refs = []
        for ref in (item.get("references") or [])[:2]:
            refs.append(
                {
                    "law_name": ref.get("law_name"),
                    "article_label": ref.get("article_label"),
                    "snippet": (ref.get("snippet") or "")[:170],
                    "scope": ref.get("scope"),
                    "jurisdiction": ref.get("jurisdiction"),
                }
            )
        legal_payload.append({"clause_type": item.get("clause_type"), "references": refs})

    return {
        "summary": analysis.get("summary", {}),
        "overall_risk": analysis.get("overall_risk", {}),
        "clauses": clauses_payload,
        "legal_grounding": legal_payload,
    }


def _compute_overall_from_clauses(clauses: list[dict[str, Any]]) -> dict[str, Any]:
    if not clauses:
        return {"level": "medium", "score": 50, "critical_flags": ["Sin clausulas evaluadas."]}

    highest = "low"
    max_score = 0
    total = 0
    flags = []
    for clause in clauses:
        risk = clause.get("risk", {})
        level = normalize_for_match(risk.get("level", ""))
        if level not in RISK_LEVELS:
            level = "medium"
        score = _clamp_score(risk.get("score", 50))
        total += score
        max_score = max(max_score, score)
        if RISK_PRIORITY[level] > RISK_PRIORITY[highest]:
            highest = level
        if level in {"high", "critical"}:
            flags.append(
                f"{clause.get('clause_label') or clause.get('clause_type')}: "
                f"{risk.get('rationale', 'Revisar en detalle.')}"
            )
    avg = round(total / max(1, len(clauses)), 2)
    return {"level": highest, "score": max(avg, float(max_score)), "critical_flags": flags[:10]}


def enrich_analysis_with_gemini(analysis: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    client = GeminiClient.from_env()
    meta = {
        "provider": "gemini",
        "configured": client.configured,
        "model": client.model,
        "used": False,
        "error": None,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    if not client.configured:
        return analysis, meta

    payload = _compact_analysis_for_llm(analysis)
    prompt = (
        "Eres un analista LegalOps para contratos en Mexico. "
        "Evalua riesgos de cada clausula con base en la evidencia proporcionada. "
        "No des asesoria legal definitiva. Si falta evidencia, dilo claramente. "
        "Devuelve SOLO JSON valido con esta estructura exacta:\n"
        "{\n"
        '  "overall": {"level":"low|medium|high|critical","score":0-100,"rationale":"texto"},\n'
        '  "clauses":[{"clause_type":"texto","level":"low|medium|high|critical","score":0-100,'
        '"rationale":"texto","probable_impact":"texto","recommendation_initial":"texto","confidence":0-1}],\n'
        '  "hallazgos":{"incumplimientos":["texto"],"dudas":["texto"],"vobo":["texto"]}\n'
        "}\n"
        f"ANALISIS_BASE:\n{json.dumps(payload, ensure_ascii=False)}"
    )

    try:
        parsed, _ = client.generate_json(prompt=prompt, temperature=0.1, max_output_tokens=2000)
    except Exception as exc:  # pragma: no cover - operativo
        meta["error"] = str(exc)
        return analysis, meta

    clause_updates = {
        (item.get("clause_type") or "").strip(): item
        for item in parsed.get("clauses", [])
        if isinstance(item, dict)
    }
    for clause in analysis.get("clauses", []):
        clause_type = clause.get("clause_type", "")
        update = clause_updates.get(clause_type)
        if not update:
            continue
        level = normalize_for_match(update.get("level", ""))
        if level not in RISK_LEVELS:
            continue
        clause["risk"]["level"] = level
        clause["risk"]["score"] = _clamp_score(update.get("score", clause["risk"].get("score", 50)))
        clause["risk"]["rationale"] = (update.get("rationale") or clause["risk"].get("rationale", "")).strip()
        clause["risk"]["probable_impact"] = (
            update.get("probable_impact") or clause["risk"].get("probable_impact", "")
        ).strip()
        clause["risk"]["recommendation_initial"] = (
            update.get("recommendation_initial") or clause["risk"].get("recommendation_initial", "")
        ).strip()
        clause["risk"]["confidence"] = _clamp_confidence(update.get("confidence", clause["risk"].get("confidence", 0.7)))

    computed = _compute_overall_from_clauses(analysis.get("clauses", []))
    overall = parsed.get("overall", {})
    overall_level = normalize_for_match(overall.get("level", ""))
    overall_score = _clamp_score(overall.get("score", computed.get("score", 50)))
    rationale = (overall.get("rationale") or "").strip()

    analysis["overall_risk"] = {
        "level": overall_level if overall_level in RISK_LEVELS else computed.get("level", "medium"),
        "score": max(float(computed.get("score", 50)), float(overall_score)),
        "critical_flags": computed.get("critical_flags", []),
        "llm_rationale": rationale,
    }

    hallazgos = parsed.get("hallazgos", {})
    analysis["llm_hallazgos"] = {
        "incumplimientos": hallazgos.get("incumplimientos", [])[:12],
        "dudas": hallazgos.get("dudas", [])[:12],
        "vobo": hallazgos.get("vobo", [])[:12],
    }

    meta["used"] = True
    return analysis, meta


def generate_executive_summary_with_gemini(analysis: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    client = GeminiClient.from_env()
    meta = {
        "provider": "gemini",
        "configured": client.configured,
        "model": client.model,
        "used": False,
        "error": None,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "agent": "executive_summary",
    }
    if not client.configured:
        return None, meta

    clause_map = {cl.get("clause_type"): cl for cl in analysis.get("clauses", []) if isinstance(cl, dict)}
    high_risk_clauses = []
    for clause in analysis.get("clauses", []):
        if not isinstance(clause, dict):
            continue
        risk_level = normalize_for_match((clause.get("risk") or {}).get("level", ""))
        if risk_level not in {"high", "critical"}:
            continue
        high_risk_clauses.append(
            {
                "clause_label": clause.get("clause_label") or clause.get("clause_type"),
                "risk_level": risk_level,
                "rationale": ((clause.get("risk") or {}).get("rationale") or "")[:180],
            }
        )

    focused_context = {
        "contract_context": analysis.get("contract_context", {}),
        "overall_risk": analysis.get("overall_risk", {}),
        "summary_hints": analysis.get("summary", {}),
        "parties_text": (clause_map.get("parties") or {}).get("extracted_text", "")[:2000],
        "object_text": (clause_map.get("object") or {}).get("extracted_text", "")[:2400],
        "term_text": (clause_map.get("term") or {}).get("extracted_text", "")[:1600],
        "payments_text": (clause_map.get("payments") or {}).get("extracted_text", "")[:1600],
        "jurisdiction_text": (clause_map.get("jurisdiction") or {}).get("extracted_text", "")[:1600],
        "high_risk_clauses": high_risk_clauses[:8],
    }

    prompt = (
        "Eres un abogado senior LegalOps en Mexico. "
        "Redacta un resumen ejecutivo contractual fluido, natural y profesional para socio de despacho. "
        "Debe sonar humano, no plantilla. "
        "Longitud: 160 a 260 palabras. "
        "Estructura narrativa: contexto, obligaciones clave, riesgos concretos, recomendacion accionable. "
        "NO uses encabezados literales tipo 'En vigencia/plazo se observa'. "
        "NO pegues snippets crudos; parafrasea con precision juridica. "
        "Si falta evidencia relevante, menciónalo brevemente sin romper el flujo. "
        "No inventes hechos. Sin bullets, sin markdown, sin JSON.\n"
        f"CONTEXTO:\n{json.dumps(focused_context, ensure_ascii=False)}"
    )
    try:
        raw = client.generate_text(prompt=prompt, temperature=0.22, max_output_tokens=650)
    except Exception as exc:  # pragma: no cover - operativo
        meta["error"] = str(exc)
        return None, meta

    summary = _clean_llm_text(raw, max_len=1200)
    retries = 0
    if _summary_looks_weak(summary):
        retries += 1
        retry_prompt = (
            "Reescribe el resumen ejecutivo de forma clara y util para un abogado que decide rapido. "
            "Evita copiar encabezados literales del contrato. "
            "No uses frases plantilla como 'Sobre vigencia/plazo, se observa'. "
            "No inventes informacion.\n"
            f"TEXTO_A_MEJORAR:\n{summary}\n"
            f"CONTEXTO:\n{json.dumps(focused_context, ensure_ascii=False)}"
        )
        try:
            retry_raw = client.generate_text(prompt=retry_prompt, temperature=0.2, max_output_tokens=650)
            retry_summary = _clean_llm_text(retry_raw, max_len=1200)
            if not _summary_looks_weak(retry_summary):
                summary = retry_summary
        except Exception:
            pass

    if _summary_looks_weak(summary):
        return None, meta

    meta["used"] = True
    meta["retries"] = retries
    return summary, meta


def rewrite_clause_with_gemini(
    *,
    clause_label: str,
    clause_type: str,
    current_text: str,
    recommendation: str,
    legal_refs: list[dict[str, Any]] | None = None,
) -> tuple[str | None, dict[str, Any]]:
    client = GeminiClient.from_env()
    meta = {
        "provider": "gemini",
        "configured": client.configured,
        "model": client.model,
        "used": False,
        "error": None,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "agent": "clause_rewrite",
    }
    if not client.configured:
        return None, meta

    refs = []
    for ref in (legal_refs or [])[:3]:
        refs.append(
            {
                "law_name": ref.get("law_name"),
                "article_label": ref.get("article_label"),
                "snippet": (ref.get("snippet") or "")[:160],
            }
        )
    prompt = (
        "Eres abogado senior de contratos en Mexico. "
        "Reescribe una clausula contractual para mejorar equilibrio y claridad legal, "
        "incorporando la recomendacion dada. "
        "Responde SOLO con la redaccion propuesta de la clausula, sin encabezados ni explicaciones.\n"
        f"CLAUSULA_LABEL: {clause_label}\n"
        f"CLAUSULA_TYPE: {clause_type}\n"
        f"TEXTO_ACTUAL: {current_text[:1200]}\n"
        f"RECOMENDACION: {recommendation[:400]}\n"
        f"REFERENCIAS: {json.dumps(refs, ensure_ascii=False)}\n"
    )
    try:
        raw = client.generate_text(prompt=prompt, temperature=0.2, max_output_tokens=900)
    except Exception as exc:  # pragma: no cover - operativo
        meta["error"] = str(exc)
        return None, meta

    rewritten = _clean_llm_text(raw, max_len=1800)
    if len(rewritten) < 40:
        return None, meta
    meta["used"] = True
    return rewritten, meta


def _select_relevant_clauses(question: str, analysis: dict[str, Any]) -> list[dict[str, Any]]:
    clauses = analysis.get("clauses", [])
    if not clauses:
        return []

    query = normalize_for_match(question)
    targets = []
    for keyword, clause_type in QUESTION_TO_CLAUSE.items():
        if keyword in query and clause_type not in targets:
            targets.append(clause_type)
    if targets:
        prioritized = [clause for clause in clauses if clause.get("clause_type") in targets]
        fallback = [clause for clause in clauses if clause.get("clause_type") not in targets]
        return (prioritized + fallback)[:4]

    tokens = [
        tok
        for tok in re.split(r"\W+", query)
        if len(tok) > 2 and tok not in SPANISH_STOPWORDS
    ]
    if not tokens:
        return clauses[:4]

    scored = []
    for clause in clauses:
        label_payload = normalize_for_match((clause.get("clause_label") or "") + " " + (clause.get("clause_type") or ""))
        payload = normalize_for_match(
            (clause.get("extracted_text") or "")
        )
        score = 0
        for tok in tokens:
            if tok in label_payload:
                score += 3
            if tok in payload:
                score += 1
        score += RISK_PRIORITY.get(normalize_for_match((clause.get("risk") or {}).get("level", "")), 0) * 0.05
        scored.append((score, clause))
    scored.sort(key=lambda item: item[0], reverse=True)
    if scored and scored[0][0] <= 0:
        return clauses[:4]
    return [item[1] for item in scored[:4]]


def plan_research_actions_with_gemini(
    *,
    question: str,
    analysis: dict[str, Any],
    feedback_summary: dict[str, Any],
    conversation_context: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    client = GeminiClient.from_env()
    meta = {
        "provider": "gemini",
        "configured": client.configured,
        "model": client.model,
        "used": False,
        "error": None,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "agent": "planner",
    }
    if not client.configured:
        return None, meta

    clauses = []
    for clause in _select_relevant_clauses(question, analysis):
        clauses.append(
            {
                "clause_type": clause.get("clause_type"),
                "clause_label": clause.get("clause_label"),
                "status": clause.get("status"),
                "risk_level": (clause.get("risk") or {}).get("level"),
                "snippet": (clause.get("extracted_text") or "")[:320],
            }
        )

    payload = {
        "question": question,
        "summary": analysis.get("summary", {}),
        "overall_risk": analysis.get("overall_risk", {}),
        "relevant_clauses": clauses,
        "feedback_summary": feedback_summary,
        "conversation_context": conversation_context or {},
    }
    prompt = (
        "Eres un planner LegalOps para un despacho legal en Mexico. "
        "Tu trabajo es decidir SI conviene buscar soporte legal adicional en el acervo normativo, "
        "y en su caso definir tareas de investigacion concretas. "
        "Mantente contextual: usa historial reciente y objetivo activo para no preguntar lo ya resuelto. "
        "Devuelve SOLO JSON valido con esta estructura exacta:\n"
        "{\n"
        '  "conversation_mode":"saludo|descubrimiento|analisis|dictamen|seguimiento",\n'
        '  "needs_research":true|false,\n'
        '  "research_tasks":[\n'
        '    {"query":"texto","purpose":"texto","clause_type":"texto","scope":"federal|state|mixed","jurisdiction_hint":"texto"}\n'
        "  ],\n"
        '  "assistant_intent":"texto"\n'
        "}\n"
        "Reglas: maximo 3 research_tasks; no inventes hechos no soportados por el contexto. "
        "Si la pregunta trata de ley aplicable/jurisdiccion/foro, prioriza clause_type='jurisdiction'.\n"
        f"CONTEXTO:\n{json.dumps(payload, ensure_ascii=False)}"
    )

    try:
        parsed, _ = client.generate_json(prompt=prompt, temperature=0.15, max_output_tokens=1400)
    except Exception as exc:  # pragma: no cover - operativo
        meta["error"] = str(exc)
        return None, meta

    tasks = []
    for task in parsed.get("research_tasks", []):
        if not isinstance(task, dict):
            continue
        query = (task.get("query") or "").strip()
        if len(query) < 4:
            continue
        scope = normalize_for_match(task.get("scope", "mixed"))
        if scope not in {"federal", "state", "mixed"}:
            scope = "mixed"
        tasks.append(
            {
                "query": query[:180],
                "purpose": (task.get("purpose") or "Sustentar analisis y dictamen.").strip()[:180],
                "clause_type": (task.get("clause_type") or "").strip()[:60],
                "scope": scope,
                "jurisdiction_hint": (task.get("jurisdiction_hint") or "").strip()[:80],
            }
        )
        if len(tasks) >= 3:
            break

    mode = normalize_for_match(parsed.get("conversation_mode", "analisis"))
    if mode not in {"saludo", "descubrimiento", "analisis", "dictamen", "seguimiento"}:
        mode = "analisis"

    plan = {
        "conversation_mode": mode,
        "needs_research": bool(parsed.get("needs_research", bool(tasks))),
        "research_tasks": tasks,
        "assistant_intent": (parsed.get("assistant_intent") or "Guiar analisis de riesgos y dictamen.").strip()[:220],
    }
    meta["used"] = True
    return plan, meta


def draft_dialogue_reply_with_gemini(
    *,
    question: str,
    analysis: dict[str, Any],
    legal_fichas: list[dict[str, Any]],
    feedback_summary: dict[str, Any],
    research_plan: dict[str, Any] | None,
    conversation_context: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    client = GeminiClient.from_env()
    meta = {
        "provider": "gemini",
        "configured": client.configured,
        "model": client.model,
        "used": False,
        "error": None,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "agent": "dialogue",
    }
    if not client.configured:
        return None, meta

    relevant_clauses = []
    for clause in _select_relevant_clauses(question, analysis):
        relevant_clauses.append(
            {
                "clause_type": clause.get("clause_type"),
                "clause_label": clause.get("clause_label"),
                "status": clause.get("status"),
                "risk": clause.get("risk", {}),
                "snippet": (clause.get("extracted_text") or "")[:360],
            }
        )

    payload = {
        "question": question,
        "summary": analysis.get("summary", {}),
        "overall_risk": analysis.get("overall_risk", {}),
        "relevant_clauses": relevant_clauses,
        "legal_fichas": legal_fichas[:8],
        "feedback_summary": feedback_summary,
        "research_plan": research_plan or {},
        "conversation_context": conversation_context or {},
    }

    prompt = (
        "Eres un abogado senior experto en LegalOps para Mexico, actuando como copiloto legal de un abogado. "
        "Responde SIEMPRE en tono conversacional natural, cercano y profesional (no robotico). "
        "Mantén continuidad con el historial y el objetivo actual. "
        "Guia al usuario hacia acciones utiles del agente (analisis de riesgos, dudas/incumplimientos, dictamen). "
        "No des asesoria legal definitiva. Si falta evidencia textual, dilo explicitamente. "
        "Devuelve SOLO JSON valido con esta estructura exacta:\n"
        "{\n"
        '  "answer":"texto conversacional",\n'
        '  "confidence":0-1,\n'
        '  "risk_estimate":{"level":"low|medium|high|critical","impacto_probable":"texto","recomendacion_inicial":"texto"},\n'
        '  "missing_evidence":true|false,\n'
        '  "human_review_required":true|false,\n'
        '  "next_actions":["texto","texto"]\n'
        "}\n"
        "Reglas: evita respuestas mecanicas; evita plantillas repetitivas; no inventes articulos ni datos. "
        "Si la pregunta es sobre ley aplicable/jurisdiccion, responde eso en la primera frase y no te desvias a otras clausulas. "
        "Si detectas frustracion o urgencia, reconocelo en una frase breve y pasa a la solucion.\n"
        f"CONTEXTO:\n{json.dumps(payload, ensure_ascii=False)}"
    )

    try:
        parsed, raw = client.generate_json(prompt=prompt, temperature=0.35, max_output_tokens=1700)
    except Exception as exc:  # pragma: no cover - operativo
        meta["error"] = str(exc)
        convo_payload = conversation_context or {}
        fallback_prompt = (
            "Eres un asistente legal conversacional para un despacho en Mexico. "
            "Responde en 2-4 frases claras, naturales y utiles. "
            "No uses listas numeradas ni respuestas roboticas. "
            "Si falta evidencia, dilo y pide siguiente dato concreto.\n"
            f"PREGUNTA: {question}\n"
            f"RIESGO_GLOBAL: {json.dumps(analysis.get('overall_risk', {}), ensure_ascii=False)}\n"
            f"CONTEXTO_CONVERSACION: {json.dumps(convo_payload, ensure_ascii=False)}\n"
        )
        try:
            raw_text = client.generate_text(prompt=fallback_prompt, temperature=0.35, max_output_tokens=500)
            cleaned = _clean_llm_text(raw_text, max_len=1000)
            if cleaned:
                result = {
                    "answer": cleaned,
                    "confidence": 0.72,
                    "risk_estimate": _default_risk_from_analysis(analysis),
                    "missing_evidence": False,
                    "human_review_required": True,
                    "next_actions": [
                        "Puedo detallar riesgos por clausula.",
                        "Si quieres, genero dictamen preliminar.",
                    ],
                }
                meta["used"] = True
                return result, meta
        except Exception:
            pass
        return None, meta

    if not isinstance(parsed, dict) or not REQUIRED_DIALOG_KEYS.issubset(parsed.keys()):
        return None, meta
    if not isinstance(parsed.get("risk_estimate"), dict):
        return None, meta

    risk = parsed.get("risk_estimate", {}) if isinstance(parsed.get("risk_estimate"), dict) else {}
    level = normalize_for_match(risk.get("level", ""))
    if level not in RISK_LEVELS:
        level = "medium"

    answer = (parsed.get("answer") or "").strip()
    if not answer:
        answer = _clean_llm_text(raw, max_len=1400)
    if not answer:
        return None, meta

    next_actions = []
    for action in parsed.get("next_actions", []):
        text = _normalize_next_action(action)
        if text:
            next_actions.append(text[:150])
        if len(next_actions) >= 3:
            break

    default_risk = _default_risk_from_analysis(analysis)
    result = {
        "answer": answer,
        "confidence": _clamp_confidence(parsed.get("confidence", 0.72)),
        "risk_estimate": {
            "level": level,
            "impacto_probable": (risk.get("impacto_probable") or default_risk["impacto_probable"]).strip(),
            "recomendacion_inicial": (risk.get("recomendacion_inicial") or default_risk["recomendacion_inicial"]).strip(),
        },
        "missing_evidence": bool(parsed.get("missing_evidence", False)),
        "human_review_required": bool(parsed.get("human_review_required", level in {"high", "critical"})),
        "next_actions": next_actions
        or [
            "Si quieres, te detallo incumplimientos, dudas y Vo.Bo. por clausula.",
            "Puedo prepararte un dictamen preliminar con recomendaciones negociables.",
        ],
    }
    meta["used"] = True
    return result, meta


def answer_question_with_gemini(
    *,
    question: str,
    analysis: dict[str, Any],
    legal_hits: list[dict[str, Any]],
    feedback_summary: dict[str, Any],
    conversation_context: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    client = GeminiClient.from_env()
    meta = {
        "provider": "gemini",
        "configured": client.configured,
        "model": client.model,
        "used": False,
        "error": None,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    if not client.configured:
        return None, meta

    relevant_clauses = []
    for clause in _select_relevant_clauses(question, analysis):
        relevant_clauses.append(
            {
                "clause_type": clause.get("clause_type"),
                "clause_label": clause.get("clause_label"),
                "status": clause.get("status"),
                "risk": clause.get("risk", {}),
                "snippet": (clause.get("extracted_text") or "")[:420],
            }
        )
    legal_context = []
    for hit in legal_hits[:4]:
        legal_context.append(
            {
                "law_name": hit.get("law_name"),
                "article_label": hit.get("article_label"),
                "scope": hit.get("scope"),
                "jurisdiction": hit.get("jurisdiction"),
                "snippet": (hit.get("snippet") or "")[:180],
            }
        )

    payload = {
        "question": question,
        "summary": analysis.get("summary", {}),
        "overall_risk": analysis.get("overall_risk", {}),
        "clauses": relevant_clauses,
        "legal_context": legal_context,
        "feedback_summary": feedback_summary,
        "conversation_context": conversation_context or {},
    }

    prompt = (
        "Eres un asistente LegalOps para despacho legal en Mexico. "
        "Responde en espanol claro, natural y contextual (como colega experto). "
        "No des asesoria legal definitiva. "
        "Si falta evidencia textual, dilo de forma explicita. "
        "Devuelve SOLO JSON valido con esta estructura exacta:\n"
        "{\n"
        '  "answer":"texto",\n'
        '  "confidence":0-1,\n'
        '  "risk_estimate":{"level":"low|medium|high|critical","impacto_probable":"texto","recomendacion_inicial":"texto"},\n'
        '  "missing_evidence":true|false,\n'
        '  "human_review_required":true|false\n'
        "}\n"
        f"CONTEXTO:\n{json.dumps(payload, ensure_ascii=False)}"
    )

    try:
        parsed, raw = client.generate_json(prompt=prompt, temperature=0.2, max_output_tokens=1600)
    except Exception as exc:  # pragma: no cover - operativo
        meta["error"] = str(exc)
        return None, meta

    risk = parsed.get("risk_estimate", {}) if isinstance(parsed.get("risk_estimate"), dict) else {}
    level = normalize_for_match(risk.get("level", ""))
    if level not in RISK_LEVELS:
        level = "medium"

    answer = (parsed.get("answer") or "").strip()
    if not answer:
        answer = _clean_llm_text(raw, max_len=1300)
    if not answer:
        return None, meta

    default_risk = _default_risk_from_analysis(analysis)
    result = {
        "answer": answer,
        "confidence": _clamp_confidence(parsed.get("confidence", 0.7)),
        "risk_estimate": {
            "level": level,
            "impacto_probable": (risk.get("impacto_probable") or default_risk["impacto_probable"]).strip(),
            "recomendacion_inicial": (risk.get("recomendacion_inicial") or default_risk["recomendacion_inicial"]).strip(),
        },
        "missing_evidence": bool(parsed.get("missing_evidence", False)),
        "human_review_required": bool(parsed.get("human_review_required", level in {"high", "critical"})),
    }
    meta["used"] = True
    return result, meta
