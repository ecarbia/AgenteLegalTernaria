const state = {
  dashboard: null,
  contracts: [],
  selectedDocumentId: null,
  currentAnalysis: null,
  currentFeedback: null,
  chatContextDocumentId: null,
  ragPoller: null,
  backendOk: false,
  llmStatus: null,
  dashboardClientFilter: "",
  dashboardPage: 1,
  dashboardPageSize: 10,
  reviewPage: 1,
  reviewPageSize: 10,
  reviewStatusFilter: "",
};

const menuButtons = Array.from(document.querySelectorAll(".menu-item"));
const tabPanels = Array.from(document.querySelectorAll(".tab-panel"));

const healthPill = document.getElementById("health-pill");
const globalMessage = document.getElementById("global-message");

const refreshDashboardBtn = document.getElementById("refresh-dashboard");
const reanalyzeAllBtn = document.getElementById("reanalyze-all");
const downloadMatrixBtn = document.getElementById("btn-download-matrix");

const metricTotal = document.getElementById("metric-total");
const metricHighRisk = document.getElementById("metric-high-risk");
const metricPending = document.getElementById("metric-pending");
const metricAnalyzed = document.getElementById("metric-analyzed");
const contractsTableBody = document.getElementById("contracts-table-body");
const dashboardClientFilter = document.getElementById("dashboard-client-filter");
const dashboardPageSize = document.getElementById("dashboard-page-size");
const dashboardPrevPage = document.getElementById("dashboard-prev-page");
const dashboardNextPage = document.getElementById("dashboard-next-page");
const dashboardPageInfo = document.getElementById("dashboard-page-info");

const newAnalysisForm = document.getElementById("new-analysis-form");
const fileInput = document.getElementById("file-input");
const intakeProgress = document.getElementById("intake-progress");
const intakeProgressLabel = document.getElementById("intake-progress-label");
const intakeMessage = document.getElementById("intake-message");

const analysisEmpty = document.getElementById("analysis-empty");
const analysisContent = document.getElementById("analysis-content");
const selectedContractName = document.getElementById("selected-contract-name");
const selectedContractMeta = document.getElementById("selected-contract-meta");
const analysisOverallRisk = document.getElementById("analysis-overall-risk");
const analysisOverallScore = document.getElementById("analysis-overall-score");
const analysisRiskIcon = document.getElementById("analysis-risk-icon");
const riskCardGeneral = document.getElementById("risk-card-general");
const riskCardCritical = document.getElementById("risk-card-critical");
const riskCardCompliance = document.getElementById("risk-card-compliance");
const analysisCriticalCount = document.getElementById("analysis-critical-count");
const analysisCriticalMessage = document.getElementById("analysis-critical-message");
const analysisCompliance = document.getElementById("analysis-compliance");
const analysisUpdatesFoot = document.getElementById("analysis-updates-foot");
const analysisExecutiveSummary = document.getElementById("analysis-executive-summary");
const analysisJurisdiction = document.getElementById("analysis-jurisdiction");
const analysisGoverningLaws = document.getElementById("analysis-governing-laws");
const analysisFindingsList = document.getElementById("analysis-findings-list");

const analyzeSelectedBtn = document.getElementById("btn-analyze-selected");
const generateDictamenBtn = document.getElementById("btn-generate-dictamen");
const downloadDictamenBtn = document.getElementById("btn-download-dictamen");
const dictamenPreview = document.getElementById("dictamen-preview");

const chatThread = document.getElementById("chat-thread");
const chatForm = document.getElementById("chat-form");
const chatInput = document.getElementById("chat-input");

const feedbackFab = document.getElementById("feedback-fab");
const feedbackDrawer = document.getElementById("feedback-drawer");
const feedbackClose = document.getElementById("feedback-close");
const feedbackForm = document.getElementById("feedback-form");
const feedbackClause = document.getElementById("feedback-clause");
const feedbackSignal = document.getElementById("feedback-signal");
const feedbackRisk = document.getElementById("feedback-risk");
const feedbackNote = document.getElementById("feedback-note");
const feedbackSummary = document.getElementById("feedback-summary");
const feedbackList = document.getElementById("feedback-list");

const ragBuildSampleBtn = document.getElementById("rag-build-sample");
const ragBuildFullBtn = document.getElementById("rag-build-full");
const ragBuildStateSampleBtn = document.getElementById("rag-build-state-sample");
const ragBuildStateFullBtn = document.getElementById("rag-build-state-full");
const ragCheckUpdatesBtn = document.getElementById("rag-check-updates");
const ragRefreshStatusBtn = document.getElementById("rag-refresh-status");
const ragStatusBox = document.getElementById("rag-status");
const toggleSidebarBtn = document.getElementById("toggle-sidebar");
const reviewTableBody = document.getElementById("review-table-body");
const reviewStatusFilter = document.getElementById("review-status-filter");
const reviewPageSize = document.getElementById("review-page-size");
const reviewRefreshBtn = document.getElementById("review-refresh");
const reviewPrevPage = document.getElementById("review-prev-page");
const reviewNextPage = document.getElementById("review-next-page");
const reviewPageInfo = document.getElementById("review-page-info");

const SIDEBAR_STORAGE_KEY = "agente_legal_sidebar_collapsed";

async function api(path, options = {}) {
  const response = await fetch(path, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || "Error inesperado.");
  }
  return payload;
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatDate(isoDate) {
  if (!isoDate) return "Sin fecha";
  const date = new Date(isoDate);
  if (Number.isNaN(date.getTime())) return isoDate;
  return date.toLocaleString();
}

function riskClass(level) {
  if (!level) return "risk-medium";
  return `risk-${level}`;
}

const MEXICO_STATES = [
  "aguascalientes",
  "baja california",
  "baja california sur",
  "campeche",
  "chiapas",
  "chihuahua",
  "ciudad de mexico",
  "coahuila",
  "colima",
  "durango",
  "estado de mexico",
  "guanajuato",
  "guerrero",
  "hidalgo",
  "jalisco",
  "michoacan",
  "morelos",
  "nayarit",
  "nuevo leon",
  "oaxaca",
  "puebla",
  "queretaro",
  "quintana roo",
  "san luis potosi",
  "sinaloa",
  "sonora",
  "tabasco",
  "tamaulipas",
  "tlaxcala",
  "veracruz",
  "yucatan",
  "zacatecas",
];

function normalizeMatch(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function titleCase(value) {
  return String(value || "")
    .split(" ")
    .filter(Boolean)
    .map((token) => token[0]?.toUpperCase() + token.slice(1))
    .join(" ");
}

function showMessage(text, kind = "ok") {
  globalMessage.textContent = text;
  globalMessage.classList.remove("hidden", "ok", "error");
  globalMessage.classList.add(kind);
}

function clearMessage() {
  globalMessage.classList.add("hidden");
}

function setSidebarCollapsed(collapsed, { persist = true } = {}) {
  document.body.classList.toggle("sidebar-collapsed", Boolean(collapsed));
  if (toggleSidebarBtn) {
    toggleSidebarBtn.textContent = collapsed ? "Mostrar menú" : "Ocultar menú";
  }
  if (!persist) return;
  try {
    window.localStorage.setItem(SIDEBAR_STORAGE_KEY, collapsed ? "1" : "0");
  } catch {
    // Ignore persistence issues.
  }
}

function initSidebarState() {
  let collapsed = false;
  try {
    collapsed = window.localStorage.getItem(SIDEBAR_STORAGE_KEY) === "1";
  } catch {
    collapsed = false;
  }
  setSidebarCollapsed(collapsed, { persist: false });
}

function renderHealthPill() {
  if (!state.backendOk) {
    healthPill.textContent = "Backend no disponible";
    return;
  }
  if (state.llmStatus?.configured) {
    healthPill.textContent = `Backend activo · Gemini ON (${state.llmStatus.model || "default"})`;
    return;
  }
  healthPill.textContent = "Backend activo · Gemini OFF";
}

function setTab(tabId) {
  menuButtons.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.tab === tabId);
  });
  tabPanels.forEach((panel) => {
    panel.classList.toggle("is-active", panel.id === `tab-${tabId}`);
  });
  feedbackFab.classList.toggle("hidden", tabId !== "analysis");
  if (tabId !== "analysis") {
    toggleFeedbackDrawer(false);
  }
  if (tabId === "analysis" && state.selectedDocumentId) {
    loadAnalysisPanel(state.selectedDocumentId);
  }
  if (tabId === "review-queue") {
    loadReviewQueue();
  }
}

function findContract(documentId) {
  return state.contracts.find((item) => item.document_id === documentId) || null;
}

function setIntakeProgress(value, text) {
  const safe = Math.max(0, Math.min(100, value));
  intakeProgress.style.width = `${safe}%`;
  intakeProgressLabel.textContent = `${safe}%`;
  if (text) intakeMessage.textContent = text;
}

function updateAnalyzeSelectedButton(contract, hasAnalysis = false) {
  const selected = contract || findContract(state.selectedDocumentId);
  const ready = Boolean(hasAnalysis || selected?.analysis_available);
  analyzeSelectedBtn.disabled = !state.selectedDocumentId || ready;
  analyzeSelectedBtn.title = ready
    ? "Este contrato ya tiene analisis. Usa Reanalizar desde el dashboard si necesitas recalcular."
    : "Ejecutar analisis para el contrato seleccionado.";
}

function renderClientFilterOptions(clients = []) {
  const current = state.dashboardClientFilter || "";
  if (!dashboardClientFilter) return;
  const options = [`<option value="">Todos</option>`];
  clients.forEach((name) => {
    const selected = current && normalizeMatch(name) === normalizeMatch(current) ? "selected" : "";
    options.push(`<option value="${escapeHtml(name)}" ${selected}>${escapeHtml(name)}</option>`);
  });
  dashboardClientFilter.innerHTML = options.join("");
}

function renderDashboard(payload) {
  const summary = payload.summary || {};
  const contracts = payload.contracts || [];
  const clients = payload.clients || [];
  renderClientFilterOptions(clients);

  metricTotal.textContent = summary.total_contracts || 0;
  metricHighRisk.textContent = summary.high_risk || 0;
  metricPending.textContent = summary.pending_review || 0;
  metricAnalyzed.textContent = summary.analyzed_contracts || 0;

  contractsTableBody.innerHTML = "";
  const pageSize = Number(state.dashboardPageSize || 10);
  const totalPages = Math.max(1, Math.ceil(contracts.length / pageSize));
  state.dashboardPage = Math.min(Math.max(1, state.dashboardPage), totalPages);
  const start = (state.dashboardPage - 1) * pageSize;
  const pagedContracts = contracts.slice(start, start + pageSize);

  if (dashboardPageInfo) {
    dashboardPageInfo.textContent = `Pagina ${state.dashboardPage} de ${totalPages} · ${contracts.length} contratos`;
  }
  if (dashboardPrevPage) dashboardPrevPage.disabled = state.dashboardPage <= 1;
  if (dashboardNextPage) dashboardNextPage.disabled = state.dashboardPage >= totalPages;

  if (!contracts.length) {
    contractsTableBody.innerHTML = `
      <tr>
        <td colspan="9">No hay contratos cargados.</td>
      </tr>
    `;
    return;
  }

  pagedContracts.forEach((contract) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(contract.project_name || contract.document_name)}</td>
      <td>${escapeHtml(contract.client_name || "Sin cliente")}</td>
      <td>${escapeHtml(contract.contract_type || "N/A")}</td>
      <td>${escapeHtml(contract.counterparty || "N/A")}</td>
      <td>${escapeHtml(contract.expected_sign_date || "N/A")}</td>
      <td>${escapeHtml(formatDate(contract.updated_at))}</td>
      <td><span class="risk-badge ${riskClass(contract.risk_level)}">${escapeHtml(contract.risk_level)}</span></td>
      <td><span class="status-chip">${escapeHtml(contract.status || "N/A")}</span></td>
      <td class="actions"></td>
    `;

    const actionsCell = tr.querySelector(".actions");

    const openBtn = document.createElement("button");
    openBtn.type = "button";
    openBtn.className = "btn btn-light";
    openBtn.textContent = "Ver analisis";
    openBtn.addEventListener("click", () => {
      state.selectedDocumentId = contract.document_id;
      setTab("analysis");
    });
    actionsCell.appendChild(openBtn);

    const analyzeBtn = document.createElement("button");
    analyzeBtn.type = "button";
    analyzeBtn.className = "btn btn-light";
    analyzeBtn.textContent = contract.analysis_available ? "Re-analizar" : "Analizar";
    analyzeBtn.addEventListener("click", async () => {
      try {
        await analyzeContract(contract.document_id);
      } catch (error) {
        showMessage(error.message, "error");
      }
    });
    actionsCell.appendChild(analyzeBtn);

    const pdfBtn = document.createElement("button");
    pdfBtn.type = "button";
    pdfBtn.className = "btn btn-light";
    pdfBtn.textContent = "PDF dictamen";
    pdfBtn.addEventListener("click", () => {
      window.open(`/api/export/dictamen/${contract.document_id}.pdf`, "_blank");
    });
    actionsCell.appendChild(pdfBtn);

    contractsTableBody.appendChild(tr);
  });
}

function renderLawList(element, values) {
  element.innerHTML = "";
  const normalized = Array.isArray(values) && values.length ? values : [{ label: "No identificado", url: "" }];
  normalized.forEach((value) => {
    const li = document.createElement("li");
    if (typeof value === "object" && value !== null && value.url) {
      const link = document.createElement("a");
      link.href = String(value.url);
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent = String(value.label || "Referencia normativa");
      li.appendChild(link);
    } else {
      li.textContent = typeof value === "object" && value !== null ? String(value.label || "No identificado") : String(value);
    }
    element.appendChild(li);
  });
}

function toggleFeedbackDrawer(forceOpen) {
  const shouldOpen = typeof forceOpen === "boolean" ? forceOpen : feedbackDrawer.classList.contains("hidden");
  feedbackDrawer.classList.toggle("hidden", !shouldOpen);
  feedbackDrawer.setAttribute("aria-hidden", shouldOpen ? "false" : "true");
  feedbackFab.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
}

function clearCardTones(element) {
  element.classList.remove("tone-low", "tone-medium", "tone-high", "tone-critical");
}

function computeCompliancePercent(analysis) {
  const clauses = analysis.clauses || [];
  if (!clauses.length) return 0;

  const statusWeight = {
    found: 1,
    partial: 0.68,
    not_found: 0.22,
  };
  const riskPenalty = {
    low: 0.02,
    medium: 0.15,
    high: 0.33,
    critical: 0.5,
  };

  let total = 0;
  clauses.forEach((clause) => {
    const base = statusWeight[clause.status] ?? 0.4;
    const penalty = riskPenalty[clause.risk?.level] ?? 0.18;
    total += Math.max(0, base - penalty);
  });
  return Math.round((total / clauses.length) * 100);
}

function sanitizeSummaryText(value, maxLen = 260) {
  const text = String(value || "")
    .replace(/[\x00-\x1F\x7F]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return maxLen > 0 ? text.slice(0, maxLen) : text;
}

function getClauseByType(analysis, clauseType) {
  return (analysis.clauses || []).find((clause) => clause.clause_type === clauseType) || null;
}

function extractSentence(text, keywords, maxLen = 240) {
  const clean = sanitizeSummaryText(text, 900);
  if (!clean) return "";
  const segments = clean
    .split(/(?<=[\.;:])\s+/)
    .map((item) => item.trim())
    .filter(Boolean);
  const candidate = segments.find((segment) => {
    const segmentNorm = normalizeMatch(segment);
    return keywords.some((keyword) => segmentNorm.includes(keyword));
  });
  const selected = candidate || segments[0] || clean;
  return sanitizeSummaryText(selected, maxLen);
}

function isLikelyPartiesText(value) {
  const probe = normalizeMatch(value || "");
  return (
    probe.includes("comparecen") ||
    probe.includes("por una parte") ||
    probe.includes("por la otra parte") ||
    probe.includes("entre")
  );
}

function isLikelyObjectText(value) {
  const probe = normalizeMatch(value || "");
  return probe.includes("objeto") || probe.includes("servicios") || probe.includes("alcance");
}

function isStrongExecutiveSummary(raw) {
  const text = sanitizeSummaryText(raw, 950);
  if (text.length < 80) return false;
  const probe = normalizeMatch(text);
  const weakPatterns = [
    "aborda la relacion entre primera. objeto",
    "objeto principal es: en la ciudad de mexico",
    "regula la relacion entre en la ciudad de mexico",
    "su objeto principal es: objeto del contrato",
    "sobre vigencia/plazo, se observa",
    "hallazgo(s) critico(s)",
  ];
  if (weakPatterns.some((pattern) => probe.includes(pattern))) return false;
  return true;
}

function buildExecutiveSummaryText(contract, analysis) {
  const raw = sanitizeSummaryText(analysis.executive_summary || "", 900);
  if (raw.length >= 80 && isStrongExecutiveSummary(raw)) {
    return raw;
  }

  const summaryParties = Array.isArray(analysis.summary?.parties)
    ? sanitizeSummaryText(analysis.summary.parties.find(Boolean), 330)
    : "";
  const summaryObject = sanitizeSummaryText(analysis.summary?.object || "", 340);

  const partiesClause = getClauseByType(analysis, "parties");
  const objectClause = getClauseByType(analysis, "object");
  const termClause = getClauseByType(analysis, "term");
  const paymentsClause = getClauseByType(analysis, "payments");
  const jurisdictionClause = getClauseByType(analysis, "jurisdiction");
  const context = analysis.contract_context || {};

  const partiesSource = partiesClause?.extracted_text || summaryParties;

  const objectSource = objectClause?.extracted_text || summaryObject;

  const termSource = termClause?.extracted_text || analysis.summary?.term?.duration_text || "";
  const paymentsSource = paymentsClause?.extracted_text || "";
  const jurisdictionSource = jurisdictionClause?.extracted_text || analysis.summary?.jurisdiction || "";

  const partiesText =
    extractSentence(partiesSource, ["comparecen", "por una parte", "por la otra parte", "entre"], 190) ||
    "las partes contratantes identificadas en el contrato";
  const objectText =
    extractSentence(objectSource, ["objeto", "servicios", "alcance", "prest"], 260) ||
    "No se identificó con suficiente precisión el objeto contractual en el texto extraído.";
  const termText = extractSentence(termSource, ["vigencia", "duracion", "plazo", "terminacion", "inicio"], 180);

  const overallLevel = normalizeMatch(analysis.overall_risk?.level || "medium");
  const riskLabelMap = {
    low: "bajo",
    medium: "medio",
    high: "alto",
    critical: "critico",
  };
  const riskLabel = riskLabelMap[overallLevel] || "medio";
  const criticalCount = (analysis.clauses || []).filter((clause) => normalizeMatch(clause.risk?.level) === "critical").length;
  const contractName = contract.project_name || contract.document_name || context.project_name || "analizado";
  const typeLabel = contract.contract_type || context.contract_type || "contrato";
  const paymentsText = extractSentence(paymentsSource, ["pago", "honorarios", "contraprestacion", "factura"], 160);
  const jurisdictionText = extractSentence(
    jurisdictionSource,
    ["ley", "jurisdiccion", "tribunal", "federal", "ciudad de mexico"],
    170
  );

  return [
    `El ${typeLabel.toLowerCase()} ${contractName} documenta la relacion entre ${partiesText}.`,
    `El alcance principal identificado es: ${objectText}.`,
    termText ? `En vigencia/plazo se observa: ${termText}.` : "No se identifico con claridad la vigencia en el texto extraido.",
    paymentsText ? `En pagos destaca: ${paymentsText}.` : "",
    jurisdictionText ? `En jurisdiccion/ley aplicable se identifica: ${jurisdictionText}.` : "",
    `El riesgo global preliminar es ${riskLabel}${criticalCount ? ` y registra ${criticalCount} hallazgo(s) critico(s)` : ""}; se recomienda revision humana para cierre de dictamen.`,
  ]
    .filter(Boolean)
    .join(" ");
}

function resolveCardToneByRisk(level) {
  if (level === "critical") return "tone-critical";
  if (level === "high") return "tone-high";
  if (level === "medium") return "tone-medium";
  return "tone-low";
}

function resolveCardToneByCompliance(percent) {
  if (percent < 50) return "tone-critical";
  if (percent < 75) return "tone-medium";
  return "tone-low";
}

function findNormativeRefsByClause(analysis, clauseType) {
  const refs = [];
  (analysis.legal_grounding || []).forEach((item) => {
    if (item.clause_type !== clauseType) return;
    (item.references || []).forEach((ref) => {
      refs.push({
        lawName: ref.law_name || "Normativa aplicable",
        article: ref.article_label || "Fragmento",
        snippet: (ref.snippet || "").slice(0, 180),
      });
    });
  });
  return refs.slice(0, 3);
}

function getLatestFeedbackEntryByClause() {
  const entries = (state.currentFeedback?.entries || []).slice().sort((a, b) => String(a.created_at || "").localeCompare(String(b.created_at || "")));
  const byClause = {};
  entries.forEach((entry) => {
    const clauseType = String(entry.clause_type || "general").trim() || "general";
    byClause[clauseType] = entry;
  });
  return byClause;
}

function resolveFindingQueueStatus(item) {
  const latestByClause = getLatestFeedbackEntryByClause();
  const latest = latestByClause[item.clauseType];
  const signal = normalizeMatch(latest?.signal || "");
  if (signal === "vobo" || signal === "vo_bo") return "Vo.Bo.";
  if (signal === "duda") return "Duda";
  if (signal === "incumplimiento") return "Requiere revision";
  if (item.level === "critical" || item.level === "high") return "Requiere revision";
  if (item.level === "medium") return "Duda";
  return "Vo.Bo.";
}

function queueStatusClass(status) {
  const probe = normalizeMatch(status);
  if (probe.includes("revision")) return "status-review";
  if (probe.includes("duda")) return "status-duda";
  return "status-vobo";
}

async function submitClauseFeedback(clauseType, signal, note = "") {
  if (!state.selectedDocumentId) return;
  await api("/api/feedback", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      document_id: state.selectedDocumentId,
      clause_type: clauseType,
      signal,
      note,
      reviewer: "abogado_sr",
    }),
  });
  await loadFeedback(state.selectedDocumentId);
  await loadDashboard();
  await loadReviewQueue();
}

async function saveClauseEdit(clauseType, revisedText) {
  if (!state.selectedDocumentId) return;
  await api("/api/clause-edit", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      document_id: state.selectedDocumentId,
      clause_type: clauseType,
      revised_text: revisedText,
      reviewer: "abogado_sr",
      note: "Revision de clausula desde analisis detallado.",
    }),
  });
  await loadAnalysisPanel(state.selectedDocumentId);
  await loadReviewQueue();
}

async function suggestClauseRewrite(clauseType) {
  if (!state.selectedDocumentId) return "";
  const payload = await api("/api/clause-rewrite-suggest", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      document_id: state.selectedDocumentId,
      clause_type: clauseType,
    }),
  });
  return String(payload.suggested_text || "").trim();
}

function buildFindings(analysis) {
  const clauses = analysis.clauses || [];
  const items = [];
  clauses.forEach((clause) => {
    const level = clause.risk?.level || "medium";
    const flagged = level === "critical" || level === "high" || clause.status === "partial" || clause.status === "not_found";
    if (!flagged) return;

    const titleBase = clause.risk?.rationale || "Hallazgo detectado en la cláusula.";
    const title = `${clause.clause_label}: ${titleBase}`.slice(0, 140);
    const detail =
      clause.risk?.probable_impact ||
      (clause.extracted_text ? clause.extracted_text.slice(0, 240) : "No hay suficiente evidencia textual.");
    const refs = findNormativeRefsByClause(analysis, clause.clause_type);
    const signal = level === "critical" || level === "high" ? "Incumplimiento detectado" : "Evidencia incompleta";
    items.push({
      clauseType: clause.clause_type,
      clauseLabel: clause.clause_label,
      level,
      signal,
      title,
      detail,
      editableText: clause.extracted_text || "",
      recommendation: clause.risk?.recommendation_initial || "Escalar a revisión jurídica especializada.",
      refs,
    });
  });

  const priority = { critical: 4, high: 3, medium: 2, low: 1 };
  items.sort((a, b) => (priority[b.level] || 0) - (priority[a.level] || 0));
  return items;
}

function renderFindings(analysis) {
  analysisFindingsList.innerHTML = "";
  const findings = buildFindings(analysis);
  if (!findings.length) {
    analysisFindingsList.innerHTML = `<article class="finding-item"><p class="hint-text">Sin hallazgos prioritarios. Puedes generar dictamen para cierre.</p></article>`;
    return;
  }

  findings.forEach((item, idx) => {
    const detail = document.createElement("details");
    detail.className = `finding-item ${riskClass(item.level)}`;
    if (idx === 0) detail.open = true;
    const queueStatus = resolveFindingQueueStatus(item);
    const refsHtml = item.refs.length
      ? item.refs
          .map(
            (ref) =>
              `<li><strong>${escapeHtml(ref.lawName)}</strong> · ${escapeHtml(ref.article)}<br/>${escapeHtml(ref.snippet)}</li>`
          )
          .join("")
      : "<li>Sin referencia normativa directa recuperada; validar manualmente.</li>";

    detail.innerHTML = `
      <summary>
        <span class="finding-index">${idx + 1}.</span>
        <span class="finding-title">${escapeHtml(item.title)}</span>
        <span class="risk-badge ${riskClass(item.level)}">${escapeHtml(item.level)}</span>
      </summary>
      <div class="finding-content">
        <div class="finding-actions">
          <span class="finding-status ${queueStatusClass(queueStatus)}">${escapeHtml(queueStatus)}</span>
          <button class="btn btn-light btn-quick-feedback" type="button" data-clause="${escapeHtml(
            item.clauseType
          )}" data-signal="incumplimiento">Requiere revision</button>
          <button class="btn btn-light btn-quick-feedback" type="button" data-clause="${escapeHtml(
            item.clauseType
          )}" data-signal="duda">Marcar duda</button>
          <button class="btn btn-light btn-quick-feedback" type="button" data-clause="${escapeHtml(
            item.clauseType
          )}" data-signal="vo_bo">Vo.Bo.</button>
          <button class="btn btn-light btn-open-edit" type="button">Editar clausula</button>
          <button class="btn btn-light btn-rewrite-ai" type="button" data-clause="${escapeHtml(
            item.clauseType
          )}">Reescribir con IA</button>
        </div>
        <p class="finding-signal">${escapeHtml(item.signal)}</p>
        <p class="finding-text">${escapeHtml(item.detail)}</p>
        <div class="finding-edit hidden">
          <textarea class="finding-edit-input">${escapeHtml(item.editableText || "")}</textarea>
          <div class="inline-actions">
            <button class="btn btn-primary btn-save-edit" type="button" data-clause="${escapeHtml(
              item.clauseType
            )}">Guardar version revisada</button>
            <button class="btn btn-light btn-cancel-edit" type="button">Cancelar</button>
          </div>
        </div>
        <div class="finding-box finding-box-law">
          <p class="finding-box-title">Normativa aplicable</p>
          <ul>${refsHtml}</ul>
        </div>
        <div class="finding-box finding-box-rec">
          <p class="finding-box-title">Recomendacion</p>
          <p>${escapeHtml(item.recommendation)}</p>
        </div>
      </div>
    `;

    detail.querySelectorAll(".btn-quick-feedback").forEach((button) => {
      button.addEventListener("click", async () => {
        const clauseType = button.getAttribute("data-clause") || "";
        const signal = button.getAttribute("data-signal") || "duda";
        const signalLabel =
          signal === "vo_bo" ? "Vo.Bo." : signal === "incumplimiento" ? "Requiere revision" : "Duda";
        const approved = window.confirm(
          `¿Confirmas enviar "${signalLabel}" de la clausula ${item.clauseLabel} al abogado Sr para validacion?`
        );
        if (!approved) return;
        const note = window.prompt("Nota para abogado Sr (opcional):", "") || "";
        try {
          await submitClauseFeedback(clauseType, signal, note);
          showMessage("Accion enviada a bandeja de revision legal.", "ok");
          await loadAnalysisPanel(state.selectedDocumentId);
        } catch (error) {
          showMessage(error.message, "error");
        }
      });
    });

    const editWrap = detail.querySelector(".finding-edit");
    const editInput = detail.querySelector(".finding-edit-input");
    const openEditBtn = detail.querySelector(".btn-open-edit");
    const cancelEditBtn = detail.querySelector(".btn-cancel-edit");
    const saveEditBtn = detail.querySelector(".btn-save-edit");
    const rewriteAiBtn = detail.querySelector(".btn-rewrite-ai");

    openEditBtn?.addEventListener("click", () => {
      editWrap?.classList.remove("hidden");
    });
    cancelEditBtn?.addEventListener("click", () => {
      editWrap?.classList.add("hidden");
    });
    saveEditBtn?.addEventListener("click", async () => {
      const clauseType = saveEditBtn.getAttribute("data-clause") || "";
      const revisedText = String(editInput?.value || "").trim();
      if (revisedText.length < 30) {
        showMessage("La clausula revisada necesita al menos 30 caracteres.", "error");
        return;
      }
      const approved = window.confirm(
        `¿Confirmas enviar esta redaccion de ${item.clauseLabel} para revision/Vo.Bo. del abogado Sr?`
      );
      if (!approved) return;
      try {
        await saveClauseEdit(clauseType, revisedText);
        showMessage("Clausula revisada guardada.", "ok");
      } catch (error) {
        showMessage(error.message, "error");
      }
    });
    rewriteAiBtn?.addEventListener("click", async () => {
      const clauseType = rewriteAiBtn.getAttribute("data-clause") || "";
      if (!clauseType) return;
      rewriteAiBtn.disabled = true;
      const previous = rewriteAiBtn.textContent;
      rewriteAiBtn.textContent = "Generando...";
      try {
        const suggestion = await suggestClauseRewrite(clauseType);
        if (!suggestion) {
          showMessage("No se obtuvo sugerencia de redaccion IA.", "error");
          return;
        }
        editWrap?.classList.remove("hidden");
        if (editInput) {
          editInput.value = suggestion;
        }
        showMessage("Propuesta de clausula generada por IA.", "ok");
      } catch (error) {
        showMessage(error.message, "error");
      } finally {
        rewriteAiBtn.disabled = false;
        rewriteAiBtn.textContent = previous || "Reescribir con IA";
      }
    });

    analysisFindingsList.appendChild(detail);
  });
}

function renderRiskSummaryCards(analysis) {
  const overall = analysis.overall_risk || {};
  const level = (overall.level || "medium").toLowerCase();
  const score = Number(overall.score || 0);
  const criticalCount = (analysis.clauses || []).filter((clause) => clause.risk?.level === "critical").length;
  const compliance = computeCompliancePercent(analysis);

  analysisOverallRisk.textContent = level ? level[0].toUpperCase() + level.slice(1) : "-";
  analysisOverallScore.textContent = `Puntuacion: ${Number.isFinite(score) ? Math.round(score) : 0}/100`;
  analysisRiskIcon.textContent = level === "critical" ? "!" : level === "high" ? "!" : level === "medium" ? "▲" : "✓";

  analysisCriticalCount.textContent = String(criticalCount);
  analysisCriticalMessage.textContent =
    criticalCount > 0 ? "Requieren atencion inmediata" : "Sin hallazgos críticos";

  analysisCompliance.textContent = `${compliance}%`;
  const checkedAt = analysis.federal_updates_check?.checked_at;
  analysisUpdatesFoot.textContent = checkedAt
    ? `Normativas verificadas: ${new Date(checkedAt).toLocaleDateString()}`
    : "Normativas federales sin verificacion reciente";

  [riskCardGeneral, riskCardCritical, riskCardCompliance].forEach(clearCardTones);
  riskCardGeneral.classList.add(resolveCardToneByRisk(level));
  riskCardCritical.classList.add(criticalCount > 0 ? "tone-critical" : "tone-low");
  riskCardCompliance.classList.add(resolveCardToneByCompliance(compliance));
}

function toValidHttpUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  try {
    const url = new URL(raw);
    if (url.protocol === "http:" || url.protocol === "https:") {
      return url.toString();
    }
    return "";
  } catch {
    return "";
  }
}

function extractApplicableLaws(analysis) {
  const laws = [];
  const seen = new Set();
  const pushLaw = (lawName, url = "") => {
    const label = sanitizeSummaryText(lawName, 170);
    const normalized = normalizeMatch(label);
    if (!label || normalized === "no identificado") return;
    if (normalized.includes("identificada en el documento") || normalized.includes("validar detalle")) return;
    const key = normalized;
    if (seen.has(key)) return;
    seen.add(key);
    laws.push({ label, url: toValidHttpUrl(url) });
  };

  pushLaw(analysis.summary?.governing_law || "", "");
  (analysis.legal_grounding || []).forEach((item) => {
    (item.references || []).forEach((reference) => {
      pushLaw(reference.law_name, reference.ref_url || reference.doc_url);
    });
  });
  return laws.slice(0, 10);
}

function resolveJurisdiction(analysis) {
  const jurisdictionText = normalizeMatch(analysis.summary?.jurisdiction || "");
  const detectedStates = new Set();

  MEXICO_STATES.forEach((stateName) => {
    if (jurisdictionText.includes(stateName)) {
      detectedStates.add(stateName);
    }
  });
  if (jurisdictionText.includes("cdmx") || jurisdictionText.includes("distrito federal")) {
    detectedStates.add("ciudad de mexico");
  }

  let hasFederal =
    jurisdictionText.includes("federal") ||
    jurisdictionText.includes("estados unidos mexicanos") ||
    jurisdictionText.includes("mexico");

  (analysis.legal_grounding || []).forEach((item) => {
    (item.references || []).forEach((reference) => {
      const scope = normalizeMatch(reference.scope);
      const jurisdiction = normalizeMatch(reference.jurisdiction);
      if (scope === "federal" || jurisdiction === "mexico_federal") {
        hasFederal = true;
      }
      if (scope === "state" && jurisdiction && jurisdiction !== "mexico_federal") {
        detectedStates.add(jurisdiction.replaceAll("_", " "));
      }
    });
  });

  const states = Array.from(detectedStates)
    .filter((value) => value && value !== "mexico federal")
    .map((value) => titleCase(value))
    .sort((a, b) => a.localeCompare(b));

  if (hasFederal && states.length) {
    return `Federal + Estatal (${states.join(", ")})`;
  }
  if (hasFederal) return "Federal";
  if (states.length) return `Estatal (${states.join(", ")})`;
  return "No identificado";
}

function renderAnalysis(contract, analysis) {
  analysisEmpty.classList.add("hidden");
  analysisContent.classList.remove("hidden");
  state.currentAnalysis = analysis;

  selectedContractName.textContent = contract.project_name || contract.document_name || "Contrato";
  selectedContractMeta.textContent =
    `${contract.contract_type || "Tipo no definido"} · ${contract.counterparty || "Sin contraparte"} · ` +
    `${contract.expected_sign_date || "Sin fecha prevista"}`;

  renderRiskSummaryCards(analysis);
  analysisExecutiveSummary.textContent = buildExecutiveSummaryText(contract, analysis);
  analysisJurisdiction.textContent = resolveJurisdiction(analysis);
  renderLawList(analysisGoverningLaws, extractApplicableLaws(analysis));
  renderFindings(analysis);
  updateAnalyzeSelectedButton(contract, true);

  if (state.chatContextDocumentId !== contract.document_id) {
    chatThread.innerHTML = "";
    appendChatMessage(
      "assistant",
      `<p>Soy Contract Analyst Agent. Estoy listo para ayudarte a revisar riesgos, resolver dudas normativas y cerrar dictamen de este contrato. ¿En qué quieres empezar?</p>`
    );
    state.chatContextDocumentId = contract.document_id;
  }

  feedbackClause.innerHTML = `<option value="">General</option>`;
  (analysis.clauses || []).forEach((clause) => {
    const option = document.createElement("option");
    option.value = clause.clause_type;
    option.textContent = clause.clause_label || clause.clause_type;
    feedbackClause.appendChild(option);
  });
}

function appendChatMessage(role, htmlContent) {
  const block = document.createElement("article");
  block.className = `chat-msg ${role}`;
  block.innerHTML = htmlContent;
  chatThread.appendChild(block);
  chatThread.scrollTop = chatThread.scrollHeight;
}

function renderFeedback(payload) {
  state.currentFeedback = payload;
  const summary = payload.summary || {};
  const signals = summary.signals || {};
  feedbackSummary.textContent =
    `Feedback total: ${summary.total || 0} · ` +
    `Incumplimiento: ${signals.incumplimiento || 0} · ` +
    `Duda: ${signals.duda || 0} · Vo.Bo.: ${signals.vo_bo || 0}`;

  feedbackList.innerHTML = "";
  const entries = payload.entries || [];
  if (!entries.length) {
    feedbackList.innerHTML = "<li>Sin feedback.</li>";
    if (state.currentAnalysis) {
      renderFindings(state.currentAnalysis);
    }
    return;
  }
  entries
    .slice()
    .reverse()
    .slice(0, 12)
    .forEach((entry) => {
      const li = document.createElement("li");
      li.innerHTML =
        `<strong>${escapeHtml(entry.signal || "general")}</strong> · ` +
        `${escapeHtml(entry.clause_type || "general")} · ` +
        `${escapeHtml(entry.note || "Sin nota")}<br/>` +
        `<span class="chat-meta">${escapeHtml(formatDate(entry.created_at))}</span>`;
      feedbackList.appendChild(li);
    });

  if (state.currentAnalysis) {
    renderFindings(state.currentAnalysis);
  }
}

function renderDictamen(dictamen) {
  dictamenPreview.classList.remove("hidden");
  const incumplimientos = dictamen.incumplimientos || [];
  const dudas = dictamen.dudas || [];
  const vobo = dictamen.vo_bo || [];
  const recomendaciones = dictamen.recomendaciones || [];

  dictamenPreview.innerHTML = `
    <h4>Dictamen: ${escapeHtml(dictamen.conclusion || "N/A")}</h4>
    <p><strong>Riesgo global:</strong> ${escapeHtml(dictamen.overall_risk?.level || "N/A")} (${escapeHtml(
      String(dictamen.overall_risk?.score || "N/A")
    )})</p>
    <p><strong>Incumplimientos:</strong> ${incumplimientos.length}</p>
    <p><strong>Dudas:</strong> ${dudas.length}</p>
    <p><strong>Vo.Bo.:</strong> ${vobo.length}</p>
    <p><strong>Recomendaciones:</strong> ${escapeHtml(recomendaciones.slice(0, 3).join(" | ") || "N/A")}</p>
    <p class="chat-meta">${escapeHtml(dictamen.disclaimer || "")}</p>
  `;
}

async function checkHealth() {
  try {
    await api("/api/health");
    state.backendOk = true;
  } catch {
    state.backendOk = false;
  }
  renderHealthPill();
}

async function refreshLlmStatus() {
  try {
    const payload = await api("/api/llm/status");
    state.llmStatus = payload;
  } catch {
    state.llmStatus = null;
  }
  renderHealthPill();
}

async function loadDashboard() {
  const params = new URLSearchParams();
  if (state.dashboardClientFilter) {
    params.set("client_name", state.dashboardClientFilter);
  }
  const path = params.toString() ? `/api/dashboard?${params.toString()}` : "/api/dashboard";
  const payload = await api(path);
  state.dashboard = payload;
  state.contracts = payload.contracts || [];
  state.dashboardClientFilter = payload.filters?.client_name || state.dashboardClientFilter || "";
  renderDashboard(payload);
  const selected = findContract(state.selectedDocumentId);
  updateAnalyzeSelectedButton(selected, Boolean(selected?.analysis_available));
}

function renderReviewQueue(payload) {
  if (!reviewTableBody) return;
  const items = payload.items || [];
  reviewTableBody.innerHTML = "";
  if (!items.length) {
    reviewTableBody.innerHTML = `<tr><td colspan="7">Sin elementos en la bandeja.</td></tr>`;
  } else {
    items.forEach((item) => {
      const tr = document.createElement("tr");
      const statusClass = queueStatusClass(item.queue_status || "Duda");
      tr.innerHTML = `
        <td>${escapeHtml(item.project_name || item.document_name || "Contrato")}</td>
        <td>${escapeHtml(item.client_name || "Sin cliente")}</td>
        <td>${escapeHtml(item.clause_label || item.clause_type || "Clausula")}</td>
        <td><span class="finding-status ${statusClass}">${escapeHtml(item.queue_status || "Duda")}</span></td>
        <td><span class="risk-badge ${riskClass(item.risk_level)}">${escapeHtml(item.risk_level || "medium")}</span></td>
        <td>${escapeHtml(item.latest_feedback_note || "Sin nota")}</td>
        <td><button class="btn btn-light btn-open-analysis" type="button">Abrir</button></td>
      `;
      tr.querySelector(".btn-open-analysis")?.addEventListener("click", () => {
        state.selectedDocumentId = item.document_id;
        setTab("analysis");
      });
      reviewTableBody.appendChild(tr);
    });
  }

  const pagination = payload.pagination || {};
  const page = Number(pagination.page || 1);
  const pages = Number(pagination.pages || 1);
  const total = Number(pagination.total || 0);
  if (reviewPageInfo) {
    reviewPageInfo.textContent = `Pagina ${page} de ${pages} · ${total} elementos`;
  }
  if (reviewPrevPage) reviewPrevPage.disabled = page <= 1;
  if (reviewNextPage) reviewNextPage.disabled = page >= pages;
}

async function loadReviewQueue() {
  if (!reviewTableBody) return;
  const params = new URLSearchParams();
  params.set("page", String(state.reviewPage));
  params.set("per_page", String(state.reviewPageSize));
  if (state.reviewStatusFilter) params.set("status", state.reviewStatusFilter);
  if (state.dashboardClientFilter) params.set("client_name", state.dashboardClientFilter);
  const payload = await api(`/api/review/queue?${params.toString()}`);
  state.reviewPage = Number(payload.pagination?.page || state.reviewPage || 1);
  renderReviewQueue(payload);
}

async function loadFeedback(documentId) {
  const payload = await api(`/api/feedback/${documentId}`);
  renderFeedback(payload);
}

async function analyzeContract(documentId) {
  showMessage("Analizando contrato...", "ok");
  await api(`/api/analyze/${documentId}`, { method: "POST" });
  await loadDashboard();
  await loadReviewQueue();
  if (state.selectedDocumentId === documentId) {
    await loadAnalysisPanel(documentId);
  }
  showMessage("Analisis completo.", "ok");
}

async function loadAnalysisPanel(documentId) {
  if (!documentId) {
    analysisEmpty.classList.remove("hidden");
    analysisContent.classList.add("hidden");
    updateAnalyzeSelectedButton(null, false);
    return;
  }
  const contract = findContract(documentId);
  if (!contract) {
    await loadDashboard();
  }
  const actualContract = findContract(documentId);
  if (!actualContract) {
    analysisEmpty.classList.remove("hidden");
    analysisContent.classList.add("hidden");
    updateAnalyzeSelectedButton(null, false);
    return;
  }

  try {
    const refreshSummary = state.llmStatus?.configured ? "?refresh_summary=true" : "";
    const payload = await api(`/api/analysis/${documentId}${refreshSummary}`);
    renderAnalysis(actualContract, payload.analysis);
    await loadFeedback(documentId);
  } catch (error) {
    analysisEmpty.classList.add("hidden");
    analysisContent.classList.remove("hidden");
    selectedContractName.textContent = actualContract.project_name || actualContract.document_name;
    selectedContractMeta.textContent = "Sin analisis. Usa el boton 'Analizar seleccionado'.";
    analysisOverallRisk.textContent = "-";
    analysisOverallScore.textContent = "Puntuacion: -/100";
    analysisCriticalCount.textContent = "0";
    analysisCriticalMessage.textContent = "Sin hallazgos críticos";
    analysisCompliance.textContent = "0%";
    analysisUpdatesFoot.textContent = "Sin verificacion normativa";
    analysisExecutiveSummary.textContent = "Sin resumen ejecutivo disponible.";
    [riskCardGeneral, riskCardCritical, riskCardCompliance].forEach(clearCardTones);
    riskCardGeneral.classList.add("tone-medium");
    riskCardCritical.classList.add("tone-low");
    riskCardCompliance.classList.add("tone-low");
    analysisJurisdiction.textContent = "No identificado";
    renderLawList(analysisGoverningLaws, []);
    analysisFindingsList.innerHTML = `<article class="finding-item"><p class="hint-text">Aun no existe analisis para este contrato.</p></article>`;
    feedbackSummary.textContent = "Sin feedback registrado.";
    feedbackList.innerHTML = "<li>Sin feedback.</li>";
    dictamenPreview.classList.add("hidden");
    updateAnalyzeSelectedButton(actualContract, false);
  }
}

function renderRagStatus(payload) {
  const byScope = payload.by_scope || {};
  const lastRun = payload.job?.last_run ? formatDate(payload.job.last_run) : "Sin ejecucion";
  const updatesCount = (payload.meta?.federal_updates_keys || []).length;
  ragStatusBox.innerHTML = `
    <p><strong>Running:</strong> ${payload.job?.running ? "Si" : "No"}</p>
    <p><strong>Fuentes:</strong> ${payload.sources || 0} (Federal ${byScope.federal || 0} / Estatal ${
      byScope.state || 0
    })</p>
    <p><strong>Chunks:</strong> ${payload.chunks || 0}</p>
    <p><strong>Ultima corrida:</strong> ${escapeHtml(lastRun)}</p>
    <p><strong>Actualizaciones federales:</strong> ${updatesCount}</p>
  `;
}

async function refreshRagStatus() {
  const payload = await api("/api/rag/status");
  renderRagStatus(payload);
  return payload;
}

function startRagPolling() {
  if (state.ragPoller) {
    clearInterval(state.ragPoller);
  }
  state.ragPoller = setInterval(async () => {
    try {
      const status = await refreshRagStatus();
      if (!status.job?.running) {
        clearInterval(state.ragPoller);
        state.ragPoller = null;
      }
    } catch {
      clearInterval(state.ragPoller);
      state.ragPoller = null;
    }
  }, 4000);
}

async function triggerRagBuild(limit) {
  const body = limit ? { limit } : {};
  const payload = await api("/api/rag/rebuild", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  showMessage(payload.message, "ok");
  await refreshRagStatus();
  startRagPolling();
}

async function triggerStateBuild(limitStates, maxPages = 26, includeCdmx = true) {
  const body = {
    max_pages: maxPages,
    include_cdmx: includeCdmx,
  };
  if (limitStates) body.limit_states = limitStates;
  const payload = await api("/api/rag/rebuild-state", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  showMessage(payload.message, "ok");
  await refreshRagStatus();
  startRagPolling();
}

async function triggerFederalUpdatesCheck() {
  const payload = await api("/api/rag/check-federal-updates", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({}),
  });
  const changed = payload.updates_check?.hash_changed ? "Si" : "No";
  showMessage(`Actualizaciones federales verificadas. Cambio detectado: ${changed}.`, "ok");
  await refreshRagStatus();
}

menuButtons.forEach((button) => {
  button.addEventListener("click", () => setTab(button.dataset.tab));
});

if (toggleSidebarBtn) {
  toggleSidebarBtn.addEventListener("click", () => {
    const collapsed = !document.body.classList.contains("sidebar-collapsed");
    setSidebarCollapsed(collapsed);
  });
}

dashboardClientFilter?.addEventListener("change", async () => {
  state.dashboardClientFilter = dashboardClientFilter.value || "";
  state.dashboardPage = 1;
  state.reviewPage = 1;
  try {
    await loadDashboard();
    await loadReviewQueue();
  } catch (error) {
    showMessage(error.message, "error");
  }
});

dashboardPageSize?.addEventListener("change", () => {
  state.dashboardPageSize = Number(dashboardPageSize.value || 10);
  state.dashboardPage = 1;
  renderDashboard(state.dashboard || { summary: {}, contracts: [] });
});

dashboardPrevPage?.addEventListener("click", () => {
  state.dashboardPage = Math.max(1, state.dashboardPage - 1);
  renderDashboard(state.dashboard || { summary: {}, contracts: [] });
});

dashboardNextPage?.addEventListener("click", () => {
  const totalContracts = (state.dashboard?.contracts || []).length;
  const maxPage = Math.max(1, Math.ceil(totalContracts / Math.max(1, state.dashboardPageSize)));
  state.dashboardPage = Math.min(maxPage, state.dashboardPage + 1);
  renderDashboard(state.dashboard || { summary: {}, contracts: [] });
});

reviewStatusFilter?.addEventListener("change", async () => {
  state.reviewStatusFilter = reviewStatusFilter.value || "";
  state.reviewPage = 1;
  try {
    await loadReviewQueue();
  } catch (error) {
    showMessage(error.message, "error");
  }
});

reviewPageSize?.addEventListener("change", async () => {
  state.reviewPageSize = Number(reviewPageSize.value || 10);
  state.reviewPage = 1;
  try {
    await loadReviewQueue();
  } catch (error) {
    showMessage(error.message, "error");
  }
});

reviewPrevPage?.addEventListener("click", async () => {
  state.reviewPage = Math.max(1, state.reviewPage - 1);
  try {
    await loadReviewQueue();
  } catch (error) {
    showMessage(error.message, "error");
  }
});

reviewNextPage?.addEventListener("click", async () => {
  state.reviewPage += 1;
  try {
    await loadReviewQueue();
  } catch (error) {
    state.reviewPage = Math.max(1, state.reviewPage - 1);
    showMessage(error.message, "error");
  }
});

reviewRefreshBtn?.addEventListener("click", async () => {
  try {
    await loadReviewQueue();
    showMessage("Bandeja de revision actualizada.", "ok");
  } catch (error) {
    showMessage(error.message, "error");
  }
});

feedbackFab.addEventListener("click", () => toggleFeedbackDrawer());
feedbackClose.addEventListener("click", () => toggleFeedbackDrawer(false));

document.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  if (!feedbackDrawer.classList.contains("hidden")) toggleFeedbackDrawer(false);
});

refreshDashboardBtn.addEventListener("click", async () => {
  try {
    await loadDashboard();
    await loadReviewQueue();
    showMessage("Dashboard actualizado.", "ok");
  } catch (error) {
    showMessage(error.message, "error");
  }
});

reanalyzeAllBtn.addEventListener("click", async () => {
  try {
    showMessage("Reanalizando todos los contratos...", "ok");
    const payload = await api("/api/analyze-all", { method: "POST" });
    await loadDashboard();
    await loadReviewQueue();
    showMessage(
      `Reanalisis finalizado. Procesados: ${payload.processed || 0}, OK: ${payload.ok || 0}, errores: ${
        payload.errors || 0
      }.`,
      payload.errors ? "error" : "ok"
    );
    if (state.selectedDocumentId) {
      await loadAnalysisPanel(state.selectedDocumentId);
    }
  } catch (error) {
    showMessage(error.message, "error");
  }
});

downloadMatrixBtn.addEventListener("click", () => {
  window.open("/api/export/consolidated.pdf", "_blank");
});

newAnalysisForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  clearMessage();

  if (!fileInput.files.length) {
    showMessage("Selecciona un archivo de contrato.", "error");
    return;
  }

  try {
    setIntakeProgress(8, "Subiendo contrato...");

    const formData = new FormData(newAnalysisForm);
    if (document.getElementById("include-historical").checked) {
      formData.set("include_historical", "true");
    } else {
      formData.set("include_historical", "false");
    }
    const uploadPayload = await api("/api/documents", {
      method: "POST",
      body: formData,
    });

    const documentId = uploadPayload.document?.document_id;
    setIntakeProgress(55, "Ejecutando analisis contractual...");
    await api(`/api/analyze/${documentId}`, { method: "POST" });
    setIntakeProgress(100, "Proceso completo.");

    await loadDashboard();
    await loadReviewQueue();
    newAnalysisForm.reset();
    state.selectedDocumentId = documentId;
    await loadAnalysisPanel(documentId);
    setTab("analysis");
    showMessage("Contrato cargado y analizado.", "ok");
  } catch (error) {
    setIntakeProgress(0, "Error en proceso de carga.");
    showMessage(error.message, "error");
  }
});

analyzeSelectedBtn.addEventListener("click", async () => {
  if (!state.selectedDocumentId) {
    showMessage("Selecciona un contrato primero.", "error");
    return;
  }
  try {
    await analyzeContract(state.selectedDocumentId);
  } catch (error) {
    showMessage(error.message, "error");
  }
});

generateDictamenBtn.addEventListener("click", async () => {
  if (!state.selectedDocumentId) {
    showMessage("Selecciona un contrato primero.", "error");
    return;
  }
  try {
    const payload = await api(`/api/dictamen/${state.selectedDocumentId}`);
    renderDictamen(payload.dictamen);
    showMessage("Dictamen generado.", "ok");
  } catch (error) {
    showMessage(error.message, "error");
  }
});

downloadDictamenBtn.addEventListener("click", () => {
  if (!state.selectedDocumentId) {
    showMessage("Selecciona un contrato primero.", "error");
    return;
  }
  window.open(`/api/export/dictamen/${state.selectedDocumentId}.pdf`, "_blank");
});

chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const question = (chatInput.value || "").trim();
  if (!question) return;
  if (!state.selectedDocumentId) {
    showMessage("Selecciona un contrato para usar el chat.", "error");
    return;
  }

  appendChatMessage("user", `<p>${escapeHtml(question)}</p>`);
  chatInput.value = "";

  try {
    const payload = await api("/api/questions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        document_id: state.selectedDocumentId,
        question,
      }),
    });

    const citations = (payload.citations || [])
      .slice(0, 2)
      .map(
        (item) =>
          `<li>${escapeHtml(item.section || "Seccion")}: ${escapeHtml((item.snippet || "").slice(0, 96))}</li>`
      )
      .join("");

    const legalRefs = (payload.legal_citations || [])
      .slice(0, 2)
      .map(
        (item) =>
          `<li>${escapeHtml(item.law_name || "Ley")} · ${escapeHtml(item.article_label || "Fragmento")}</li>`
      )
      .join("");
    const nextActions = (payload.next_actions || [])
      .slice(0, 3)
      .map((item) => `<li>${escapeHtml(item)}</li>`)
      .join("");

    const sourcesBlock = `
      ${citations
        ? `<details class="chat-sources"><summary>Fuentes del contrato (${(payload.citations || []).slice(0, 2).length})</summary><ul>${citations}</ul></details>`
        : ""}
      ${legalRefs
        ? `<details class="chat-sources"><summary>Fuentes normativas (${(payload.legal_citations || []).slice(0, 2).length})</summary><ul>${legalRefs}</ul></details>`
        : ""}
    `;

    appendChatMessage(
      "assistant",
      `
        <p>${escapeHtml(payload.answer || "Sin respuesta.")}</p>
        ${
          payload.chat_mode === "small_talk"
            ? ""
            : `<div class="chat-meta">
          Riesgo: ${escapeHtml(payload.risk_estimate?.level || "N/A")} ·
          Confianza: ${escapeHtml(String(payload.confidence || "N/A"))}
        </div>`
        }
        ${nextActions ? `<div class="chat-next"><strong>Proximo paso sugerido:</strong><ul>${nextActions}</ul></div>` : ""}
        ${sourcesBlock}
      `
    );
  } catch (error) {
    appendChatMessage("assistant", `<p>Error: ${escapeHtml(error.message)}</p>`);
  }
});

chatInput.addEventListener("keydown", (event) => {
  if (event.isComposing) return;
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    chatForm.requestSubmit();
  }
});

feedbackForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!state.selectedDocumentId) {
    showMessage("Selecciona un contrato para guardar feedback.", "error");
    return;
  }

  try {
    await api("/api/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        document_id: state.selectedDocumentId,
        clause_type: feedbackClause.value || "",
        signal: feedbackSignal.value,
        corrected_risk: feedbackRisk.value || "",
        note: feedbackNote.value || "",
      }),
    });
    feedbackNote.value = "";
    await loadFeedback(state.selectedDocumentId);
    await loadDashboard();
    await loadReviewQueue();
    await loadAnalysisPanel(state.selectedDocumentId);
    showMessage("Feedback guardado.", "ok");
  } catch (error) {
    showMessage(error.message, "error");
  }
});

ragBuildSampleBtn.addEventListener("click", async () => {
  try {
    await triggerRagBuild(30);
  } catch (error) {
    showMessage(error.message, "error");
  }
});

ragBuildFullBtn.addEventListener("click", async () => {
  try {
    await triggerRagBuild(null);
  } catch (error) {
    showMessage(error.message, "error");
  }
});

ragBuildStateSampleBtn.addEventListener("click", async () => {
  try {
    await triggerStateBuild(6, 20, true);
  } catch (error) {
    showMessage(error.message, "error");
  }
});

ragBuildStateFullBtn.addEventListener("click", async () => {
  try {
    await triggerStateBuild(null, 30, true);
  } catch (error) {
    showMessage(error.message, "error");
  }
});

ragCheckUpdatesBtn.addEventListener("click", async () => {
  try {
    await triggerFederalUpdatesCheck();
  } catch (error) {
    showMessage(error.message, "error");
  }
});

ragRefreshStatusBtn.addEventListener("click", async () => {
  try {
    await refreshRagStatus();
  } catch (error) {
    showMessage(error.message, "error");
  }
});

async function init() {
  initSidebarState();
  updateAnalyzeSelectedButton(null, false);
  toggleFeedbackDrawer(false);
  if (dashboardPageSize) dashboardPageSize.value = String(state.dashboardPageSize);
  if (reviewPageSize) reviewPageSize.value = String(state.reviewPageSize);
  await checkHealth();
  await refreshLlmStatus();
  try {
    await Promise.all([loadDashboard(), refreshRagStatus(), loadReviewQueue()]);
    setIntakeProgress(0, "Completa el formulario para iniciar.");
    setTab("dashboard");
    if (state.backendOk && !state.llmStatus?.configured) {
      showMessage("Gemini no configurado. Operando en modo heuristico.", "error");
    }
  } catch (error) {
    showMessage(error.message, "error");
  }
}

init();
