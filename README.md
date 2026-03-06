# Agente LegalOps MVP

MVP funcional para despacho legal con:

- ingesta de contratos (`PDF`, `DOCX`, `TXT`),
- extraccion automatica de clausulas clave,
- deteccion de riesgo semaforizado,
- preguntas en lenguaje natural con trazabilidad documental y memoria conversacional por contrato.

## Levantar local

1. Ir a la carpeta del proyecto:

```bash
cd /Users/eduardocarbia/Desktop/Scripts/agente-legal
```

2. Ejecutar backend + UI:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Opcional (recomendado) para modo LLM real:
# export GEMINI_API_KEY="tu_api_key"
# export GEMINI_MODEL="gemini-1.5-flash"
python3 app.py
```

3. Abrir en navegador:

```text
http://127.0.0.1:5050
```

## Conectar LLM (Gemini)

Define variables de entorno antes de correr `app.py`:

```bash
export GEMINI_API_KEY="tu_api_key_de_google_ai_studio"
export GEMINI_MODEL="gemini-1.5-flash"
```

Validar estado:

```bash
curl -s http://127.0.0.1:5050/api/llm/status
```

## Construir RAG legal (Leyes de Mexico)

El agente incorpora un RAG normativo con fuente oficial:

- [LeyesBiblio - Cámara de Diputados](https://www.diputados.gob.mx/LeyesBiblio/index.htm)
- [Leyes de los Estados](https://www.diputados.gob.mx/LeyesBiblio/gobiernos.htm)
- [Actualizaciones federales](https://www.diputados.gob.mx/LeyesBiblio/actual/ultima.htm)

Opciones:

1. Desde la UI, sección `Base normativa federal (RAG)`:
   - `Indexar muestra (30 leyes)` para validar rápido.
   - `Indexar completo` para todo el catálogo detectado.
   - `Estados/CDMX muestra` para validar indexación estatal.
   - `Estados/CDMX completo` para indexar ligas de `Leyes del Estado` + CDMX (Congreso y Gobierno).
   - `Verificar actualizaciones federales` para sincronizar cambios detectados en `actual/ultima.htm`.
2. Desde CLI:

```bash
cd /Users/eduardocarbia/Desktop/Scripts/agente-legal
python3 build_legal_rag.py --mode federal --limit 30
python3 build_legal_rag.py --mode state --state-limit 8 --state-max-pages 20
python3 build_legal_rag.py --mode updates
python3 build_legal_rag.py --mode all
```

El índice se guarda en:

- `data/legal_corpus/legal_rag.sqlite`

## Cómo funciona el RAG

- Descarga DOCs vigentes de `LeyesBiblio`.
- Convierte DOC a texto con `textutil`.
- Segmenta por `Artículo ...`.
- Indexa en SQLite FTS5 para retrieval semántico-lexical rápido.
- Inyecta citas legales en respuestas (`legal_citations`) y en análisis (`legal_grounding`).
- Indexa leyes estatales desde ligas `Leyes del Estado` de `gobiernos.htm`.
- Incluye `Leyes de la Ciudad de México` (sitio del Congreso y sitio del Gobierno/consejería).
- Verifica `actual/ultima.htm` previo a consultas y sincroniza leyes federales actualizadas cuando hay cambios.

## Flujo de prueba recomendado

1. Ve a `Nuevo Analisis`, captura metadatos y sube contrato.
2. El sistema analiza y lo agrega al `Dashboard` consolidado/semaforizado.
3. En `Analisis IA` consulta por chat, revisa hallazgos y emite feedback abogado (`lawyer-in-the-loop`).
4. Genera `dictamen individual` y descarga PDF.
5. Descarga la `matriz consolidada` en PDF.

Puedes usar el archivo de ejemplo:

- `samples/contrato_demo.txt`

## Estructura principal

- `app.py`: API Flask + rutas de UI.
- `legalops_engine.py`: parser, extraccion, riesgo, Q&A con citas.
- `templates/index.html`: interfaz web.
- `static/styles.css`: estilos y responsive layout.
- `static/app.js`: logica cliente.
- `data/`: repositorio local de uploads, texto extraido y analisis.

## Endpoints API

- `GET /api/health`
- `GET /api/llm/status`
- `GET /api/dashboard`
- `GET /api/documents`
- `POST /api/documents` (multipart field `file`)
- `POST /api/analyze/<document_id>`
- `POST /api/analyze-all`
- `GET /api/analysis/<document_id>`
- `GET /api/feedback/<document_id>`
- `POST /api/feedback`
- `GET /api/dictamen/<document_id>`
- `GET /api/export/dictamen/<document_id>.pdf`
- `GET /api/export/consolidated.pdf`
- `POST /api/questions`
- `GET /api/rag/status`
- `POST /api/rag/rebuild`
- `POST /api/rag/rebuild-state`
- `POST /api/rag/check-federal-updates`
- `POST /api/rag/search`

### Ejemplo `POST /api/questions`

```json
{
  "document_id": "doc_123abc",
  "question": "Que riesgos ves en indemnizacion y responsabilidad?"
}
```

### Ejemplo `POST /api/rag/rebuild`

```json
{
  "limit": 30
}
```

## Documentacion base del proyecto

- `MVP_ESPECIFICACION.md`
- `clause_extraction.schema.json`
- `risk_rubric.yaml`
- `qa_response_template.md`
- `system_prompt_legalops.md`

## Guardrails aplicados

- No entrega asesoria legal definitiva.
- No inventa informacion; reporta faltante de evidencia.
- Respuestas con citas verificables.
- Prioriza confidencialidad, trazabilidad y auditabilidad.
- Tono conversacional natural, evitando respuestas rígidas tipo plantilla.
