# Especificacion Funcional - Agente LegalOps (MVP v1)

## 1) Mision del agente

Centralizar la ingesta de documentos legales (principalmente contratos), extraer clausulas esenciales, detectar y dimensionar riesgos, y responder consultas en lenguaje natural con trazabilidad documental.

## 2) Objetivos de negocio del MVP

1. Reducir tiempos de revision legal.
2. Estandarizar criterios de analisis contractual.
3. Identificar riesgos tempranos con priorizacion clara.
4. Mejorar calidad y velocidad de respuesta del despacho.

## 3) Alcance funcional v1

- Ingesta de contratos `PDF/DOCX/TXT` en un repositorio unico.
- Extraccion estructurada de clausulas clave:
  - partes
  - objeto
  - vigencia
  - pagos
  - terminacion
  - responsabilidad
  - indemnizacion
  - confidencialidad
  - propiedad intelectual
  - jurisdiccion
  - cumplimiento
- Deteccion de riesgo por clausula con:
  - nivel `low|medium|high|critical`
  - impacto probable
  - recomendacion inicial
- Respuestas a preguntas en lenguaje natural con citas de fuente:
  - documento
  - seccion
  - pagina (cuando aplique)

## 4) Reglas criticas (guardrails)

- No emitir asesoria legal definitiva.
- Entregar analisis tecnico y sugerir revision humana.
- No inventar informacion.
- Si falta evidencia, declararlo explicitamente.
- Cada hallazgo debe incluir referencia verificable al texto original.
- Priorizar confidencialidad, trazabilidad y auditabilidad.

## 5) Fuera de alcance v1

- Firma electronica y ejecucion contractual automatica.
- Negociacion automatica con contrapartes.
- Integraciones profundas con ERP/CRM/CLM enterprise.
- Aprobaciones legales 100% autonomas sin humano.

## 6) Flujo operativo end-to-end

1. `Ingesta`
   - Carga individual y por lote.
   - Validacion de formato, checksum, deduplicacion y metadata minima.
2. `Normalizacion`
   - OCR si aplica.
   - Parsing por tipo de archivo.
   - Segmentacion por secciones/paginas/parrafos.
3. `Extraccion`
   - Identificacion de clausulas objetivo.
   - Estructura de salida usando `clause_extraction.schema.json`.
4. `Riesgo`
   - Evaluacion por clausula con `risk_rubric.yaml`.
   - Score + nivel + explicacion + recomendacion inicial.
5. `QA con trazabilidad`
   - Respuesta a consulta en lenguaje natural.
   - Citas obligatorias a evidencia documental.
6. `Revision humana`
   - Confirmacion/rechazo de hallazgos.
   - Registro de decisiones para mejora continua.

## 7) Arquitectura logica minima

- `Document Repository`
  - almacenamiento cifrado de archivos y metadatos.
- `Text Extraction Service`
  - parsing/OCR y segmentacion.
- `Clause Extraction Service`
  - salida estructurada por clausula.
- `Risk Engine`
  - aplica reglas de severidad, impacto y recomendacion.
- `QA Service`
  - responde preguntas usando evidencia trazable.
- `Audit Service`
  - bitacora de ingesta, cambios, decisiones humanas y acceso.
- `LegalOps UI`
  - matriz consolidada semaforizada.
  - detalle por contrato.
  - chat juridico con citas.

## 8) Modelo de datos minimo (resumen)

### Entidad `document`

- `document_id`
- `name`
- `source_type`
- `repository_path`
- `ingestion_timestamp`
- `hash_sha256`
- `page_count` (si aplica)

### Entidad `clause_finding`

- `finding_id`
- `document_id`
- `clause_type`
- `status` (`found|partial|not_found|ambiguous`)
- `extracted_text`
- `normalized_data`
- `evidence[]` (doc/seccion/pagina/snippet)
- `risk`

### Entidad `analysis_report`

- `analysis_id`
- `document_id`
- `overall_risk`
- `critical_flags[]`
- `compliance_checks[]`
- `human_review`
- `generated_at`

## 9) API minima sugerida (REST)

- `POST /legalops/documents`
  - ingesta de contrato.
- `GET /legalops/documents/{documentId}`
  - metadata + estado de procesamiento.
- `POST /legalops/analysis`
  - ejecutar extraccion y riesgo.
- `GET /legalops/analysis/{analysisId}`
  - reporte estructurado completo.
- `POST /legalops/questions`
  - consulta en lenguaje natural con citas.

### Ejemplo de respuesta `POST /legalops/questions`

```json
{
  "answer": "Se detecta limite de responsabilidad unilateral a favor del proveedor.",
  "confidence": 0.87,
  "citations": [
    {
      "document_id": "doc_2026_0007",
      "document_name": "Contrato_Servicios_X.docx",
      "section": "Limitacion de responsabilidad",
      "page": 14,
      "snippet": "El proveedor no sera responsable por danos indirectos..."
    }
  ],
  "missing_evidence": false,
  "human_review_required": true,
  "disclaimer": "Analisis tecnico automatizado. Requiere validacion de abogado responsable."
}
```

## 10) Matriz de priorizacion de riesgo

- `critical`: potencial dano severo inmediato o incumplimiento grave.
- `high`: exposicion economica/operativa relevante.
- `medium`: riesgo moderado con mitigacion factible.
- `low`: riesgo acotado y controlable.

Reglas y gatillos iniciales se especifican en `risk_rubric.yaml`.

## 11) Seguridad, confidencialidad y auditoria

- Cifrado en transito y en reposo.
- Control de acceso por rol (RBAC).
- Registro inmutable de:
  - acceso a documentos
  - ejecuciones de analisis
  - cambios de clasificacion
  - validaciones humanas
- Politica de retencion y purge.
- Aislamiento por tenant/cliente.

## 12) KPIs del MVP (objetivo)

- `Tiempo contrato unico`: <= 20 min desde carga a dictamen inicial.
- `Tiempo lote 100 contratos`: <= 48 h a matriz consolidada.
- `Precision extraccion clausulas criticas`: >= 95% en muestra auditada.
- `Reduccion tiempo total de revision`: >= 40% vs proceso manual.
- `Adopcion`: >= 70% de abogados usando la plataforma al menos 1 vez/semana.

## 13) Criterios de aceptacion de v1

1. El sistema procesa `PDF/DOCX/TXT` y devuelve salida valida contra `clause_extraction.schema.json`.
2. Todo hallazgo tiene al menos una evidencia trazable.
3. Toda respuesta de QA incluye citas y disclaimer.
4. Hallazgos `high/critical` quedan marcados para revision humana.
5. La matriz consolidada muestra severidad por contrato y por clausula.

## 14) Plan de entrega por fases (12 semanas)

- `Semanas 1-2`: requisitos, reglas de riesgo, arquitectura segura.
- `Semanas 3-5`: ingesta y repositorio.
- `Semanas 5-8`: extraccion, riesgo, UX de analisis y chat.
- `Semanas 9-10`: piloto controlado con lote real.
- `Semanas 11-12`: hyper-care, documentacion y transferencia.

## 15) Decision operativa clave para MVP

Mantener `lawyer-in-the-loop` como requisito obligatorio para minimizar riesgo de error legal y acelerar adopcion interna sin comprometer responsabilidad profesional.
