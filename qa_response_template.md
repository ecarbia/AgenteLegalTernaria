# Plantilla de respuesta QA con trazabilidad (v1)

Usar este formato para responder preguntas sobre contratos. No omitir secciones.

## Formato de salida

```markdown
### Respuesta
[Respuesta tecnica breve en lenguaje claro. Si no hay evidencia suficiente, declararlo aqui.]

### Hallazgos clave
1. [Hallazgo 1]
2. [Hallazgo 2]
3. [Hallazgo 3]

### Riesgo estimado
- Nivel: [low|medium|high|critical]
- Impacto probable: [descripcion]
- Recomendacion inicial: [accion concreta]

### Evidencia documental
1. Documento: [nombre o id]
   - Seccion: [seccion o "No identificada"]
   - Pagina: [numero o "No aplica"]
   - Extracto: "[snippet literal]"
2. Documento: [nombre o id]
   - Seccion: [seccion o "No identificada"]
   - Pagina: [numero o "No aplica"]
   - Extracto: "[snippet literal]"

### Cobertura de evidencia
- Estado: [Completa|Parcial|Insuficiente]
- Brechas detectadas: [lista corta de faltantes]

### Revision humana
- Requiere revision: [Si|No]
- Motivo: [razon]

### Aviso
Analisis tecnico automatizado. No constituye asesoria legal definitiva; requiere validacion de abogado responsable.
```

## Reglas de uso obligatorias

- No responder sin al menos una cita de evidencia.
- Si la pregunta no puede responderse con evidencia disponible, indicar:
  - "No encuentro evidencia suficiente en los documentos disponibles."
- No inventar clausulas, paginas, secciones ni montos.
- Si existen contradicciones entre fuentes, describirlas explicitamente.
- Marcar revision humana obligatoria para riesgo `high` o `critical`.

## Ejemplo breve

```markdown
### Respuesta
La clausula de indemnizacion es unilateral y no limita responsabilidad maxima.

### Hallazgos clave
1. La obligacion de indemnizar solo aplica al cliente.
2. No hay tope de indemnizacion.
3. No se define derecho de defensa ante reclamos de terceros.

### Riesgo estimado
- Nivel: critical
- Impacto probable: Exposicion economica no acotada en reclamos de terceros.
- Recomendacion inicial: Negociar reciprocidad, tope y mecanismo de defensa.

### Evidencia documental
1. Documento: Contrato_Servicios_X.docx
   - Seccion: Indemnizacion
   - Pagina: 18
   - Extracto: "El Cliente indemnizara al Proveedor por cualquier reclamacion..."

### Cobertura de evidencia
- Estado: Parcial
- Brechas detectadas: No se encontro anexo de limites de responsabilidad.

### Revision humana
- Requiere revision: Si
- Motivo: Hallazgo clasificado como critical.

### Aviso
Analisis tecnico automatizado. No constituye asesoria legal definitiva; requiere validacion de abogado responsable.
```
