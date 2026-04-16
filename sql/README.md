# Integración MB160 → ERPs Intelisis

Orquestación SQL Server para despachar marcajes del checador MB160 a las tablas `Asiste`/`AsisteD` de cada ERP de la misma instancia.

---

## Arquitectura general

```
Dispositivo MB160
    │
    ▼  (Python poller)
Checador.dbo.AsistenciaMarcaje  ──INSERT──►  TRIGGER
                                               │
                                               ▼
                                   Checador.dbo.MarcajeDispatchQueue
                                   (Estatus=0, Pendiente)
                                               │
                              SQL Agent Jobs (horarios de corte)
                                               │
                          ┌────────────────────┼────────────────────┐
                          ▼                    ▼                    ▼
                     kingv7.dbo          obsidianav7.dbo        bbgv7.dbo
                     cotailor7000.dbo
                     Asiste + AsisteD → spAfectar
```

**No se modifica ningún archivo Python.** La orquestación es 100% SQL Server.

---

## Modelo de movimientos

Por cada empleado y día se generan **hasta 4 movimientos** en `Asiste`, cada uno con sus renglones
en `AsisteD` (un renglón por marcaje del checador dentro de esa ventana).

```
Asiste (Mov='Entrada')       → AsisteD: todos los marcajes < 12:00
Asiste (Mov='SalidaComida')  → AsisteD: 1er marcaje en 12:50–15:59
Asiste (Mov='EntradaComida') → AsisteD: 2do marcaje en 12:50–15:59
Asiste (Mov='Salida')        → AsisteD: todos los marcajes >= 16:00
```

`AsisteD.Registro` = mismo valor que `Asiste.Mov` del encabezado.

### Clasificación de TipoMov por hora del evento

| Ventana horaria       | `Asiste.Mov` / `AsisteD.Registro` | Condición adicional          | `@TipoCorte` del job |
|-----------------------|-----------------------------------|------------------------------|----------------------|
| `< 12:00:00`          | `'Entrada'`                       | —                            | `0`                  |
| `12:00:00 – 12:49:59` | Descartado (Estatus=4)            | Zona gris, sin categoría     | —                    |
| `12:50:00 – 15:59:59` | `'SalidaComida'`                  | 1er marcaje del empleado+día | `4`                  |
| `12:50:00 – 15:59:59` | `'EntradaComida'`                 | 2do marcaje del empleado+día | `4`                  |
| `12:50:00 – 15:59:59` | Descartado (Estatus=4)            | 3er+ marcaje → excedente     | —                    |
| `>= 16:00:00`         | `'Salida'`                        | —                            | `1`                  |

La posición (1ro/2do) se calcula con `ROW_NUMBER()` particionado por `(BaseDatos, PersonaID, DATE(EventoFechaHora))` ordenado por hora.

---

## Empresas configuradas

| EmpresaID | Prefijo UD | Base ERP        | CodigoEmpresa |
|-----------|------------|-----------------|---------------|
| 2         | `2`        | `kingv7`        | GNS           |
| 3         | `3`        | `obsidianav7`   | GNS           |
| 4         | `4`        | `bbgv7`         | GNS           |
| 5         | `5`        | `cotailor7000`  | GNS           |

**Prefijo:** primer dígito de `UsuarioDispositivo` (ej. `50076` → empresa 5, Personal = `50076`).

---

## Tablas creadas en `Checador`

### `dbo.EmpresaConfig`
Mapeo prefijo → base ERP.

| Columna        | Tipo           | Descripción                                  |
|----------------|----------------|----------------------------------------------|
| EmpresaConfigID| INT IDENTITY   | PK                                           |
| EmpresaID      | INT            | 2, 3, 4, 5                                  |
| EmpresaPrefix  | CHAR(1)        | '2','3','4','5' — primer dígito de UD        |
| BaseDatos      | SYSNAME        | Nombre de la DB del ERP                      |
| CodigoEmpresa  | NVARCHAR(50)   | Valor en `Asiste.Empresa` (ej. 'GNS')        |
| Activo         | BIT            | 1 = activa, 0 = ignorada                     |

### `dbo.MarcajeDispatchQueue`
Cola de despacho. Un registro por marcaje (UNIQUE AsistenciaMarcajeID).

| Columna                | Tipo           | Descripción                                  |
|------------------------|----------------|----------------------------------------------|
| MarcajeDispatchQueueID | BIGINT IDENTITY| PK                                           |
| AsistenciaMarcajeID    | BIGINT         | FK a AsistenciaMarcaje (UNIQUE)              |
| EmpresaID              | INT            |                                              |
| BaseDatos              | SYSNAME        | Destino ERP                                  |
| PersonaID              | INT            | = CAST(UsuarioDispositivo AS INT) completo   |
| Punch                  | TINYINT        | Valor original del dispositivo (referencia)  |
| EventoFechaHora        | DATETIME2(0)   |                                              |
| Estatus                | TINYINT        | 0=Pendiente 1=Procesando 2=Hecho 3=Error 4=Descartado |
| Intentos               | INT            | Reintentos automáticos (Estatus=3 → reintenta) |
| UltimoError            | NVARCHAR(4000) | Mensaje del último error                     |
| AsisteID               | INT            | ID generado en Asiste del ERP                |
| ProcesadoEn            | DATETIME2(3)   |                                              |

---

## Objetos SQL creados en `Checador`

| Objeto                                        | Tipo    | Descripción                                              |
|-----------------------------------------------|---------|----------------------------------------------------------|
| `dbo.EmpresaConfig`                           | Tabla   | Configuración de empresas                                |
| `dbo.MarcajeDispatchQueue`                    | Tabla   | Cola de despacho                                         |
| `dbo.tr_AsistenciaMarcaje_DispatchQueue`      | Trigger | AFTER INSERT en AsistenciaMarcaje → encola automáticamente|
| `dbo.sp_ProcessMarcajeQueue`                  | SP      | Orquestador: toma batch, inserta en ERP, llama spAfectar |

### SP: `sp_ProcessMarcajeQueue`

```sql
EXEC dbo.sp_ProcessMarcajeQueue
    @TipoCorte TINYINT = NULL,  -- 0=Entrada  4=Comida  1=Salida  NULL=todos
    @BatchSize INT     = 200
```

Usa `READPAST + UPDLOCK + ROWLOCK` para procesar concurrentemente sin duplicados.  
Captura `AsisteID` con `OUTPUT INSERTED.ID` (no `SCOPE_IDENTITY`).  
`Renglon` en AsisteD = `MAX(Renglon)+1` global de toda la tabla.  
Cada grupo (empleado+día+TipoMov) se procesa en **transacción explícita** — si falla algún paso, Asiste+AsisteD se revierten y los registros del grupo quedan en Estatus=3 (reintentable).  
Post-`spAfectar`: valida que `Asiste.MovID` no sea NULL; si lo es, fuerza Estatus=3 en lugar de Hecho.

---

## Jobs de SQL Server Agent

| Job                    | Horario           | `@TipoCorte` | Ventana procesada  |
|------------------------|-------------------|--------------|--------------------|
| `MB160_Corte_Entrada`  | Diario 12:00      | `0`          | marcajes < 12:00   |
| `MB160_Corte_Comida`   | Diario 16:00      | `4`          | 12:50 – 15:59      |
| `MB160_Corte_Salida`   | Diario 23:00      | `1`          | >= 16:00           |
| `MB160_Corte_Semanal`  | Martes 23:30      | `NULL`       | todos los pendientes|

---

## Inventario de scripts

### Scripts de infraestructura (ejecutar una sola vez)

| Script                                          | Dónde ejecutar | Descripción                                        |
|-------------------------------------------------|----------------|----------------------------------------------------|
| `create_EmpresaConfig.sql`                      | `Checador`     | Crea tabla EmpresaConfig                           |
| `create_MarcajeDispatchQueue.sql`               | `Checador`     | Crea tabla MarcajeDispatchQueue + índice           |
| `create_trigger_AsistenciaMarcaje_DispatchQueue.sql` | `Checador` | Crea trigger AFTER INSERT                         |
| `sp_ProcessMarcajeQueue.sql`                    | `Checador`     | Crea SP orquestador                                |
| `create_job_ProcessMarcajeQueue.sql`            | `msdb`         | Crea los 4 Agent Jobs (requiere reemplazar `<NOMBRE_BASE_CHECADOR>`) |

### Scripts de piloto (cotailordev — referencia)

| Script                          | Dónde ejecutar | Descripción                                          |
|---------------------------------|----------------|------------------------------------------------------|
| `pilot_01_setup.sql`            | `Checador`     | Setup completo del piloto (EmpresaConfig + tablas + trigger + SP) |
| `pilot_02_backfill.sql`         | `Checador`     | Encola histórico de cotailordev                      |
| `pilot_03_verificacion.sql`     | `Checador`     | Consultas de verificación cruzada checador ↔ ERP     |
| `pilot_04_jobs_cotailordev.sql` | `msdb`         | Jobs del piloto                                      |
| `pilot_05_deploy_cotailordev.sql` | `Checador`   | Deploy + batch de prueba + validación con nuevas reglas |

### Scripts de producción

| Script                        | Dónde ejecutar | Descripción                                             |
|-------------------------------|----------------|---------------------------------------------------------|
| `prod_01_activar_empresas.sql`| `Checador`     | Pre-checks de los 4 ERPs + activa todas en EmpresaConfig|
| `prod_02_backfill_abril.sql`  | `Checador`     | Encola histórico desde 2026-04-01 para todas las empresas|
| `prod_03_jobs_produccion.sql` | `msdb`         | Crea los 4 jobs con `Checador` hardcodeado              |

---

## Instrucciones de despliegue en producción

> **Flujo validado en cotailordev (2026-04-15).**  
> La base de cotailor en producción es `cotailor7000`.

### Prerequisitos
- SQL Server Agent habilitado y corriendo
- El usuario tiene acceso de lectura/escritura en `Checador` y en los 4 ERPs
- El usuario tiene permisos de `sysadmin` o `SQLAgentOperatorRole` en `msdb`
- Las tablas `dbo.EmpresaConfig` y `dbo.MarcajeDispatchQueue` ya existen en `Checador` (creadas en el piloto)
- El trigger y el SP ya están instalados en `Checador`

---

### Paso 1 — Redesplegar el SP en Checador

El SP fue actualizado con dos correcciones críticas validadas en piloto:
1. `Estatus = 'SINAFECTAR'` (sin espacio) — requerido por `spAfectar`
2. `INSERT AsisteD` **antes** de `EXEC spAfectar` — spAfectar necesita los renglones para afectar

```sql
-- Ejecutar en: Checador
-- Script: sp_ProcessMarcajeQueue.sql
-- (usar CREATE OR ALTER — reemplaza sin borrar datos)
```

Verificar que se actualizó:
```sql
USE Checador;
SELECT modify_date FROM sys.objects WHERE name = 'sp_ProcessMarcajeQueue';
-- La fecha debe ser de hoy
```

---

### Paso 2 — Activar las 4 empresas

```sql
-- Ejecutar en: Checador
-- Script: prod_01_activar_empresas.sql
```

Revisar la Sección A (pre-checks). Si algún ERP muestra `✗ ERROR`, corregir permisos antes de continuar.  
La Sección B actualiza `EmpresaConfig`: activa las 4 empresas y apunta cotailor a `cotailor7000`.

| EmpresaID | Prefijo | Base ERP        |
|-----------|---------|-----------------|
| 2         | `2`     | `kingv7`        |
| 3         | `3`     | `obsidianav7`   |
| 4         | `4`     | `bbgv7`         |
| 5         | `5`     | `cotailor7000`  |

---

### Paso 3 — Backfill desde el 1 de abril

```sql
-- Ejecutar en: Checador
-- Script: prod_02_backfill_abril.sql
```

Encola todo lo de `2026-04-01` en adelante para las 4 empresas activas.  
Se puede re-ejecutar sin duplicar (tiene guard `WHERE NOT EXISTS`).  
Anotar el total de registros encolados por empresa.

---

### Paso 4 — Prueba con batch pequeño (5 por tipo)

```sql
USE Checador;

-- Procesar 5 de cada tipo
EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 0, @BatchSize = 5;   -- Entrada  (< 12:00)
EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 4, @BatchSize = 5;   -- Comida   (12:50–15:59)
EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 1, @BatchSize = 5;   -- Salida   (>= 16:00)
```

Verificar en **cada uno** de los 4 ERPs:

```sql
-- Últimos inserts (repetir para kingv7, obsidianav7, bbgv7, cotailor7000)
SELECT TOP 5 a.ID, a.MovID, a.Estatus, d.Personal, d.Registro, d.HoraRegistro, d.Fecha
FROM [cotailor7000].dbo.AsisteD d
INNER JOIN [cotailor7000].dbo.Asiste a ON a.ID = d.ID
WHERE a.Usuario = 'INTELISIS'
ORDER BY d.ID DESC;
```

**Resultado esperado:**
- `Asiste.Estatus = 'PROCESAR'`
- `Asiste.MovID` con folio generado (no NULL)
- `AsisteD.Registro` correcto según hora del evento
- Sin `UltimoError` en la cola

Verificar la cola:
```sql
USE Checador;
SELECT BaseDatos, Estatus,
       CASE Estatus WHEN 0 THEN 'Pendiente' WHEN 1 THEN 'Procesando'
                    WHEN 2 THEN 'Hecho' WHEN 3 THEN 'Error' WHEN 4 THEN 'Descartado' END AS Desc,
       COUNT(*) AS Total
FROM dbo.MarcajeDispatchQueue
GROUP BY BaseDatos, Estatus
ORDER BY BaseDatos, Estatus;
```

Si hay registros con `Estatus=3`, revisar `UltimoError` antes de continuar.

---

### Paso 5 — Procesar el backfill completo

```sql
USE Checador;
EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = NULL, @BatchSize = 500;
-- Ejecutar varias veces hasta que no queden Estatus=0
```

---

### Paso 6 — Crear los Agent Jobs

```sql
-- Ejecutar en: msdb
-- Script: prod_03_jobs_produccion.sql
```

A partir de aquí el sistema es completamente automático.

| Job                   | Horario          | `@TipoCorte` |
|-----------------------|------------------|--------------|
| `MB160_Corte_Entrada` | Diario 12:00     | `0`          |
| `MB160_Corte_Comida`  | Diario 16:00     | `4`          |
| `MB160_Corte_Salida`  | Diario 23:00     | `1`          |
| `MB160_Corte_Semanal` | Martes 23:30     | `NULL`       |

---

## Verificación diaria

```sql
USE Checador;

-- Estado general de la cola
SELECT
    BaseDatos,
    Estatus,
    CASE Estatus
        WHEN 0 THEN 'Pendiente'
        WHEN 1 THEN 'Procesando'
        WHEN 2 THEN 'Hecho'
        WHEN 3 THEN 'Error'
        WHEN 4 THEN 'Descartado'
    END AS Descripcion,
    COUNT(*) AS Total
FROM dbo.MarcajeDispatchQueue
WHERE CAST(FechaRegistro AS DATE) = CAST(SYSDATETIME() AS DATE)
GROUP BY BaseDatos, Estatus
ORDER BY BaseDatos, Estatus;

-- Ver errores pendientes de revisión
SELECT TOP 20
    MarcajeDispatchQueueID,
    BaseDatos,
    PersonaID,
    EventoFechaHora,
    Intentos,
    UltimoError
FROM dbo.MarcajeDispatchQueue
WHERE Estatus = 3
ORDER BY UltimoCambio DESC;
```

---

## Troubleshooting

### Error: `OUTPUT INSERTED.ID regresó NULL`
El campo `Asiste.ID` no se está generando automáticamente en el INSERT. Revisar si la tabla `Asiste` del ERP tiene un trigger o procedimiento que asigna el ID — si es así, verificar que `spAfectar` no interfiere.

### Error: `Cannot insert NULL into Renglon`
No debe ocurrir con el SP actual. Si aparece, verificar que la versión desplegada es la más reciente (`CREATE OR ALTER` en SSMS).

### Registros con Estatus=3 que no se reintentan
Los reintentos ocurren en el siguiente ciclo del job. Si un registro acumula muchos intentos sin resolverse, revisar `UltimoError` y corregir el problema en el ERP (ej. PersonaID inexistente).

### Registros con Estatus=4 (Descartado)
Esperado para marcajes entre 12:00 y 12:49 (zona gris sin categoría). Si hay un volumen inusual, revisar los horarios de los turnos.

### Nuevo marcaje no aparece en la cola
Verificar que el trigger está activo:
```sql
USE Checador;
SELECT name, is_disabled FROM sys.triggers
WHERE name = 'tr_AsistenciaMarcaje_DispatchQueue';
-- is_disabled debe ser 0
```

Verificar que el `UsuarioDispositivo` del empleado tiene un prefijo registrado en `EmpresaConfig` con `Activo=1`.

---

## Agregar una nueva empresa

1. Definir el prefijo (primer dígito de `UsuarioDispositivo` de los empleados).
2. Insertar en `EmpresaConfig`:
```sql
USE Checador;
INSERT INTO dbo.EmpresaConfig (EmpresaID, EmpresaPrefix, BaseDatos, CodigoEmpresa, Activo)
VALUES (<ID>, '<prefijo>', N'<nombre_base_erp>', N'<codigo>', 1);
```
3. El trigger y el SP funcionan automáticamente para la nueva empresa sin cambios adicionales.
4. Ejecutar backfill si hay histórico previo.
