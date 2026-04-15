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

## Clasificación de Registro por hora del evento

El campo `AsisteD.Registro` se asigna según la **hora del marcaje**, no el valor Punch del dispositivo:

| Ventana horaria       | `Registro` en AsisteD | `@TipoCorte` del job |
|-----------------------|-----------------------|----------------------|
| `< 12:00:00`          | `'Entrada'`           | `0`                  |
| `12:00:00 – 12:49:59` | Descartado (Estatus=4)| —                    |
| `12:50:00 – 15:59:59` | `'Comida'`            | `4`                  |
| `>= 16:00:00`         | `'Salida'`            | `1`                  |

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

### Prerequisitos
- SQL Server Agent habilitado y corriendo
- El usuario tiene acceso de lectura/escritura en `Checador` y en los 4 ERPs
- El usuario tiene permisos de `sysadmin` o `SQLAgentOperatorRole` en `msdb`
- Las tablas `dbo.EmpresaConfig` y `dbo.MarcajeDispatchQueue` ya existen en `Checador` (creadas en el piloto)
- El trigger y el SP ya están instalados en `Checador`

### Paso 1 — Activar las 4 empresas

```sql
-- Ejecutar en: Checador
-- Script: prod_01_activar_empresas.sql
```

Revisar la Sección A. Si algún ERP muestra `✗ ERROR`, corregir permisos antes de continuar.  
La Sección B actualiza `EmpresaConfig`: activa las 4 empresas y apunta cotailor a `cotailor7000`.

### Paso 2 — Backfill desde el 1 de abril

```sql
-- Ejecutar en: Checador
-- Script: prod_02_backfill_abril.sql
```

Encola todo lo de `2026-04-01` en adelante. Se puede re-ejecutar sin duplicar.  
Anotar el total de registros encolados por empresa.

### Paso 3 — Prueba con batch pequeño

```sql
USE Checador;

EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 0, @BatchSize = 10;   -- Entrada
EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 4, @BatchSize = 10;   -- Comida
EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 1, @BatchSize = 10;   -- Salida
```

Verificar en los 4 ERPs:
```sql
-- Ver últimos inserts por empresa
SELECT TOP 10 d.ID, d.Personal, d.Registro, d.HoraRegistro, d.Fecha
FROM [kingv7].dbo.AsisteD d
INNER JOIN [kingv7].dbo.Asiste a ON a.ID = d.ID
WHERE a.MovID = 'AVC1' AND a.Usuario = 'INTELISIS'
ORDER BY d.ID DESC;
-- Repetir para obsidianav7, bbgv7, cotailor7000
```

Verificar que `Registro` coincide con la hora:
- Hora < 12:00 → `'Entrada'`
- Hora 12:50–15:59 → `'Comida'`
- Hora >= 16:00 → `'Salida'`

### Paso 4 — Crear los Agent Jobs

```sql
-- Ejecutar en: msdb
-- Script: prod_03_jobs_produccion.sql
```

A partir de aquí el sistema es completamente automático. Los jobs corren a las 12:00, 16:00, 23:00 y martes 23:30.

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
