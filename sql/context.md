# Contexto del proyecto — MB160 Marcaje → Intelisis ERP

## ¿Qué hace este sistema?

Puente entre el dispositivo de asistencia **MB160** y el ERP **Intelisis**.
Los marcajes del checador se sincronizan a la tabla `AsistenciaMarcaje` vía API. Desde ahí, un trigger los encola en `MarcajeDispatchQueue`. Un SP orquestador (`sp_ProcessMarcajeQueue`) los despacha al ERP generando movimientos de asistencia (`Asiste` / `AsisteD`).

---

## Arquitectura general

```
MB160 (checador)
    ↓ HTTP POST (API .NET)
AsistenciaMarcaje          ← tabla principal de marcajes
    ↓ AFTER INSERT trigger
MarcajeDispatchQueue       ← cola de despacho (Estatus: 0=Pendiente 1=Procesando 2=Hecho 3=Error 4=Descartado)
    ↓ SQL Agent Job (scheduled)
sp_ProcessMarcajeQueue     ← SP orquestador
    ↓ sp_executesql cross-DB
[ERP].dbo.Asiste           ← encabezado de movimiento
[ERP].dbo.AsisteD          ← detalle (renglones)
    ↓ spAfectar
MovID generado (folio ERP)
```

---

## Bases de datos ERP (misma instancia SQL Server)

| BD             | Prefijo PersonaID | Empresa |
|----------------|-------------------|---------|
| `kingv7`       | 2                 | King    |
| `obsidianav7`  | 3                 | Obsidiana |
| `bbgv7`        | 4                 | BBG     |
| `cotailordev`  | 5                 | Cotailor (ambiente de pruebas) |

La tabla `EmpresaConfig` en `MB160` mapea `EmpresaID → BaseDatos + CodigoEmpresa`.

---

## Modelo de datos ERP

### Asiste (encabezado de movimiento)
Columnas clave: `ID` (identity), `Empresa`, `Mov`, `FechaEmision`, `FechaAplicacion`, `Estatus`, `MovID`, `Usuario`, `Ejercicio`, `Periodo`.

- `Mov` = tipo de movimiento (debe existir en `movtipo` con `ConsecutivoControl` configurado para que `spAfectar` genere el folio `MovID`).
- `Estatus='SINAFECTAR'` al insertar → `spAfectar` lo cambia a `'PROCESAR'` y llena `MovID`.

### AsisteD (renglones de detalle)
Columnas clave: `ID` (FK → Asiste.ID), `Renglon` (INT, único global en la tabla), `Personal` (PersonaID del empleado), `Registro` (VARCHAR **10**), `HoraRegistro` (NCHAR 5, formato `HH:mm`), `FechaD`, `FechaA`, `Fecha`.

### movtipo (catálogo de movimientos)
```sql
SELECT * FROM cotailordev.dbo.movtipo WHERE modulo = 'asis';
```
Movimientos configurados para ASIS:
- `Entrada`
- `SalidaComida`
- `Entradacomida`  ← 'c' minúscula, así está en el catálogo
- `Salida`

Todos deben tener `ConsecutivoControl` configurado, si no, `spAfectar` falla silenciosamente (no genera `MovID`).

---

## Modelo de movimientos (nuevo, vigente)

**1 Asiste por (BaseDatos, Fecha, TipoMov)** — máximo 4 movimientos por día por empresa.

| Ventana horaria       | Asiste.Mov      | AsisteD.Registro | Condición                          |
|-----------------------|-----------------|------------------|------------------------------------|
| `< 12:00`             | `Entrada`       | `Entrada`        | Todos los marcajes                 |
| `12:00 – 12:49:59`    | —               | —                | ZONAGRIS → Descartado (Estatus=4)  |
| `12:50 – 15:59:59`    | `SalidaComida`  | `SalComida`      | 1er marcaje del empleado en el día |
| `12:50 – 15:59:59`    | `Entradacomida` | `EntComida`      | 2do marcaje del empleado en el día |
| `12:50 – 15:59:59`    | —               | —                | 3er+ → Descartado (Estatus=4)      |
| `>= 16:00`            | `Salida`        | `Salida`         | Todos los marcajes                 |

**Nota:** `AsisteD.Registro` es VARCHAR(10). `SalidaComida` y `Entradacomida` se abrevian a `SalComida` y `EntComida`.

El split 1ro/2do en ventana comida se calcula con `ROW_NUMBER() OVER (PARTITION BY BaseDatos, PersonaID, CAST(EventoFechaHora AS DATE) ORDER BY EventoFechaHora)`.

---

## sp_ProcessMarcajeQueue — flujo resumido

```sql
EXEC dbo.sp_ProcessMarcajeQueue
    @TipoCorte TINYINT = NULL,   -- 0=Entrada 4=Comida 1=Salida NULL=todo
    @BatchSize  INT    = 200
```

### Pasos internos

1. **Tomar batch** — `TOP(@BatchSize)` de `MarcajeDispatchQueue` con `READPAST + UPDLOCK + ROWLOCK`, marcar `Estatus=1`.
2. **Clasificar TipoMov** — UPDATE set-based + CTE con ROW_NUMBER.
3. **Descartar** ZONAGRIS y excedente comida → `Estatus=4`.
4. **Cursor EXTERNO** por grupo `(BaseDatos, Fecha, TipoMov)`:
   - `BEGIN TRANSACTION`
   - `INSERT Asiste` (1 por grupo) → captura `@AsisteID` via `OUTPUT INSERTED.ID`
   - **Cursor INTERNO** por marcaje del grupo (todos los empleados del día):
     - `INSERT AsisteD` (`Renglon = MAX(Renglon)+1` global, `Personal = @InnerPersonaID`)
   - `EXEC spAfectar 'ASIS', @AsisteID, 'AFECTAR', 'Todo', NULL, 'INTELISIS', @Estacion=1, @ensilencio=1`
   - Validar `MovID IS NOT NULL` post-spAfectar (con `@ensilencio=1` los fallos son silenciosos)
   - Marcar cola `Estatus=2`, `AsistenciaMarcaje.TieneMovimientos=1`
   - `COMMIT`
   - En `CATCH`: `ROLLBACK`, cerrar cursor interno si quedó abierto, marcar cola `Estatus=3`

### Variables clave del SP

```sql
-- Cursor externo (por grupo)
@GrpDB SYSNAME, @GrpEmpresaID INT, @GrpCode NVARCHAR(50),
@GrpFecha DATE, @GrpTipoMov NVARCHAR(20), @GrpRegistroCorto NVARCHAR(10)

-- Cursor interno (por marcaje, PersonaID varía fila a fila)
@InnerPersonaID INT, @QueueID BIGINT, @MarcajeID BIGINT,
@FechaEvento DATETIME2(0), @HoraStr NCHAR(5)

-- Compartidas
@SQL NVARCHAR(MAX), @Params NVARCHAR(MAX),
@AsisteID INT, @MovIDPost NVARCHAR(50), @ErrMsg NVARCHAR(4000)
```

---

## Archivos del proyecto

| Archivo | Descripción |
|---|---|
| `sql/sp_ProcessMarcajeQueue.sql` | Fuente autoritativa del SP (desplegar en MB160) |
| `sql/pilot_01_setup.sql` | Setup completo del piloto: tablas + trigger + SP inline |
| `sql/pilot_05_deploy_cotailordev.sql` | Deploy + prueba de batch para cotailordev (SP inline) |
| `sql/README.md` | Documentación del modelo, ventanas horarias, pasos de prueba |

Los archivos pilot tienen una **copia inline del SP** — cualquier cambio en `sp_ProcessMarcajeQueue.sql` debe replicarse en los otros dos.

---

## SQL Agent Jobs

Un job por ventana horaria, cada uno llama:
```sql
EXEC MB160.dbo.sp_ProcessMarcajeQueue @TipoCorte = <0|4|1>, @BatchSize = 200;
```

`@TipoCorte` filtra la cola por ventana horaria — permite procesar solo lo que ya cerró (ej. corte de entrada a las 12:00, comida a las 16:00, etc.).

---

## Gotchas / lecciones aprendidas

### spAfectar
- Con `@ensilencio=1` no lanza errores — siempre validar `MovID IS NOT NULL` después.
- Si `movtipo` no tiene `ConsecutivoControl` configurado para el `Mov`, `spAfectar` no genera `MovID` y queda `NULL` silenciosamente.
- `spAfectar` requiere que `AsisteD` ya exista ANTES de ser llamado.
- Para cancelar un movimiento ya afectado: `EXEC spAfectar 'ASIS', @ID, 'CANCELAR', 'Todo', NULL, 'INTELISIS', @Estacion=1, @ensilencio=1`.

### Fechas
- Usar formato `'yyyymmdd'` (ej. `'20260417'`) en literales de fecha — siempre inequívoco sin importar configuración regional del servidor.
- El SP usa parámetros `DATE` tipados en `sp_executesql`, así que no hay problema de formato ahí.

### AsisteD.Registro (VARCHAR 10)
- `'SalidaComida'` = 12 chars → no cabe. Se abrevia a `'SalComida'` (9).
- `'Entradacomida'` = 13 chars → no cabe. Se abrevia a `'EntComida'` (9).
- El mapeo se hace en `@GrpRegistroCorto` dentro del cursor externo.

### IDENTITY tras ROLLBACK
- Si `spAfectar` falla y se hace `ROLLBACK`, los registros de `Asiste` se borran pero el contador IDENTITY ya avanzó. Los IDs almacenados en la cola quedan "huérfanos" y no existen en `Asiste`. Es comportamiento esperado — se reintentarán en el siguiente ciclo.

### Entradacomida
- En el catálogo `movtipo` de Intelisis está guardado como `Entradacomida` (c minúscula). El SP usa ese mismo valor exacto.

### cross-DB dynamic SQL
- Todo INSERT/SELECT a tablas ERP se hace via `sp_executesql` con el nombre de BD interpolado: `[' + @GrpDB + N'].dbo.Asiste`.
- Los parámetros se pasan tipados (no concatenados) para evitar SQL injection y problemas de casting.

---

## Comandos útiles de diagnóstico

```sql
-- Estado de la cola por ventana
SELECT
    CASE
        WHEN CAST(EventoFechaHora AS TIME) < '12:00' THEN 'Entrada'
        WHEN CAST(EventoFechaHora AS TIME) < '12:50' THEN 'ZonaGris'
        WHEN CAST(EventoFechaHora AS TIME) < '16:00' THEN 'Comida'
        ELSE 'Salida'
    END AS Ventana,
    Estatus, COUNT(*) AS Total
FROM MB160.dbo.MarcajeDispatchQueue
WHERE CAST(EventoFechaHora AS DATE) = '20260417'
GROUP BY
    CASE
        WHEN CAST(EventoFechaHora AS TIME) < '12:00' THEN 'Entrada'
        WHEN CAST(EventoFechaHora AS TIME) < '12:50' THEN 'ZonaGris'
        WHEN CAST(EventoFechaHora AS TIME) < '16:00' THEN 'Comida'
        ELSE 'Salida'
    END, Estatus
ORDER BY Ventana, Estatus;

-- Movimientos generados en ERP
SELECT a.Mov, COUNT(DISTINCT a.ID) AS Asistes, COUNT(d.Renglon) AS Renglones
FROM cotailordev.dbo.Asiste a
JOIN cotailordev.dbo.AsisteD d ON d.ID = a.ID
WHERE a.FechaAplicacion = '20260417' AND a.Usuario = 'INTELISIS'
GROUP BY a.Mov;

-- Resetear cola para reintento
UPDATE MB160.dbo.MarcajeDispatchQueue
SET Estatus = 0, Intentos = 0, UltimoError = NULL, UltimoCambio = SYSDATETIME()
WHERE Estatus IN (1, 2, 3)
  AND CAST(EventoFechaHora AS DATE) = '20260417';

-- Cancelar todos los Asiste del día en cotailordev (cursor)
-- Ver: sql/context.md sección "Cancelar movimientos"
```

---

## Cancelar movimientos Asiste en ERP

```sql
USE cotailordev;
GO
DECLARE @AsisteID INT, @Cancelados INT = 0, @Errores INT = 0;

DECLARE curCancelar CURSOR LOCAL FAST_FORWARD FOR
    SELECT ID FROM dbo.Asiste
    WHERE FechaAplicacion = '20260417'
      AND Mov IN ('Entrada','SalidaComida','Entradacomida','Salida')
      AND Usuario = 'INTELISIS'
    ORDER BY ID;

OPEN curCancelar;
FETCH NEXT FROM curCancelar INTO @AsisteID;
WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        EXEC dbo.spAfectar 'ASIS', @AsisteID, 'CANCELAR', 'Todo',
            NULL, 'INTELISIS', @Estacion=1, @ensilencio=1;
        SET @Cancelados += 1;
        PRINT 'Cancelado ID=' + CAST(@AsisteID AS VARCHAR);
    END TRY
    BEGIN CATCH
        SET @Errores += 1;
        PRINT 'ERROR ID=' + CAST(@AsisteID AS VARCHAR) + ': ' + ERROR_MESSAGE();
    END CATCH;
    FETCH NEXT FROM curCancelar INTO @AsisteID;
END;
CLOSE curCancelar; DEALLOCATE curCancelar;
PRINT 'Cancelados: ' + CAST(@Cancelados AS VARCHAR) + ' | Errores: ' + CAST(@Errores AS VARCHAR);
GO
```

---

## Flujo completo de prueba (post-cleanup)

```
1. Cancelar Asiste incorrectos en ERP   → cursor spAfectar 'CANCELAR'
2. Resetear cola                         → UPDATE MarcajeDispatchQueue SET Estatus=0
3. Redesplegar SP                        → pilot_05_deploy_cotailordev.sql Sección A
4. Ejecutar SP                           → EXEC MB160.dbo.sp_ProcessMarcajeQueue @TipoCorte=NULL, @BatchSize=500
5. Verificar conteo                      → debe dar 1 fila por Mov (Entrada=1, SalidaComida=1, etc.)
6. Verificar renglones                   → SELECT Asiste JOIN AsisteD, todos los empleados como Personal
7. Verificar MovID                       → ningún MovID debe ser NULL
8. Verificar cola                        → todos los registros del día en Estatus=2
```
