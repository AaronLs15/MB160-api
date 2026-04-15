/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRODUCCIÓN — PASO 2: BACKFILL DESDE 2026-04-01 (todas las empresas)        ║
║                                                                              ║
║  Encola en MarcajeDispatchQueue los registros de AsistenciaMarcaje          ║
║  desde el 1 de abril para las 4 empresas activas.                           ║
║                                                                              ║
║  ⚠ Ejecutar DESPUÉS de prod_01_activar_empresas.sql                         ║
║  ⚠ La protección anti-duplicados evita volver a encolar lo ya encolado      ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

USE Checador;
GO

DECLARE
    @FechaDesde DATETIME2(0) = '2026-04-01 00:00:00',
    @FechaHasta DATETIME2(0) = NULL;

SET @FechaHasta = ISNULL(@FechaHasta, SYSDATETIME());

-- ─────────────────────────────────────────────────────────────────────────────
-- Vista previa: cuántos registros hay en el rango por empresa y ventana horaria
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── Registros en AsistenciaMarcaje por empresa y ventana horaria (ANTES de encolar) ──';

SELECT
    ec.BaseDatos                                           AS Empresa,
    CASE
        WHEN CAST(am.EventoFechaHora AS TIME) <  '12:00:00' THEN '0  Entrada  (< 12:00)'
        WHEN CAST(am.EventoFechaHora AS TIME) <  '12:50:00' THEN '!  Zona gris (12:00–12:49)'
        WHEN CAST(am.EventoFechaHora AS TIME) <  '16:00:00' THEN '4  Comida   (12:50–15:59)'
        ELSE                                                     '1  Salida   (>= 16:00)'
    END                                                    AS VentanaHoraria,
    COUNT(*)                                               AS Total
FROM dbo.AsistenciaMarcaje am
INNER JOIN dbo.EmpresaConfig ec
    ON  ec.EmpresaPrefix = LEFT(am.UsuarioDispositivo, 1)
    AND ec.Activo        = 1
WHERE LEN(am.UsuarioDispositivo)    >= 2
  AND ISNUMERIC(am.UsuarioDispositivo) = 1
  AND am.EventoFechaHora BETWEEN @FechaDesde AND @FechaHasta
GROUP BY
    ec.BaseDatos,
    CASE
        WHEN CAST(am.EventoFechaHora AS TIME) <  '12:00:00' THEN '0  Entrada  (< 12:00)'
        WHEN CAST(am.EventoFechaHora AS TIME) <  '12:50:00' THEN '!  Zona gris (12:00–12:49)'
        WHEN CAST(am.EventoFechaHora AS TIME) <  '16:00:00' THEN '4  Comida   (12:50–15:59)'
        ELSE                                                     '1  Salida   (>= 16:00)'
    END
ORDER BY ec.BaseDatos, VentanaHoraria;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- INSERT en la cola — protección anti-duplicados incluida
-- ─────────────────────────────────────────────────────────────────────────────
DECLARE
    @FechaDesde DATETIME2(0) = '2026-04-01 00:00:00',
    @FechaHasta DATETIME2(0) = NULL;

SET @FechaHasta = ISNULL(@FechaHasta, SYSDATETIME());

INSERT INTO dbo.MarcajeDispatchQueue
    (AsistenciaMarcajeID, EmpresaID, BaseDatos, PersonaID, Punch, EventoFechaHora)
SELECT
    am.AsistenciaMarcajeID,
    CAST(LEFT(am.UsuarioDispositivo, 1) AS INT)   AS EmpresaID,
    ec.BaseDatos,
    CAST(am.UsuarioDispositivo AS INT)            AS PersonaID,  -- valor completo
    am.Punch,
    am.EventoFechaHora
FROM dbo.AsistenciaMarcaje am
INNER JOIN dbo.EmpresaConfig ec
    ON  ec.EmpresaPrefix = LEFT(am.UsuarioDispositivo, 1)
    AND ec.Activo        = 1
LEFT JOIN dbo.MarcajeDispatchQueue q
    ON q.AsistenciaMarcajeID = am.AsistenciaMarcajeID
WHERE LEN(am.UsuarioDispositivo)    >= 2
  AND ISNUMERIC(am.UsuarioDispositivo) = 1
  AND am.EventoFechaHora BETWEEN @FechaDesde AND @FechaHasta
  AND q.MarcajeDispatchQueueID IS NULL;   -- no duplicar los ya encolados

PRINT CONCAT('Registros encolados: ', @@ROWCOUNT);
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- Resumen post-INSERT: pendientes en cola por empresa y ventana horaria
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── Pendientes en cola por empresa y ventana horaria (listos para despachar) ──';

SELECT
    BaseDatos                                              AS Empresa,
    CASE
        WHEN CAST(EventoFechaHora AS TIME) <  '12:00:00' THEN '0  Entrada  (< 12:00)'
        WHEN CAST(EventoFechaHora AS TIME) <  '12:50:00' THEN '!  Zona gris (12:00–12:49) → Descartado'
        WHEN CAST(EventoFechaHora AS TIME) <  '16:00:00' THEN '4  Comida   (12:50–15:59)'
        ELSE                                                   '1  Salida   (>= 16:00)'
    END                                                    AS VentanaHoraria,
    COUNT(*)                                               AS EnCola,
    MIN(EventoFechaHora)                                   AS PrimerRegistro,
    MAX(EventoFechaHora)                                   AS UltimoRegistro
FROM dbo.MarcajeDispatchQueue
WHERE Estatus = 0   -- Pendiente
GROUP BY
    BaseDatos,
    CASE
        WHEN CAST(EventoFechaHora AS TIME) <  '12:00:00' THEN '0  Entrada  (< 12:00)'
        WHEN CAST(EventoFechaHora AS TIME) <  '12:50:00' THEN '!  Zona gris (12:00–12:49) → Descartado'
        WHEN CAST(EventoFechaHora AS TIME) <  '16:00:00' THEN '4  Comida   (12:50–15:59)'
        ELSE                                                   '1  Salida   (>= 16:00)'
    END
ORDER BY BaseDatos, VentanaHoraria;
GO

PRINT '';
PRINT '══ BACKFILL COMPLETADO ══';
PRINT 'Siguiente: probar con batch pequeño antes de activar los jobs:';
PRINT '  EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 0, @BatchSize = 10;';
PRINT '  EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 4, @BatchSize = 10;';
PRINT '  EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 1, @BatchSize = 10;';
PRINT 'Verificar en los 4 ERPs. Si todo OK → prod_03_jobs_produccion.sql';
GO
