/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  PASO 2 — BACKFILL HISTÓRICO COTAILORDEV (prefijo 005)                      ║
║  Encola en MarcajeDispatchQueue los registros que ya existían en            ║
║  AsistenciaMarcaje ANTES de instalar el trigger.                            ║
║                                                                              ║
║  ⚠ EJECUTAR DESPUÉS de pilot_01_setup.sql                                   ║
║  ⚠ Ajusta @FechaDesde si quieres limitar el rango de fechas.               ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

USE <NOMBRE_BASE_CHECADOR>;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- PARÁMETROS AJUSTABLES
-- ─────────────────────────────────────────────────────────────────────────────
DECLARE
    -- Fecha desde la que encolar registros.
    -- Recomendación para el piloto: los últimos 7 días para validar rápido.
    -- Cuando pase a producción usar la fecha real de inicio de operaciones.
    @FechaDesde DATETIME2(0) = '2026-03-01 00:00:00',

    -- Fecha hasta (NULL = hasta hoy inclusive)
    @FechaHasta DATETIME2(0) = NULL;   -- NULL = SYSDATETIME()

SET @FechaHasta = ISNULL(@FechaHasta, SYSDATETIME());

-- ─────────────────────────────────────────────────────────────────────────────
-- Cuántos registros hay en el rango ANTES de encolar (info)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    Punch,
    CASE Punch WHEN 0 THEN 'Entrada' WHEN 1 THEN 'Salida' WHEN 4 THEN 'Comida' ELSE 'Ignorado' END AS Tipo,
    COUNT(*) AS Total
FROM dbo.AsistenciaMarcaje
WHERE LEFT(UsuarioDispositivo, 3) = '005'
  AND LEN(UsuarioDispositivo)    = 9
  AND EventoFechaHora BETWEEN @FechaDesde AND @FechaHasta
GROUP BY Punch
ORDER BY Punch;

-- ─────────────────────────────────────────────────────────────────────────────
-- INSERT en la cola — solo los que NO están ya encolados (por si se re-ejecuta)
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO dbo.MarcajeDispatchQueue
    (AsistenciaMarcajeID, EmpresaID, BaseDatos, PersonaID, Punch, EventoFechaHora)
SELECT
    am.AsistenciaMarcajeID,
    CAST(LEFT(am.UsuarioDispositivo, 3) AS INT)  AS EmpresaID,
    ec.BaseDatos,
    CAST(RIGHT(am.UsuarioDispositivo, 6) AS INT) AS PersonaID,
    am.Punch,
    am.EventoFechaHora
FROM dbo.AsistenciaMarcaje am
INNER JOIN dbo.EmpresaConfig ec
    ON  ec.EmpresaPrefix = LEFT(am.UsuarioDispositivo, 3)
    AND ec.Activo        = 1
LEFT JOIN dbo.MarcajeDispatchQueue q
    ON q.AsistenciaMarcajeID = am.AsistenciaMarcajeID
WHERE am.Punch IN (0, 1, 4)
  AND LEN(am.UsuarioDispositivo)  = 9
  AND am.EventoFechaHora BETWEEN @FechaDesde AND @FechaHasta
  AND q.MarcajeDispatchQueueID IS NULL;  -- no duplicar los que ya estén en cola

PRINT CONCAT('Registros encolados: ', @@ROWCOUNT);
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- Resumen de lo que quedó en cola listo para despachar
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    Punch,
    CASE Punch WHEN 0 THEN 'Entrada' WHEN 1 THEN 'Salida' WHEN 4 THEN 'Comida' END AS Tipo,
    COUNT(*) AS EnCola,
    MIN(EventoFechaHora) AS PrimerRegistro,
    MAX(EventoFechaHora) AS UltimoRegistro
FROM dbo.MarcajeDispatchQueue
WHERE Estatus = 0   -- Pendiente
  AND BaseDatos = N'cotailordev'
GROUP BY Punch
ORDER BY Punch;

PRINT '';
PRINT '══ BACKFILL COMPLETADO ══';
PRINT 'Siguiente paso: ejecutar manualmente el SP con un batch pequeño para validar:';
PRINT '  EXEC dbo.sp_ProcessMarcajeQueue @Punch = 0, @BatchSize = 5;';
PRINT 'Luego revisar cotailordev.dbo.Asiste y AsisteD antes de continuar.';
GO
