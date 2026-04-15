/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRODUCCIÓN — PASO 1: ACTIVAR TODAS LAS EMPRESAS                            ║
║                                                                              ║
║  Sección A: verifica acceso a los 4 ERPs antes de activar                   ║
║  Sección B: actualiza EmpresaConfig → 4 empresas Activo=1                   ║
║  Sección C: muestra el estado final de EmpresaConfig                        ║
║                                                                              ║
║  ⚠ Ejecutar PRIMERO. Si algún pre-check falla, detener y corregir          ║
║    permisos/nombres antes de continuar.                                      ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

USE Checador;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- SECCIÓN A — Pre-checks: verificar acceso a los 4 ERPs
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── Sección A: verificando acceso a los 4 ERPs ──';
GO

-- kingv7
BEGIN TRY
    SELECT TOP 1 'kingv7 Asiste OK'  AS Check1 FROM kingv7.dbo.Asiste;
    SELECT TOP 1 'kingv7 AsisteD OK' AS Check2 FROM kingv7.dbo.AsisteD;
    IF OBJECT_ID(N'kingv7.dbo.spAfectar', N'P') IS NULL
        PRINT '⚠ ADVERTENCIA: spAfectar no encontrado en kingv7';
    ELSE
        PRINT '✓ kingv7 — Asiste, AsisteD y spAfectar accesibles';
END TRY
BEGIN CATCH
    PRINT '✗ ERROR en kingv7: ' + ERROR_MESSAGE();
    PRINT '  → Verificar permisos y nombre de base antes de continuar.';
END CATCH;
GO

-- obsidianav7
BEGIN TRY
    SELECT TOP 1 'obsidianav7 Asiste OK'  AS Check1 FROM obsidianav7.dbo.Asiste;
    SELECT TOP 1 'obsidianav7 AsisteD OK' AS Check2 FROM obsidianav7.dbo.AsisteD;
    IF OBJECT_ID(N'obsidianav7.dbo.spAfectar', N'P') IS NULL
        PRINT '⚠ ADVERTENCIA: spAfectar no encontrado en obsidianav7';
    ELSE
        PRINT '✓ obsidianav7 — Asiste, AsisteD y spAfectar accesibles';
END TRY
BEGIN CATCH
    PRINT '✗ ERROR en obsidianav7: ' + ERROR_MESSAGE();
    PRINT '  → Verificar permisos y nombre de base antes de continuar.';
END CATCH;
GO

-- bbgv7
BEGIN TRY
    SELECT TOP 1 'bbgv7 Asiste OK'  AS Check1 FROM bbgv7.dbo.Asiste;
    SELECT TOP 1 'bbgv7 AsisteD OK' AS Check2 FROM bbgv7.dbo.AsisteD;
    IF OBJECT_ID(N'bbgv7.dbo.spAfectar', N'P') IS NULL
        PRINT '⚠ ADVERTENCIA: spAfectar no encontrado en bbgv7';
    ELSE
        PRINT '✓ bbgv7 — Asiste, AsisteD y spAfectar accesibles';
END TRY
BEGIN CATCH
    PRINT '✗ ERROR en bbgv7: ' + ERROR_MESSAGE();
    PRINT '  → Verificar permisos y nombre de base antes de continuar.';
END CATCH;
GO

-- cotailor7000
BEGIN TRY
    SELECT TOP 1 'cotailor7000 Asiste OK'  AS Check1 FROM cotailor7000.dbo.Asiste;
    SELECT TOP 1 'cotailor7000 AsisteD OK' AS Check2 FROM cotailor7000.dbo.AsisteD;
    IF OBJECT_ID(N'cotailor7000.dbo.spAfectar', N'P') IS NULL
        PRINT '⚠ ADVERTENCIA: spAfectar no encontrado en cotailor7000';
    ELSE
        PRINT '✓ cotailor7000 — Asiste, AsisteD y spAfectar accesibles';
END TRY
BEGIN CATCH
    PRINT '✗ ERROR en cotailor7000: ' + ERROR_MESSAGE();
    PRINT '  → Verificar permisos y nombre de base antes de continuar.';
END CATCH;
GO

PRINT '';
PRINT '⚠ Revisar los resultados anteriores. Si algún ERP muestra ERROR,';
PRINT '  corregir antes de ejecutar la Sección B.';
PRINT '';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- SECCIÓN B — Activar las 4 empresas en EmpresaConfig
--             cotailor7000 reemplaza cotailordev (base de producción)
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── Sección B: actualizando EmpresaConfig ──';

MERGE dbo.EmpresaConfig AS t
USING (VALUES
    (2, '2', N'kingv7',        N'GNS', 1),
    (3, '3', N'obsidianav7',   N'GNS', 1),
    (4, '4', N'bbgv7',         N'GNS', 1),
    (5, '5', N'cotailor7000',  N'GNS', 1)   -- producción (reemplaza cotailordev del piloto)
) AS s (EmpresaID, EmpresaPrefix, BaseDatos, CodigoEmpresa, Activo)
ON t.EmpresaPrefix = s.EmpresaPrefix
WHEN MATCHED THEN
    UPDATE SET
        BaseDatos     = s.BaseDatos,
        CodigoEmpresa = s.CodigoEmpresa,
        Activo        = s.Activo
WHEN NOT MATCHED BY TARGET THEN
    INSERT (EmpresaID, EmpresaPrefix, BaseDatos, CodigoEmpresa, Activo)
    VALUES (s.EmpresaID, s.EmpresaPrefix, s.BaseDatos, s.CodigoEmpresa, s.Activo);

PRINT '✓ EmpresaConfig actualizado — 4 empresas activadas';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- SECCIÓN C — Verificación final
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── Sección C: estado final de EmpresaConfig ──';

SELECT
    EmpresaID,
    EmpresaPrefix,
    BaseDatos,
    CodigoEmpresa,
    CASE Activo WHEN 1 THEN '✓ Activo' ELSE '✗ Inactivo' END AS Estado
FROM dbo.EmpresaConfig
ORDER BY EmpresaID;
GO

PRINT '';
PRINT '══ Si las 4 filas muestran "✓ Activo", continuar con prod_02_backfill_abril.sql ══';
GO
