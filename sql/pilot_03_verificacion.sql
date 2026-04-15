/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  PASO 3 — VERIFICACIÓN POST-PROCESO                                         ║
║  Ejecutar después de correr el SP manualmente o después de un ciclo de job. ║
║  Revisa que los datos llegaron correctamente a cotailordev.                 ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

USE <NOMBRE_BASE_CHECADOR>;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. ESTADO GENERAL DE LA COLA
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── 1. Estado de la cola ──';

SELECT
    Estatus,
    CASE Estatus
        WHEN 0 THEN 'Pendiente'
        WHEN 1 THEN 'Procesando'
        WHEN 2 THEN 'Hecho'
        WHEN 3 THEN 'Error'
        WHEN 4 THEN 'Descartado (fuera de ventana)'
    END AS Descripcion,
    COUNT(*) AS Total
FROM dbo.MarcajeDispatchQueue
WHERE BaseDatos = N'cotailordev'
GROUP BY Estatus
ORDER BY Estatus;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. DETALLE DE ERRORES (si hay Estatus=3)
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── 2. Errores (Estatus=3) ──';

SELECT TOP 20
    MarcajeDispatchQueueID,
    PersonaID,
    Punch,
    EventoFechaHora,
    Intentos,
    UltimoError
FROM dbo.MarcajeDispatchQueue
WHERE Estatus = 3
  AND BaseDatos = N'cotailordev'
ORDER BY MarcajeDispatchQueueID;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. REGISTROS PROCESADOS HOY EN COTAILORDEV (cruzado con AsisteD del ERP)
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── 3. Registros procesados hoy en cotailordev ──';

SELECT
    q.AsisteID,
    q.PersonaID,
    q.Punch,
    CASE q.Punch WHEN 0 THEN 'Entrada' WHEN 1 THEN 'Salida' WHEN 4 THEN 'Comida' END AS Tipo,
    q.EventoFechaHora,
    q.ProcesadoEn
FROM dbo.MarcajeDispatchQueue q
WHERE q.Estatus    = 2
  AND q.BaseDatos  = N'cotailordev'
  AND CAST(q.ProcesadoEn AS DATE) = CAST(SYSDATETIME() AS DATE)
ORDER BY q.EventoFechaHora;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. VERIFICAR EN COTAILORDEV — últimos 20 registros de Asiste
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── 4. Últimos 20 en cotailordev.dbo.Asiste ──';

SELECT TOP 20
    a.ID,
    a.Empresa,
    a.Mov,
    a.MovID,
    a.FechaEmision,
    a.Estatus,
    a.Usuario,
    a.FechaRegistro
FROM cotailordev.dbo.Asiste a
WHERE a.Mov = 'Registro'
  AND a.MovID = 'AVC1'
  AND a.Usuario = 'INTELISIS'
ORDER BY a.ID DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. VERIFICAR EN COTAILORDEV — últimas 20 líneas de AsisteD
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── 5. Últimas 20 en cotailordev.dbo.AsisteD ──';

SELECT TOP 20
    d.ID,
    d.Personal,
    d.Registro,
    d.HoraRegistro,
    d.Fecha,
    d.Sucursal
FROM cotailordev.dbo.AsisteD d
INNER JOIN cotailordev.dbo.Asiste a ON a.ID = d.ID
WHERE a.MovID   = 'AVC1'
  AND a.Usuario = 'INTELISIS'
ORDER BY d.ID DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. CRUCE CHECADOR ↔ ERP (validar tipo esperado por hora vs insertado)
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── 6. Cruce Checador ↔ cotailordev ──';

SELECT
    am.UsuarioDispositivo,
    am.EventoFechaHora                              AS FechaHoraChecador,
    CAST(am.EventoFechaHora AS TIME)                AS HoraEvento,
    am.Punch                                        AS PunchDispositivo,
    CASE
        WHEN CAST(am.EventoFechaHora AS TIME) <  '12:00:00' THEN 'Entrada'
        WHEN CAST(am.EventoFechaHora AS TIME) <  '12:50:00' THEN '(zona gris)'
        WHEN CAST(am.EventoFechaHora AS TIME) <  '16:00:00' THEN 'Comida'
        ELSE                                                      'Salida'
    END                                             AS TipoEsperado,
    d.Registro                                      AS TipoInsertado,
    CASE
        WHEN d.Registro IS NULL THEN '⚠ Sin registro'
        WHEN d.Registro = CASE
                WHEN CAST(am.EventoFechaHora AS TIME) <  '12:00:00' THEN 'Entrada'
                WHEN CAST(am.EventoFechaHora AS TIME) <  '12:50:00' THEN '(zona gris)'
                WHEN CAST(am.EventoFechaHora AS TIME) <  '16:00:00' THEN 'Comida'
                ELSE 'Salida'
             END                                    THEN 'OK'
        ELSE                                             '*** MISMATCH ***'
    END                                             AS Resultado,
    d.Personal                                      AS PersonalERP,
    d.HoraRegistro                                  AS HoraERP,
    d.Fecha                                         AS FechaERP,
    q.AsisteID
FROM dbo.MarcajeDispatchQueue q
INNER JOIN dbo.AsistenciaMarcaje am ON am.AsistenciaMarcajeID = q.AsistenciaMarcajeID
LEFT JOIN cotailordev.dbo.AsisteD d ON d.ID = q.AsisteID
WHERE q.Estatus   = 2
  AND q.BaseDatos = N'cotailordev'
  AND CAST(q.ProcesadoEn AS DATE) = CAST(SYSDATETIME() AS DATE)
ORDER BY am.EventoFechaHora;

-- ─────────────────────────────────────────────────────────────────────────────
-- 7. PENDIENTES RESTANTES (para saber cuánto falta)
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── 7. Pendientes restantes por ventana horaria ──';

SELECT
    CASE
        WHEN CAST(EventoFechaHora AS TIME) <  '12:00:00' THEN '0  Entrada  (< 12:00)'
        WHEN CAST(EventoFechaHora AS TIME) <  '12:50:00' THEN '!  Zona gris (12:00–12:49) → Descartado'
        WHEN CAST(EventoFechaHora AS TIME) <  '16:00:00' THEN '4  Comida   (12:50–15:59)'
        ELSE                                                   '1  Salida   (>= 16:00)'
    END                      AS VentanaHoraria,
    COUNT(*)                 AS Pendientes,
    MIN(EventoFechaHora)     AS MasAntiguo,
    MAX(EventoFechaHora)     AS MasReciente
FROM dbo.MarcajeDispatchQueue
WHERE Estatus IN (0, 3)
  AND BaseDatos = N'cotailordev'
GROUP BY
    CASE
        WHEN CAST(EventoFechaHora AS TIME) <  '12:00:00' THEN '0  Entrada  (< 12:00)'
        WHEN CAST(EventoFechaHora AS TIME) <  '12:50:00' THEN '!  Zona gris (12:00–12:49) → Descartado'
        WHEN CAST(EventoFechaHora AS TIME) <  '16:00:00' THEN '4  Comida   (12:50–15:59)'
        ELSE                                                   '1  Salida   (>= 16:00)'
    END
ORDER BY VentanaHoraria;
GO
