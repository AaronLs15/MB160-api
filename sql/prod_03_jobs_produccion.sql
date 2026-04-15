/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRODUCCIÓN — PASO 3: CREAR SQL SERVER AGENT JOBS                           ║
║                                                                              ║
║  Crea los 4 jobs de corte de asistencia apuntando a la base 'Checador'.    ║
║  Requiere permisos de sysadmin o SQLAgentOperatorRole en msdb.              ║
║                                                                              ║
║  Jobs creados:                                                               ║
║    MB160_Corte_Entrada  — 12:00 diario  → @TipoCorte=0  (< 12:00)          ║
║    MB160_Corte_Comida   — 16:00 diario  → @TipoCorte=4  (12:50–15:59)      ║
║    MB160_Corte_Salida   — 23:00 diario  → @TipoCorte=1  (>= 16:00)         ║
║    MB160_Corte_Semanal  — Martes 23:30  → @TipoCorte=NULL (todos)           ║
║                                                                              ║
║  ⚠ Ejecutar ÚLTIMO, después de validar con batches pequeños                 ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

USE msdb;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- Eliminar jobs anteriores si existen (permite re-ejecutar el script)
-- ─────────────────────────────────────────────────────────────────────────────
DECLARE @jobs TABLE (name SYSNAME);
INSERT INTO @jobs VALUES
    (N'MB160_Corte_Entrada'),
    (N'MB160_Corte_Comida'),
    (N'MB160_Corte_Salida'),
    (N'MB160_Corte_Semanal');

DECLARE @jname SYSNAME;
DECLARE jc CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM @jobs;
OPEN jc; FETCH NEXT FROM jc INTO @jname;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @jname)
    BEGIN
        EXEC msdb.dbo.sp_delete_job @job_name = @jname, @delete_unused_schedule = 0;
        PRINT 'Job eliminado: ' + @jname;
    END;
    FETCH NEXT FROM jc INTO @jname;
END;
CLOSE jc; DEALLOCATE jc;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- Schedules (crear solo si no existen — son reutilizables entre re-ejecuciones)
-- ─────────────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'MB160_Diario_1200')
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name        = N'MB160_Diario_1200',
        @enabled              = 1,
        @freq_type            = 4,        -- Diario
        @freq_interval        = 1,
        @freq_subday_type     = 1,        -- Una vez al día
        @freq_subday_interval = 0,
        @active_start_time    = 120000;   -- 12:00:00
GO

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'MB160_Diario_1600')
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name        = N'MB160_Diario_1600',
        @enabled              = 1,
        @freq_type            = 4,
        @freq_interval        = 1,
        @freq_subday_type     = 1,
        @freq_subday_interval = 0,
        @active_start_time    = 160000;   -- 16:00:00
GO

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'MB160_Diario_2300')
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name        = N'MB160_Diario_2300',
        @enabled              = 1,
        @freq_type            = 4,
        @freq_interval        = 1,
        @freq_subday_type     = 1,
        @freq_subday_interval = 0,
        @active_start_time    = 230000;   -- 23:00:00
GO

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'MB160_Semanal_Martes_2330')
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'MB160_Semanal_Martes_2330',
        @enabled                = 1,
        @freq_type              = 8,      -- Semanal
        @freq_interval          = 4,      -- Martes (bit 2 = 4)
        @freq_recurrence_factor = 1,      -- Cada 1 semana
        @freq_subday_type       = 1,
        @freq_subday_interval   = 0,
        @active_start_time      = 233000; -- 23:30:00
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- JOB 1: Corte de Entrada — 12:00 diario
--         Procesa marcajes con hora de evento < 12:00 → Registro = 'Entrada'
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_job
    @job_name              = N'MB160_Corte_Entrada',
    @description           = N'Corte de Entrada 12:00. Registros < 12:00 → Entrada. Regla 6.',
    @enabled               = 1,
    @notify_level_eventlog = 2;   -- log solo en error

EXEC msdb.dbo.sp_add_jobstep
    @job_name          = N'MB160_Corte_Entrada',
    @step_name         = N'Procesar TipoCorte=0 Entrada',
    @subsystem         = N'TSQL',
    @command           = N'EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 0, @BatchSize = 500;',
    @database_name     = N'Checador',
    @on_success_action = 1,
    @on_fail_action    = 2;

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160_Corte_Entrada',
    @schedule_name = N'MB160_Diario_1200';

EXEC msdb.dbo.sp_add_jobserver
    @job_name    = N'MB160_Corte_Entrada',
    @server_name = N'(local)';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- JOB 2: Corte de Comida — 16:00 diario
--         Procesa marcajes 12:50–15:59 → Registro = 'Comida'
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_job
    @job_name              = N'MB160_Corte_Comida',
    @description           = N'Corte de Comida 16:00. Registros 12:50–15:59 → Comida. Reglas 3 y 6.',
    @enabled               = 1,
    @notify_level_eventlog = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_name          = N'MB160_Corte_Comida',
    @step_name         = N'Procesar TipoCorte=4 Comida',
    @subsystem         = N'TSQL',
    @command           = N'EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 4, @BatchSize = 500;',
    @database_name     = N'Checador',
    @on_success_action = 1,
    @on_fail_action    = 2;

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160_Corte_Comida',
    @schedule_name = N'MB160_Diario_1600';

EXEC msdb.dbo.sp_add_jobserver
    @job_name    = N'MB160_Corte_Comida',
    @server_name = N'(local)';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- JOB 3: Corte de Salida — 23:00 diario
--         Procesa marcajes >= 16:00 → Registro = 'Salida'
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_job
    @job_name              = N'MB160_Corte_Salida',
    @description           = N'Corte de Salida 23:00. Registros >= 16:00 → Salida. Regla 6.',
    @enabled               = 1,
    @notify_level_eventlog = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_name          = N'MB160_Corte_Salida',
    @step_name         = N'Procesar TipoCorte=1 Salida',
    @subsystem         = N'TSQL',
    @command           = N'EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 1, @BatchSize = 500;',
    @database_name     = N'Checador',
    @on_success_action = 1,
    @on_fail_action    = 2;

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160_Corte_Salida',
    @schedule_name = N'MB160_Diario_2300';

EXEC msdb.dbo.sp_add_jobserver
    @job_name    = N'MB160_Corte_Salida',
    @server_name = N'(local)';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- JOB 4: Corte Semanal — Martes 23:30
--         Limpia cualquier pendiente rezagado de la semana (todos los tipos)
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_job
    @job_name              = N'MB160_Corte_Semanal',
    @description           = N'Corte semanal martes 23:30. Limpia pendientes rezagados (todos los tipos). Regla 9.',
    @enabled               = 1,
    @notify_level_eventlog = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_name          = N'MB160_Corte_Semanal',
    @step_name         = N'Procesar todos los pendientes de la semana',
    @subsystem         = N'TSQL',
    @command           = N'EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = NULL, @BatchSize = 1000;',
    @database_name     = N'Checador',
    @on_success_action = 1,
    @on_fail_action    = 2;

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160_Corte_Semanal',
    @schedule_name = N'MB160_Semanal_Martes_2330';

EXEC msdb.dbo.sp_add_jobserver
    @job_name    = N'MB160_Corte_Semanal',
    @server_name = N'(local)';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- Verificación: listar los 4 jobs con su schedule
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── Jobs creados ──';

SELECT
    j.name              AS Job,
    j.enabled           AS Activo,
    s.name              AS Schedule,
    s.active_start_time AS HoraInicio,
    CASE s.freq_type
        WHEN 4 THEN 'Diario'
        WHEN 8 THEN 'Semanal'
    END                 AS Frecuencia
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobschedules js ON js.job_id   = j.job_id
INNER JOIN msdb.dbo.sysschedules     s  ON s.schedule_id = js.schedule_id
WHERE j.name LIKE N'MB160_%'
ORDER BY s.active_start_time;
GO

PRINT '';
PRINT '══ JOBS CREADOS Y ACTIVOS ══';
PRINT 'Los jobs correrán automáticamente en los horarios configurados.';
PRINT 'Para ejecutar manualmente: clic derecho en el job en SSMS → Start Job at Step.';
GO
