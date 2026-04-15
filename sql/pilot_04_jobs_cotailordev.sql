/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  PASO 4 — JOBS PILOTO COTAILORDEV                                           ║
║  Crea los 4 jobs de corte en SQL Server Agent.                              ║
║  Ejecutar SOLO después de validar que pilot_03_verificacion.sql muestra      ║
║  datos correctos en cotailordev.                                            ║
║                                                                              ║
║  Reemplaza <NOMBRE_BASE_CHECADOR> con el nombre real antes de ejecutar.     ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

USE msdb;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- Limpiar jobs anteriores si existen (permite re-ejecutar)
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
        EXEC msdb.dbo.sp_delete_job @job_name = @jname, @delete_unused_schedule = 0;
    FETCH NEXT FROM jc INTO @jname;
END;
CLOSE jc; DEALLOCATE jc;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- Schedules (crear solo si no existen)
-- ─────────────────────────────────────────────────────────────────────────────

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'MB160_Diario_1200')
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name = N'MB160_Diario_1200', @enabled = 1,
        @freq_type = 4, @freq_interval = 1, @freq_subday_type = 1,
        @freq_subday_interval = 0, @active_start_time = 120000;
GO

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'MB160_Diario_1600')
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name = N'MB160_Diario_1600', @enabled = 1,
        @freq_type = 4, @freq_interval = 1, @freq_subday_type = 1,
        @freq_subday_interval = 0, @active_start_time = 160000;
GO

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'MB160_Diario_2300')
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name = N'MB160_Diario_2300', @enabled = 1,
        @freq_type = 4, @freq_interval = 1, @freq_subday_type = 1,
        @freq_subday_interval = 0, @active_start_time = 230000;
GO

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'MB160_Semanal_Martes_2330')
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name = N'MB160_Semanal_Martes_2330', @enabled = 1,
        @freq_type = 8, @freq_interval = 4,      -- Martes
        @freq_recurrence_factor = 1,
        @freq_subday_type = 1, @freq_subday_interval = 0,
        @active_start_time = 233000;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- JOB 1 — Corte de Entrada 12:00 (Regla 6)
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_job
    @job_name = N'MB160_Corte_Entrada',
    @description = N'Corte de Entrada 12:00 — Punch=0. Regla 6.',
    @enabled = 1, @notify_level_eventlog = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_name = N'MB160_Corte_Entrada', @step_name = N'Procesar Entrada',
    @subsystem = N'TSQL',
    @command = N'EXEC dbo.sp_ProcessMarcajeQueue @Punch = 0, @BatchSize = 500;',
    @database_name = N'<NOMBRE_BASE_CHECADOR>',
    @on_success_action = 1, @on_fail_action = 2;

EXEC msdb.dbo.sp_attach_schedule @job_name = N'MB160_Corte_Entrada', @schedule_name = N'MB160_Diario_1200';
EXEC msdb.dbo.sp_add_jobserver   @job_name = N'MB160_Corte_Entrada', @server_name = N'(local)';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- JOB 2 — Corte de Comida 16:00 (Reglas 3 y 6)
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_job
    @job_name = N'MB160_Corte_Comida',
    @description = N'Corte de Comida 16:00 — Punch=4, ventana 12:50-16:10. Reglas 3 y 6.',
    @enabled = 1, @notify_level_eventlog = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_name = N'MB160_Corte_Comida', @step_name = N'Procesar Comida',
    @subsystem = N'TSQL',
    @command = N'EXEC dbo.sp_ProcessMarcajeQueue @Punch = 4, @BatchSize = 500;',
    @database_name = N'<NOMBRE_BASE_CHECADOR>',
    @on_success_action = 1, @on_fail_action = 2;

EXEC msdb.dbo.sp_attach_schedule @job_name = N'MB160_Corte_Comida', @schedule_name = N'MB160_Diario_1600';
EXEC msdb.dbo.sp_add_jobserver   @job_name = N'MB160_Corte_Comida', @server_name = N'(local)';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- JOB 3 — Corte de Salida 23:00 (Regla 6)
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_job
    @job_name = N'MB160_Corte_Salida',
    @description = N'Corte de Salida 23:00 — Punch=1. Regla 6.',
    @enabled = 1, @notify_level_eventlog = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_name = N'MB160_Corte_Salida', @step_name = N'Procesar Salida',
    @subsystem = N'TSQL',
    @command = N'EXEC dbo.sp_ProcessMarcajeQueue @Punch = 1, @BatchSize = 500;',
    @database_name = N'<NOMBRE_BASE_CHECADOR>',
    @on_success_action = 1, @on_fail_action = 2;

EXEC msdb.dbo.sp_attach_schedule @job_name = N'MB160_Corte_Salida', @schedule_name = N'MB160_Diario_2300';
EXEC msdb.dbo.sp_add_jobserver   @job_name = N'MB160_Corte_Salida', @server_name = N'(local)';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- JOB 4 — Corte Semanal martes 23:30 (Regla 9)
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_job
    @job_name = N'MB160_Corte_Semanal',
    @description = N'Corte semanal martes 23:30 — limpia pendientes rezagados. Regla 9.',
    @enabled = 1, @notify_level_eventlog = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_name = N'MB160_Corte_Semanal', @step_name = N'Procesar pendientes semanal',
    @subsystem = N'TSQL',
    @command = N'EXEC dbo.sp_ProcessMarcajeQueue @Punch = NULL, @BatchSize = 1000;',
    @database_name = N'<NOMBRE_BASE_CHECADOR>',
    @on_success_action = 1, @on_fail_action = 2;

EXEC msdb.dbo.sp_attach_schedule @job_name = N'MB160_Corte_Semanal', @schedule_name = N'MB160_Semanal_Martes_2330';
EXEC msdb.dbo.sp_add_jobserver   @job_name = N'MB160_Corte_Semanal', @server_name = N'(local)';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- Verificación: listar los jobs creados
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    j.name          AS Job,
    j.enabled       AS Activo,
    s.name          AS Schedule,
    s.active_start_time AS HoraInicio,
    CASE s.freq_type WHEN 4 THEN 'Diario' WHEN 8 THEN 'Semanal' END AS Frecuencia
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobschedules js ON js.job_id = j.job_id
INNER JOIN msdb.dbo.sysschedules     s  ON s.schedule_id = js.schedule_id
WHERE j.name LIKE N'MB160_%'
ORDER BY s.active_start_time;
GO

PRINT '';
PRINT '══ JOBS CREADOS ══';
PRINT 'Los jobs están activos y correrán en los horarios configurados.';
PRINT 'Para ejecutar manualmente desde SSMS: clic derecho en el job → Start Job at Step.';
GO
