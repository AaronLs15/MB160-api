/*
    Crea los SQL Server Agent Jobs de corte de asistencia MB160.

    Según "Reglas de reloj checador" (Regla 6 y 9):
      - 12:00      → Corte de Entrada  (TipoCorte = 0 → registros < 12:00)
      - 16:00      → Corte de Comida   (TipoCorte = 4 → registros 12:50–15:59)
      - 23:00      → Corte de Salida   (TipoCorte = 1 → registros >= 16:00)
      - Martes     → Corte Semanal     (procesa cualquier pendiente rezagado de la semana)

    Nota: la clasificación Entrada/Comida/Salida se basa en la hora del evento,
    no en el valor Punch del dispositivo. Ver sp_ProcessMarcajeQueue.

    PREREQUISITOS:
      - SQL Server Agent habilitado y corriendo.
      - El usuario necesita ser miembro de sysadmin o SQLAgentOperatorRole en msdb.
      - La base del checador debe existir con el SP dbo.sp_ProcessMarcajeQueue creado.

    INSTRUCCIONES:
      Reemplaza '<NOMBRE_BASE_CHECADOR>' con el nombre real de tu base de datos
      antes de ejecutar (Ctrl+H en SSMS para reemplazar todas las ocurrencias).
*/

USE msdb;
GO

-- =============================================================================
-- Helper: elimina un job anterior si existe (permite re-ejecutar el script)
-- =============================================================================
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

-- =============================================================================
-- Schedules reutilizables (se crean solo si no existen)
-- =============================================================================

-- 12:00 diario
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

-- 16:00 diario
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

-- 23:00 diario
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

-- Martes a las 23:30 (después del cierre de salida — Regla 9)
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'MB160_Semanal_Martes_2330')
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name        = N'MB160_Semanal_Martes_2330',
        @enabled              = 1,
        @freq_type            = 8,        -- Semanal
        @freq_interval        = 4,        -- Martes (bit 2 = 4)
        @freq_recurrence_factor = 1,      -- Cada 1 semana
        @freq_subday_type     = 1,
        @freq_subday_interval = 0,
        @active_start_time    = 233000;   -- 23:30:00
GO

-- =============================================================================
-- JOB 1: Corte de Entrada — 12:00 diario (Regla 6)
-- =============================================================================
EXEC msdb.dbo.sp_add_job
    @job_name              = N'MB160_Corte_Entrada',
    @description           = N'Corte de entrada 12:00. Registros < 12:00 → Registro=Entrada. Regla 6.',
    @enabled               = 1,
    @notify_level_eventlog = 2;   -- log solo en error

EXEC msdb.dbo.sp_add_jobstep
    @job_name            = N'MB160_Corte_Entrada',
    @step_name           = N'Procesar TipoCorte=0 Entrada',
    @subsystem           = N'TSQL',
    @command             = N'EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 0, @BatchSize = 500;',
    @database_name       = N'<NOMBRE_BASE_CHECADOR>',
    @on_success_action   = 1,
    @on_fail_action      = 2;

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160_Corte_Entrada',
    @schedule_name = N'MB160_Diario_1200';

EXEC msdb.dbo.sp_add_jobserver
    @job_name    = N'MB160_Corte_Entrada',
    @server_name = N'(local)';
GO

-- =============================================================================
-- JOB 2: Corte de Comida — 16:00 diario (Regla 6)
--         El SP descarta automáticamente los marcajes fuera de 12:50–16:10 (Regla 3)
-- =============================================================================
EXEC msdb.dbo.sp_add_job
    @job_name              = N'MB160_Corte_Comida',
    @description           = N'Corte de comida 16:00. Registros 12:50–15:59 → Registro=Comida. Reglas 3 y 6.',
    @enabled               = 1,
    @notify_level_eventlog = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_name            = N'MB160_Corte_Comida',
    @step_name           = N'Procesar TipoCorte=4 Comida',
    @subsystem           = N'TSQL',
    @command             = N'EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 4, @BatchSize = 500;',
    @database_name       = N'<NOMBRE_BASE_CHECADOR>',
    @on_success_action   = 1,
    @on_fail_action      = 2;

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160_Corte_Comida',
    @schedule_name = N'MB160_Diario_1600';

EXEC msdb.dbo.sp_add_jobserver
    @job_name    = N'MB160_Corte_Comida',
    @server_name = N'(local)';
GO

-- =============================================================================
-- JOB 3: Corte de Salida — 23:00 diario (Regla 6)
-- =============================================================================
EXEC msdb.dbo.sp_add_job
    @job_name              = N'MB160_Corte_Salida',
    @description           = N'Corte de salida 23:00. Registros >= 16:00 → Registro=Salida. Regla 6.',
    @enabled               = 1,
    @notify_level_eventlog = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_name            = N'MB160_Corte_Salida',
    @step_name           = N'Procesar TipoCorte=1 Salida',
    @subsystem           = N'TSQL',
    @command             = N'EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 1, @BatchSize = 500;',
    @database_name       = N'<NOMBRE_BASE_CHECADOR>',
    @on_success_action   = 1,
    @on_fail_action      = 2;

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160_Corte_Salida',
    @schedule_name = N'MB160_Diario_2300';

EXEC msdb.dbo.sp_add_jobserver
    @job_name    = N'MB160_Corte_Salida',
    @server_name = N'(local)';
GO

-- =============================================================================
-- JOB 4: Corte Semanal — Martes 23:30 (Regla 9)
--         Procesa cualquier pendiente/error rezagado de la semana (todos los tipos).
-- =============================================================================
EXEC msdb.dbo.sp_add_job
    @job_name              = N'MB160_Corte_Semanal',
    @description           = N'Corte semanal martes 23:30. Limpia pendientes rezagados de la semana (todos los tipos). Regla 9.',
    @enabled               = 1,
    @notify_level_eventlog = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_name            = N'MB160_Corte_Semanal',
    @step_name           = N'Procesar todos los pendientes de la semana',
    @subsystem           = N'TSQL',
    @command             = N'EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = NULL, @BatchSize = 1000;',
    @database_name       = N'<NOMBRE_BASE_CHECADOR>',
    @on_success_action   = 1,
    @on_fail_action      = 2;

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160_Corte_Semanal',
    @schedule_name = N'MB160_Semanal_Martes_2330';

EXEC msdb.dbo.sp_add_jobserver
    @job_name    = N'MB160_Corte_Semanal',
    @server_name = N'(local)';
GO

-- =============================================================================
-- Verificación rápida
-- =============================================================================
SELECT
    j.name          AS Job,
    s.name          AS Schedule,
    s.active_start_time AS HoraInicio,
    CASE s.freq_type WHEN 4 THEN 'Diario' WHEN 8 THEN 'Semanal' END AS Frecuencia,
    j.enabled       AS Activo
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobschedules js ON js.job_id = j.job_id
INNER JOIN msdb.dbo.sysschedules     s  ON s.schedule_id = js.schedule_id
WHERE j.name LIKE N'MB160_%'
ORDER BY s.active_start_time;
GO
