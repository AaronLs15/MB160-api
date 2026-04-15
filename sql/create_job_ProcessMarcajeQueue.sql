/*
    Crea el SQL Server Agent Job que ejecuta sp_ProcessMarcajeQueue cada 5 minutos.

    PREREQUISITOS:
      - SQL Server Agent debe estar habilitado y corriendo.
      - El usuario que ejecute este script necesita ser miembro del rol
        sysadmin o SQLAgentOperatorRole en msdb.
      - La base de datos del checador debe existir y tener el SP creado.

    INSTRUCCIONES:
      Reemplaza '<NOMBRE_BASE_CHECADOR>' con el nombre real de tu base de datos
      antes de ejecutar este script (búscalo con Ctrl+H en SSMS).
*/

USE msdb;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Eliminar job anterior si existe (para poder re-ejecutar el script)
-- ─────────────────────────────────────────────────────────────────────────────
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'MB160_ProcessMarcajeQueue')
BEGIN
    EXEC msdb.dbo.sp_delete_job @job_name = N'MB160_ProcessMarcajeQueue', @delete_unused_schedule = 1;
END;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Crear el job
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_job
    @job_name        = N'MB160_ProcessMarcajeQueue',
    @description     = N'Despacha marcajes MB160 a las bases del ERP cada 5 minutos. Llama a dbo.sp_ProcessMarcajeQueue.',
    @enabled         = 1,
    @notify_level_eventlog = 2;   -- loguea en el Event Log solo en caso de error
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Agregar el step que ejecuta el SP
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_jobstep
    @job_name        = N'MB160_ProcessMarcajeQueue',
    @step_name       = N'Ejecutar sp_ProcessMarcajeQueue',
    @subsystem       = N'TSQL',
    @command         = N'EXEC dbo.sp_ProcessMarcajeQueue @BatchSize = 100;',
    @database_name   = N'<NOMBRE_BASE_CHECADOR>',
    @on_success_action = 1,       -- Quit with success
    @on_fail_action    = 2;       -- Quit with failure
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Crear el schedule (cada 5 minutos, todos los días)
--    Si ya existe un schedule con ese nombre (de una ejecución anterior),
--    se reutiliza vía sp_attach_schedule.
-- ─────────────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'MB160_Cada5Min')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'MB160_Cada5Min',
        @enabled                = 1,
        @freq_type              = 4,        -- Diario
        @freq_interval          = 1,        -- Cada 1 día
        @freq_subday_type       = 4,        -- Repetir cada N minutos
        @freq_subday_interval   = 5,        -- cada 5 minutos
        @active_start_time      = 000000,   -- 00:00:00
        @active_end_time        = 235959;   -- 23:59:59
END;
GO

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160_ProcessMarcajeQueue',
    @schedule_name = N'MB160_Cada5Min';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Asignar el job al servidor local
-- ─────────────────────────────────────────────────────────────────────────────
EXEC msdb.dbo.sp_add_jobserver
    @job_name  = N'MB160_ProcessMarcajeQueue',
    @server_name = N'(local)';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- Verificación rápida
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    j.name          AS Job,
    s.name          AS Schedule,
    s.freq_subday_interval AS CadaNMin,
    j.enabled       AS Activo
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobschedules js ON js.job_id = j.job_id
INNER JOIN msdb.dbo.sysschedules     s  ON s.schedule_id = js.schedule_id
WHERE j.name = N'MB160_ProcessMarcajeQueue';
GO
