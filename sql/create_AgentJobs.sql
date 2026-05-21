USE msdb;
GO

-- ============================================================
-- SQL Agent Jobs — MB160 → Intelisis
-- Un job por ventana horaria. Cada uno llama sp_ProcessMarcajeQueue
-- con el @TipoCorte correspondiente justo después de que cierra
-- la ventana, procesando todos los marcajes pendientes del corte.
--
-- Job 1 — Entrada    (< 12:00)      → corre a las 12:05
-- Job 2 — Comida     (12:50–15:59)  → corre a las 16:05
-- Job 3 — Salida     (>= 16:00)     → corre a las 21:00
-- ============================================================

-- ============================================================
-- UTILIDAD: eliminar jobs si ya existen (para re-deploy limpio)
-- ============================================================
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'MB160 - Corte Entrada')
    EXEC msdb.dbo.sp_delete_job @job_name = N'MB160 - Corte Entrada', @delete_unused_schedule = 1;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'MB160 - Corte Comida')
    EXEC msdb.dbo.sp_delete_job @job_name = N'MB160 - Corte Comida', @delete_unused_schedule = 1;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'MB160 - Corte Salida')
    EXEC msdb.dbo.sp_delete_job @job_name = N'MB160 - Corte Salida', @delete_unused_schedule = 1;
GO

-- ============================================================
-- JOB 1: Corte Entrada — corre a las 12:05 todos los días
-- ============================================================
EXEC msdb.dbo.sp_add_job
    @job_name        = N'MB160 - Corte Entrada',
    @enabled         = 1,
    @description     = N'Procesa marcajes de Entrada (< 12:00) del checador MB160 hacia Intelisis.',
    @category_name   = N'[Uncategorized (Local)]',
    @owner_login_name = N'sa';

EXEC msdb.dbo.sp_add_jobstep
    @job_name      = N'MB160 - Corte Entrada',
    @step_name     = N'Ejecutar sp_ProcessMarcajeQueue Entrada',
    @subsystem     = N'TSQL',
    @command       = N'EXEC Checador.dbo.sp_ProcessMarcajeQueue @TipoCorte = 0, @BatchSize = 500;',
    @database_name = N'Checador',
    @on_success_action = 1,  -- Quit with success
    @on_fail_action    = 2;  -- Quit with failure

EXEC msdb.dbo.sp_add_schedule
    @schedule_name      = N'Diario 12:05',
    @freq_type          = 4,      -- Daily
    @freq_interval      = 1,
    @active_start_time  = 120500; -- 12:05:00

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160 - Corte Entrada',
    @schedule_name = N'Diario 12:05';

EXEC msdb.dbo.sp_add_jobserver
    @job_name   = N'MB160 - Corte Entrada',
    @server_name = N'(local)';
GO

-- ============================================================
-- JOB 2: Corte Comida — corre a las 16:05 todos los días
-- ============================================================
EXEC msdb.dbo.sp_add_job
    @job_name        = N'MB160 - Corte Comida',
    @enabled         = 1,
    @description     = N'Procesa marcajes de SalidaComida / EntradaComida (12:50–15:59) del checador MB160 hacia Intelisis.',
    @category_name   = N'[Uncategorized (Local)]',
    @owner_login_name = N'sa';

EXEC msdb.dbo.sp_add_jobstep
    @job_name      = N'MB160 - Corte Comida',
    @step_name     = N'Ejecutar sp_ProcessMarcajeQueue Comida',
    @subsystem     = N'TSQL',
    @command       = N'EXEC Checador.dbo.sp_ProcessMarcajeQueue @TipoCorte = 4, @BatchSize = 500;',
    @database_name = N'Checador',
    @on_success_action = 1,
    @on_fail_action    = 2;

EXEC msdb.dbo.sp_add_schedule
    @schedule_name      = N'Diario 16:05',
    @freq_type          = 4,
    @freq_interval      = 1,
    @active_start_time  = 160500; -- 16:05:00

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160 - Corte Comida',
    @schedule_name = N'Diario 16:05';

EXEC msdb.dbo.sp_add_jobserver
    @job_name   = N'MB160 - Corte Comida',
    @server_name = N'(local)';
GO

-- ============================================================
-- JOB 3: Corte Salida — corre a las 21:00 todos los días
-- ============================================================
EXEC msdb.dbo.sp_add_job
    @job_name        = N'MB160 - Corte Salida',
    @enabled         = 1,
    @description     = N'Procesa marcajes de Salida (>= 16:00) del checador MB160 hacia Intelisis.',
    @category_name   = N'[Uncategorized (Local)]',
    @owner_login_name = N'sa';

EXEC msdb.dbo.sp_add_jobstep
    @job_name      = N'MB160 - Corte Salida',
    @step_name     = N'Ejecutar sp_ProcessMarcajeQueue Salida',
    @subsystem     = N'TSQL',
    @command       = N'EXEC Checador.dbo.sp_ProcessMarcajeQueue @TipoCorte = 1, @BatchSize = 500;',
    @database_name = N'Checador',
    @on_success_action = 1,
    @on_fail_action    = 2;

EXEC msdb.dbo.sp_add_schedule
    @schedule_name      = N'Diario 21:00',
    @freq_type          = 4,
    @freq_interval      = 1,
    @active_start_time  = 210000; -- 21:00:00

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'MB160 - Corte Salida',
    @schedule_name = N'Diario 21:00';

EXEC msdb.dbo.sp_add_jobserver
    @job_name   = N'MB160 - Corte Salida',
    @server_name = N'(local)';
GO

PRINT 'Jobs creados: MB160 - Corte Entrada | Comida | Salida';
GO
