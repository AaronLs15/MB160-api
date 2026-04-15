/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  PASO 1 — SETUP PILOTO COTAILORDEV                                          ║
║  Instala todos los objetos en la base del CHECADOR.                         ║
║  Solo cotailordev queda activo; las otras 3 empresas se insertan            ║
║  con Activo=0 para no tocar producción.                                     ║
║                                                                              ║
║  EJECUTAR en la base de datos del checador (no en msdb ni en el ERP).       ║
║  Reemplaza <NOMBRE_BASE_CHECADOR> con el nombre real antes de correr.       ║
╚══════════════════════════════════════════════════════════════════════════════╝

ORDEN DE EJECUCIÓN:
  1. pilot_01_setup.sql         ← este archivo
  2. pilot_02_backfill.sql
  3. (manual) EXEC dbo.sp_ProcessMarcajeQueue con batch pequeño y revisar
  4. pilot_03_verificacion.sql
  5. pilot_04_jobs_cotailordev.sql  (solo si paso 3 y 4 están bien)
*/

USE <NOMBRE_BASE_CHECADOR>;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- PRE-CHECKS: verificar que cotailordev es accesible y tiene los objetos ERP
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── Verificando acceso a cotailordev ──';

-- Si alguna de estas líneas falla, detener y revisar permisos / nombre de DB
SELECT TOP 1 'Asiste OK' AS Objeto FROM cotailordev.dbo.Asiste;
SELECT TOP 1 'AsisteD OK' AS Objeto FROM cotailordev.dbo.AsisteD;
SELECT TOP 1 'Empresa OK' AS Objeto FROM cotailordev.dbo.Empresa;
SELECT TOP 1 Empresa AS CodigoEmpresa FROM cotailordev.dbo.Empresa;  -- anotar el valor

IF OBJECT_ID('cotailordev.dbo.spAfectar', 'P') IS NULL
    PRINT '⚠ ADVERTENCIA: spAfectar no encontrado en cotailordev.dbo — verificar nombre exacto';
ELSE
    PRINT '✓ spAfectar encontrado en cotailordev';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. EmpresaConfig — solo cotailordev activo
-- ─────────────────────────────────────────────────────────────────────────────
IF OBJECT_ID(N'dbo.EmpresaConfig', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.EmpresaConfig
    (
        EmpresaConfigID  INT          IDENTITY(1,1) NOT NULL,
        EmpresaID        INT          NOT NULL,
        EmpresaPrefix    CHAR(3)      NOT NULL,
        BaseDatos        SYSNAME      NOT NULL,
        Activo           BIT          NOT NULL CONSTRAINT DF_EmpresaConfig_Activo    DEFAULT(1),
        FechaRegistro    DATETIME2(3) NOT NULL CONSTRAINT DF_EmpresaConfig_FechaReg  DEFAULT(SYSDATETIME()),
        CONSTRAINT PK_EmpresaConfig            PRIMARY KEY CLUSTERED (EmpresaConfigID),
        CONSTRAINT UQ_EmpresaConfig_Prefix     UNIQUE (EmpresaPrefix),
        CONSTRAINT UQ_EmpresaConfig_EmpresaID  UNIQUE (EmpresaID)
    );
    PRINT '✓ Tabla EmpresaConfig creada';
END
ELSE
    PRINT '✓ Tabla EmpresaConfig ya existe';

-- Insertar/actualizar las 4 empresas.
-- Activo=1 SOLO para cotailordev (prefijo 005).
-- Las otras 3 se agregan con Activo=0 para tenerlas listas cuando pase a producción.
MERGE dbo.EmpresaConfig AS t
USING (VALUES
    (2, '002', N'kingv7',       0),
    (3, '003', N'obsidianav7',  0),
    (4, '004', N'bbgv7',        0),
    (5, '005', N'cotailordev',  1)   -- ← ÚNICO activo en el piloto
) AS s (EmpresaID, EmpresaPrefix, BaseDatos, Activo)
ON t.EmpresaPrefix = s.EmpresaPrefix
WHEN MATCHED THEN
    UPDATE SET BaseDatos = s.BaseDatos, Activo = s.Activo
WHEN NOT MATCHED THEN
    INSERT (EmpresaID, EmpresaPrefix, BaseDatos, Activo)
    VALUES (s.EmpresaID, s.EmpresaPrefix, s.BaseDatos, s.Activo);

PRINT '✓ EmpresaConfig: cotailordev (005) activo, otras 3 inactivas';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. MarcajeDispatchQueue
-- ─────────────────────────────────────────────────────────────────────────────
IF OBJECT_ID(N'dbo.MarcajeDispatchQueue', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.MarcajeDispatchQueue
    (
        MarcajeDispatchQueueID  BIGINT         IDENTITY(1,1) NOT NULL,
        AsistenciaMarcajeID     BIGINT         NOT NULL,
        EmpresaID               INT            NOT NULL,
        BaseDatos               SYSNAME        NOT NULL,
        PersonaID               INT            NOT NULL,
        Punch                   TINYINT        NOT NULL,
        EventoFechaHora         DATETIME2(0)   NOT NULL,
        Estatus                 TINYINT        NOT NULL CONSTRAINT DF_MDQ_Estatus       DEFAULT(0),
        -- 0=Pendiente, 1=Procesando, 2=Hecho, 3=Error, 4=Descartado
        Intentos                INT            NOT NULL CONSTRAINT DF_MDQ_Intentos      DEFAULT(0),
        UltimoError             NVARCHAR(4000) NULL,
        AsisteID                INT            NULL,
        FechaRegistro           DATETIME2(3)   NOT NULL CONSTRAINT DF_MDQ_FechaRegistro DEFAULT(SYSDATETIME()),
        UltimoCambio            DATETIME2(3)   NOT NULL CONSTRAINT DF_MDQ_UltimoCambio  DEFAULT(SYSDATETIME()),
        ProcesadoEn             DATETIME2(3)   NULL,
        CONSTRAINT PK_MarcajeDispatchQueue         PRIMARY KEY CLUSTERED (MarcajeDispatchQueueID),
        CONSTRAINT UQ_MarcajeDispatchQueue_Marcaje UNIQUE (AsistenciaMarcajeID)
    );
    CREATE INDEX IX_MDQ_Estatus ON dbo.MarcajeDispatchQueue (Estatus, MarcajeDispatchQueueID);
    PRINT '✓ Tabla MarcajeDispatchQueue creada';
END
ELSE
    PRINT '✓ Tabla MarcajeDispatchQueue ya existe';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Trigger en AsistenciaMarcaje (nuevos registros → cola automática)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR ALTER TRIGGER dbo.tr_AsistenciaMarcaje_DispatchQueue
ON dbo.AsistenciaMarcaje
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.MarcajeDispatchQueue
        (AsistenciaMarcajeID, EmpresaID, BaseDatos, PersonaID, Punch, EventoFechaHora)
    SELECT
        i.AsistenciaMarcajeID,
        CAST(LEFT(i.UsuarioDispositivo, 3) AS INT)  AS EmpresaID,
        ec.BaseDatos,
        CAST(RIGHT(i.UsuarioDispositivo, 6) AS INT) AS PersonaID,
        i.Punch,
        i.EventoFechaHora
    FROM inserted i
    INNER JOIN dbo.EmpresaConfig ec
        ON  ec.EmpresaPrefix = LEFT(i.UsuarioDispositivo, 3)
        AND ec.Activo = 1                    -- solo empresas activas
    WHERE i.Punch IN (0, 1, 4)
      AND LEN(i.UsuarioDispositivo) = 9;
END;
GO
PRINT '✓ Trigger tr_AsistenciaMarcaje_DispatchQueue creado/actualizado';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. SP orquestador
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.sp_ProcessMarcajeQueue
    @Punch     TINYINT = NULL,
    @BatchSize INT     = 200
AS
/*
    Despacha marcajes pendientes en MarcajeDispatchQueue hacia los ERPs.
    Llamado por los jobs de corte (Regla 6) y el corte semanal (Regla 9).

    @Punch = 0  → Entrada  (job 12:00)
    @Punch = 1  → Salida   (job 23:00)
    @Punch = 4  → Comida   (job 16:00, ventana válida 12:50–16:10 — Regla 3)
    @Punch = NULL → todos  (corte semanal / ejecución manual)

    Estatus: 0=Pendiente 1=Procesando 2=Hecho 3=Error 4=Descartado
*/
BEGIN
    SET NOCOUNT ON;

    DECLARE @batch TABLE
    (
        MarcajeDispatchQueueID BIGINT,
        AsistenciaMarcajeID    BIGINT,
        BaseDatos              SYSNAME,
        EmpresaID              INT,
        PersonaID              INT,
        Punch                  TINYINT,
        EventoFechaHora        DATETIME2(0)
    );

    ;WITH cte AS (
        SELECT TOP (@BatchSize) MarcajeDispatchQueueID
        FROM dbo.MarcajeDispatchQueue WITH (READPAST, UPDLOCK, ROWLOCK)
        WHERE Estatus IN (0, 3)
          AND (@Punch IS NULL OR Punch = @Punch)
        ORDER BY MarcajeDispatchQueueID
    )
    UPDATE q
    SET Estatus = 1, Intentos = Intentos + 1, UltimoCambio = SYSDATETIME()
    OUTPUT inserted.MarcajeDispatchQueueID, inserted.AsistenciaMarcajeID,
           inserted.BaseDatos, inserted.EmpresaID, inserted.PersonaID,
           inserted.Punch, inserted.EventoFechaHora
    INTO @batch
    FROM dbo.MarcajeDispatchQueue q
    INNER JOIN cte ON cte.MarcajeDispatchQueueID = q.MarcajeDispatchQueueID;

    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    -- Descartar Comida fuera de ventana horaria (Regla 3: 12:50–16:10)
    UPDATE dbo.MarcajeDispatchQueue
    SET Estatus = 4,
        UltimoError = N'Comida fuera de ventana horaria permitida (12:50–16:10). Regla 3.',
        UltimoCambio = SYSDATETIME()
    WHERE MarcajeDispatchQueueID IN (
        SELECT MarcajeDispatchQueueID FROM @batch
        WHERE Punch = 4
          AND (CAST(EventoFechaHora AS TIME) < '12:50:00'
            OR CAST(EventoFechaHora AS TIME) > '16:10:00')
    );

    DELETE FROM @batch
    WHERE Punch = 4
      AND (CAST(EventoFechaHora AS TIME) < '12:50:00'
        OR CAST(EventoFechaHora AS TIME) > '16:10:00');

    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    DECLARE
        @QueueID      BIGINT, @MarcajeID BIGINT, @DB SYSNAME,
        @EmpresaID    INT,    @PersonaID INT,    @PunchVal TINYINT,
        @FechaEvento  DATETIME2(0),
        @SQL          NVARCHAR(MAX), @Params NVARCHAR(MAX),
        @AsisteID     INT,   @EmpresaCode NVARCHAR(50),
        @TipoRegistro NVARCHAR(20), @HoraStr NCHAR(5),
        @ErrMsg       NVARCHAR(4000);

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT MarcajeDispatchQueueID, AsistenciaMarcajeID, BaseDatos,
               EmpresaID, PersonaID, Punch, EventoFechaHora
        FROM @batch ORDER BY MarcajeDispatchQueueID;

    OPEN cur;
    FETCH NEXT FROM cur INTO @QueueID, @MarcajeID, @DB, @EmpresaID, @PersonaID, @PunchVal, @FechaEvento;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            SET @TipoRegistro = CASE @PunchVal WHEN 0 THEN N'Entrada' WHEN 1 THEN N'Salida' WHEN 4 THEN N'Comida' END;
            SET @HoraStr      = CONVERT(NCHAR(5), CAST(@FechaEvento AS TIME), 108);

            SET @SQL    = N'SELECT TOP 1 @Code = Empresa FROM [' + @DB + N'].dbo.Empresa;';
            SET @Params = N'@Code NVARCHAR(50) OUTPUT';
            EXEC sp_executesql @SQL, @Params, @Code = @EmpresaCode OUTPUT;

            SET @SQL = N'
                INSERT INTO [' + @DB + N'].dbo.Asiste
                (Empresa, Mov, MovID, FechaEmision, FechaAplicacion,
                 Estatus, Usuario, Ejercicio, Periodo, FechaRegistro,
                 Sucursal, GenerarPoliza, SincroC, SucursalOrigen,
                 Logico1, Logico2, Logico3, Logico4, Logico5, Logico6, Logico7, Logico8, Logico9)
                VALUES
                (@EmpresaCode, ''Registro'', ''AVC1'', CAST(@Fecha AS DATE), CAST(@Fecha AS DATE),
                 ''SIN AFECTAR'', ''INTELISIS'', YEAR(@Fecha), MONTH(@Fecha), SYSDATETIME(),
                 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
                SET @AsisteID = SCOPE_IDENTITY();';
            SET @Params = N'@EmpresaCode NVARCHAR(50), @Fecha DATETIME2(0), @AsisteID INT OUTPUT';
            EXEC sp_executesql @SQL, @Params, @EmpresaCode=@EmpresaCode, @Fecha=@FechaEvento, @AsisteID=@AsisteID OUTPUT;

            SET @SQL = N'
                INSERT INTO [' + @DB + N'].dbo.AsisteD
                (ID, Personal, Registro, HoraRegistro, FechaD, FechaA, Fecha, Sucursal,
                 Logico1, Logico2, Logico3, Logico4, Logico5)
                VALUES
                (@AsisteID, @PersonaID, @TipoReg, @Hora,
                 CAST(@Fecha AS DATE), CAST(@Fecha AS DATE), CAST(@Fecha AS DATE), 1,
                 0, 0, 0, 0, 0);';
            SET @Params = N'@AsisteID INT, @PersonaID INT, @TipoReg NVARCHAR(20), @Hora NCHAR(5), @Fecha DATETIME2(0)';
            EXEC sp_executesql @SQL, @Params, @AsisteID=@AsisteID, @PersonaID=@PersonaID, @TipoReg=@TipoRegistro, @Hora=@HoraStr, @Fecha=@FechaEvento;

            SET @SQL = N'EXEC [' + @DB + N'].dbo.spAfectar ''ASIS'', @AsisteID, ''AFECTAR'', ''Todo'', NULL, ''INTELISIS'', @Estacion=1, @ensilencio=1;';
            EXEC sp_executesql @SQL, N'@AsisteID INT', @AsisteID=@AsisteID;

            UPDATE dbo.MarcajeDispatchQueue
            SET Estatus=2, AsisteID=@AsisteID, ProcesadoEn=SYSDATETIME(), UltimoError=NULL, UltimoCambio=SYSDATETIME()
            WHERE MarcajeDispatchQueueID = @QueueID;

            UPDATE dbo.AsistenciaMarcaje SET TieneMovimientos=1 WHERE AsistenciaMarcajeID=@MarcajeID;

        END TRY
        BEGIN CATCH
            SET @ErrMsg = ERROR_MESSAGE();
            UPDATE dbo.MarcajeDispatchQueue
            SET Estatus=3, UltimoError=LEFT(@ErrMsg,4000), UltimoCambio=SYSDATETIME()
            WHERE MarcajeDispatchQueueID=@QueueID;
        END CATCH;

        FETCH NEXT FROM cur INTO @QueueID, @MarcajeID, @DB, @EmpresaID, @PersonaID, @PunchVal, @FechaEvento;
    END;

    CLOSE cur; DEALLOCATE cur;
END;
GO
PRINT '✓ SP dbo.sp_ProcessMarcajeQueue creado/actualizado';
GO

PRINT '';
PRINT '══ SETUP COMPLETADO ══';
PRINT 'Siguiente paso: ejecutar pilot_02_backfill.sql para encolar el historial de cotailordev.';
GO
