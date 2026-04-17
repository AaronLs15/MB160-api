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
        EmpresaConfigID  INT            IDENTITY(1,1) NOT NULL,
        EmpresaID        INT            NOT NULL,
        EmpresaPrefix    CHAR(1)        NOT NULL,    -- primer dígito de UsuarioDispositivo: '2','3','4','5'
        BaseDatos        SYSNAME        NOT NULL,
        CodigoEmpresa    NVARCHAR(50)   NOT NULL,    -- código en Asiste.Empresa (ej. 'GNS')
        Activo           BIT            NOT NULL CONSTRAINT DF_EmpresaConfig_Activo    DEFAULT(1),
        FechaRegistro    DATETIME2(3)   NOT NULL CONSTRAINT DF_EmpresaConfig_FechaReg  DEFAULT(SYSDATETIME()),
        CONSTRAINT PK_EmpresaConfig            PRIMARY KEY CLUSTERED (EmpresaConfigID),
        CONSTRAINT UQ_EmpresaConfig_Prefix     UNIQUE (EmpresaPrefix),
        CONSTRAINT UQ_EmpresaConfig_EmpresaID  UNIQUE (EmpresaID)
    );
    PRINT '✓ Tabla EmpresaConfig creada';
END
ELSE
BEGIN
    -- Agregar CodigoEmpresa si la tabla ya existía sin esa columna
    IF NOT EXISTS (SELECT 1 FROM sys.columns
                   WHERE object_id = OBJECT_ID(N'dbo.EmpresaConfig') AND name = 'CodigoEmpresa')
    BEGIN
        ALTER TABLE dbo.EmpresaConfig
            ADD CodigoEmpresa NVARCHAR(50) NOT NULL
            CONSTRAINT DF_EmpresaConfig_CodigoEmpresa DEFAULT(N'GNS');
        ALTER TABLE dbo.EmpresaConfig DROP CONSTRAINT DF_EmpresaConfig_CodigoEmpresa;
        PRINT '✓ Columna CodigoEmpresa agregada a EmpresaConfig';
    END;
    PRINT '✓ Tabla EmpresaConfig ya existe';
END;

-- Insertar/actualizar las 4 empresas.
-- Activo=1 SOLO para cotailordev (prefijo '5').
-- CodigoEmpresa='GNS' es el valor que va en Asiste.Empresa en todos los ERPs.
MERGE dbo.EmpresaConfig AS t
USING (VALUES
    (2, '2', N'kingv7',       N'GNS', 0),
    (3, '3', N'obsidianav7',  N'GNS', 0),
    (4, '4', N'bbgv7',        N'GNS', 0),
    (5, '5', N'cotailordev',  N'GNS', 1)   -- ← ÚNICO activo en el piloto
) AS s (EmpresaID, EmpresaPrefix, BaseDatos, CodigoEmpresa, Activo)
ON t.EmpresaPrefix = s.EmpresaPrefix
WHEN MATCHED THEN
    UPDATE SET BaseDatos = s.BaseDatos, CodigoEmpresa = s.CodigoEmpresa, Activo = s.Activo
WHEN NOT MATCHED THEN
    INSERT (EmpresaID, EmpresaPrefix, BaseDatos, CodigoEmpresa, Activo)
    VALUES (s.EmpresaID, s.EmpresaPrefix, s.BaseDatos, s.CodigoEmpresa, s.Activo);

PRINT '✓ EmpresaConfig: cotailordev (5/GNS) activo, otras 3 inactivas';
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
    -- Formato: primer dígito = empresa, valor completo = Personal en AsisteD
    -- Ejemplo: '50076' → empresa '5' (cotailordev), Personal = 50076
    -- El tipo de Registro (Entrada/Comida/Salida) se determina por hora en sp_ProcessMarcajeQueue.
    -- Se encolan todos los marcajes de empresas activas sin filtrar por Punch.
    INSERT INTO dbo.MarcajeDispatchQueue
        (AsistenciaMarcajeID, EmpresaID, BaseDatos, PersonaID, Punch, EventoFechaHora)
    SELECT
        i.AsistenciaMarcajeID,
        CAST(LEFT(i.UsuarioDispositivo, 1) AS INT)   AS EmpresaID,
        ec.BaseDatos,
        CAST(i.UsuarioDispositivo AS INT)            AS PersonaID,  -- valor completo
        i.Punch,
        i.EventoFechaHora
    FROM inserted i
    INNER JOIN dbo.EmpresaConfig ec
        ON  ec.EmpresaPrefix = LEFT(i.UsuarioDispositivo, 1)
        AND ec.Activo = 1
    WHERE LEN(i.UsuarioDispositivo) >= 2
      AND ISNUMERIC(i.UsuarioDispositivo) = 1;
END;
GO
PRINT '✓ Trigger tr_AsistenciaMarcaje_DispatchQueue creado/actualizado';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. SP orquestador — modelo de 4 movimientos por empleado por día
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.sp_ProcessMarcajeQueue
    @TipoCorte TINYINT = NULL,   -- 0=Entrada (<12:00)  4=Comida (12:50-15:59)  1=Salida (>=16:00)  NULL=todos
    @BatchSize INT     = 200
AS
/*
    Modelo: 1 Asiste por (empleado, día, TipoMov). Múltiples AsisteD por Asiste.
    TipoMov: 'Entrada' | 'SalidaComida' | 'EntradaComida' | 'Salida'
    AsisteD.Registro = Asiste.Mov del encabezado.
    Cada grupo se procesa en transacción explícita (atomicidad Asiste+AsisteD+spAfectar).
    Estatus: 0=Pendiente 1=Procesando 2=Hecho 3=Error(reintentable) 4=Descartado
*/
BEGIN
    SET NOCOUNT ON;

    DECLARE @batch TABLE
    (
        MarcajeDispatchQueueID BIGINT,
        AsistenciaMarcajeID    BIGINT,
        BaseDatos              SYSNAME,
        EmpresaID              INT,
        CodigoEmpresa          NVARCHAR(50),
        PersonaID              INT,
        Punch                  TINYINT,
        EventoFechaHora        DATETIME2(0),
        TipoMov                NVARCHAR(20)
    );

    ;WITH cte AS (
        SELECT TOP (@BatchSize) q.MarcajeDispatchQueueID
        FROM dbo.MarcajeDispatchQueue q WITH (READPAST, UPDLOCK, ROWLOCK)
        WHERE q.Estatus IN (0, 3)
          AND (
              @TipoCorte IS NULL
              OR (@TipoCorte = 0 AND CAST(q.EventoFechaHora AS TIME) <  '12:00:00')
              OR (@TipoCorte = 4 AND CAST(q.EventoFechaHora AS TIME) >= '12:50:00'
                                 AND CAST(q.EventoFechaHora AS TIME) <  '16:00:00')
              OR (@TipoCorte = 1 AND CAST(q.EventoFechaHora AS TIME) >= '16:00:00')
          )
        ORDER BY q.MarcajeDispatchQueueID
    )
    UPDATE q
    SET Estatus = 1, Intentos = Intentos + 1, UltimoCambio = SYSDATETIME()
    OUTPUT inserted.MarcajeDispatchQueueID, inserted.AsistenciaMarcajeID,
           inserted.BaseDatos, inserted.EmpresaID, ec.CodigoEmpresa,
           inserted.PersonaID, inserted.Punch, inserted.EventoFechaHora, NULL
    INTO @batch
    FROM dbo.MarcajeDispatchQueue q
    INNER JOIN cte ON cte.MarcajeDispatchQueueID = q.MarcajeDispatchQueueID
    INNER JOIN dbo.EmpresaConfig ec ON ec.EmpresaID = q.EmpresaID;

    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    -- Clasificación base por ventana horaria
    UPDATE @batch SET TipoMov =
        CASE
            WHEN CAST(EventoFechaHora AS TIME) < '12:00:00' THEN N'Entrada'
            WHEN CAST(EventoFechaHora AS TIME) < '12:50:00' THEN N'ZONAGRIS'
            WHEN CAST(EventoFechaHora AS TIME) < '16:00:00' THEN N'COMIDA_TBD'
            ELSE                                                  N'Salida'
        END;

    -- Split ventana comida por posición (1ro=SalidaComida, 2do=EntradaComida, 3ro+=DESCARTAR)
    ;WITH comida AS (
        SELECT MarcajeDispatchQueueID,
               ROW_NUMBER() OVER (
                   PARTITION BY BaseDatos, PersonaID, CAST(EventoFechaHora AS DATE)
                   ORDER BY EventoFechaHora
               ) AS rn
        FROM @batch WHERE TipoMov = N'COMIDA_TBD'
    )
    UPDATE b SET b.TipoMov = CASE WHEN c.rn = 1 THEN N'SalidaComida'
                                  WHEN c.rn = 2 THEN N'Entradacomida'
                                  ELSE               N'DESCARTAR' END
    FROM @batch b INNER JOIN comida c ON c.MarcajeDispatchQueueID = b.MarcajeDispatchQueueID;

    -- Descartar zona gris y excedente comida
    UPDATE dbo.MarcajeDispatchQueue
    SET Estatus = 4,
        UltimoError = CASE b.TipoMov
            WHEN N'ZONAGRIS'  THEN N'Marcaje en zona sin categoría (12:00–12:49). No corresponde a ningún corte.'
            WHEN N'DESCARTAR' THEN N'Más de 2 registros en ventana comida (12:50–15:59) para este empleado en el día. Tercero en adelante descartado.'
        END,
        UltimoCambio = SYSDATETIME()
    FROM dbo.MarcajeDispatchQueue mdq
    INNER JOIN @batch b ON b.MarcajeDispatchQueueID = mdq.MarcajeDispatchQueueID
    WHERE b.TipoMov IN (N'ZONAGRIS', N'DESCARTAR');

    DELETE FROM @batch WHERE TipoMov IN (N'ZONAGRIS', N'DESCARTAR');
    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    DECLARE
        @GrpDB        SYSNAME,   @GrpEmpresaID INT,  @GrpCode NVARCHAR(50),
        @GrpPersonaID INT,       @GrpFecha DATE,     @GrpTipoMov NVARCHAR(20),
        @GrpRegistroCorto NVARCHAR(10),
        @QueueID      BIGINT,    @MarcajeID BIGINT,  @FechaEvento DATETIME2(0),
        @HoraStr      NCHAR(5),
        @SQL          NVARCHAR(MAX), @Params NVARCHAR(MAX),
        @AsisteID     INT,       @MovIDPost NVARCHAR(50),
        @ErrMsg       NVARCHAR(4000);

    DECLARE curGrupos CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT BaseDatos, EmpresaID, CodigoEmpresa, PersonaID,
               CAST(EventoFechaHora AS DATE), TipoMov
        FROM @batch
        ORDER BY BaseDatos, PersonaID, CAST(EventoFechaHora AS DATE), TipoMov;

    OPEN curGrupos;
    FETCH NEXT FROM curGrupos INTO @GrpDB, @GrpEmpresaID, @GrpCode, @GrpPersonaID, @GrpFecha, @GrpTipoMov;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;

            -- Mapear TipoMov → Registro corto (≤10 chars para AsisteD.Registro)
            -- Asiste.Mov siempre = 'Registro' (único con folio configurado en movtipo)
            SET @GrpRegistroCorto = CASE @GrpTipoMov
                WHEN N'Entrada'       THEN N'Entrada'
                WHEN N'SalidaComida'  THEN N'SalComida'
                WHEN N'Entradacomida' THEN N'EntComida'
                WHEN N'Salida'        THEN N'Salida'
                ELSE @GrpTipoMov END;

            -- INSERT Asiste (1 por grupo, Mov = 'Registro')
            SET @AsisteID = NULL;
            SET @SQL = N'
                DECLARE @ids TABLE (ID INT);
                INSERT INTO [' + @GrpDB + N'].dbo.Asiste
                (Empresa, Mov, FechaEmision, FechaAplicacion,
                 Estatus, Usuario, Ejercicio, Periodo, FechaRegistro,
                 Sucursal, GenerarPoliza, SincroC, SucursalOrigen,
                 Logico1, Logico2, Logico3, Logico4, Logico5, Logico6, Logico7, Logico8, Logico9)
                OUTPUT INSERTED.ID INTO @ids
                VALUES
                (@EmpresaCode, ''Registro'', @Fecha, @Fecha,
                 ''SINAFECTAR'', ''INTELISIS'', YEAR(@Fecha), MONTH(@Fecha), SYSDATETIME(),
                 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
                SELECT @AsisteID = ID FROM @ids;';
            SET @Params = N'@EmpresaCode NVARCHAR(50), @Fecha DATE, @AsisteID INT OUTPUT';
            EXEC sp_executesql @SQL, @Params,
                @EmpresaCode=@GrpCode, @Fecha=@GrpFecha, @AsisteID=@AsisteID OUTPUT;

            IF @AsisteID IS NULL
                RAISERROR(N'OUTPUT INSERTED.ID regresó NULL.', 16, 1);

            -- INSERT AsisteD: un renglón por cada marcaje del grupo
            DECLARE curMarcajes CURSOR LOCAL FAST_FORWARD FOR
                SELECT MarcajeDispatchQueueID, AsistenciaMarcajeID, EventoFechaHora
                FROM @batch
                WHERE BaseDatos = @GrpDB AND PersonaID = @GrpPersonaID
                  AND CAST(EventoFechaHora AS DATE) = @GrpFecha AND TipoMov = @GrpTipoMov
                ORDER BY EventoFechaHora;

            OPEN curMarcajes;
            FETCH NEXT FROM curMarcajes INTO @QueueID, @MarcajeID, @FechaEvento;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @HoraStr = CONVERT(NCHAR(5), CAST(@FechaEvento AS TIME), 108);
                SET @SQL = N'
                    DECLARE @renglon INT;
                    SELECT @renglon = ISNULL(MAX(Renglon), 0) + 1 FROM [' + @GrpDB + N'].dbo.AsisteD;
                    INSERT INTO [' + @GrpDB + N'].dbo.AsisteD
                    (ID, Renglon, Personal, Registro, HoraRegistro, FechaD, FechaA, Fecha, Sucursal,
                     Logico1, Logico2, Logico3, Logico4, Logico5)
                    VALUES (@AsisteID, @renglon, @PersonaID, @Registro, @Hora,
                            @Fecha, @Fecha, @Fecha, 1, 0, 0, 0, 0, 0);';
                SET @Params = N'@AsisteID INT, @PersonaID INT, @Registro NVARCHAR(10), @Hora NCHAR(5), @Fecha DATE';
                EXEC sp_executesql @SQL, @Params,
                    @AsisteID=@AsisteID, @PersonaID=@GrpPersonaID,
                    @Registro=@GrpRegistroCorto, @Hora=@HoraStr, @Fecha=@GrpFecha;

                FETCH NEXT FROM curMarcajes INTO @QueueID, @MarcajeID, @FechaEvento;
            END;
            CLOSE curMarcajes; DEALLOCATE curMarcajes;

            -- spAfectar genera MovID y pone Estatus='PROCESAR'
            SET @SQL = N'EXEC [' + @GrpDB + N'].dbo.spAfectar ''ASIS'', @AsisteID, ''AFECTAR'', ''Todo'', NULL, ''INTELISIS'', @Estacion=1, @ensilencio=1;';
            EXEC sp_executesql @SQL, N'@AsisteID INT', @AsisteID=@AsisteID;

            -- Validar MovID post-spAfectar
            SET @MovIDPost = NULL;
            SET @SQL = N'SELECT @MovIDPost = MovID FROM [' + @GrpDB + N'].dbo.Asiste WHERE ID = @AsisteID;';
            EXEC sp_executesql @SQL, N'@AsisteID INT, @MovIDPost NVARCHAR(50) OUTPUT',
                @AsisteID=@AsisteID, @MovIDPost=@MovIDPost OUTPUT;

            IF @MovIDPost IS NULL
                RAISERROR(N'spAfectar no generó MovID para Asiste.ID=%d — se reintentará en el siguiente ciclo.', 16, 1, @AsisteID);

            -- Marcar todo el grupo como Hecho
            UPDATE dbo.MarcajeDispatchQueue
            SET Estatus=2, AsisteID=@AsisteID, ProcesadoEn=SYSDATETIME(), UltimoError=NULL, UltimoCambio=SYSDATETIME()
            WHERE MarcajeDispatchQueueID IN (
                SELECT MarcajeDispatchQueueID FROM @batch
                WHERE BaseDatos=@GrpDB AND PersonaID=@GrpPersonaID
                  AND CAST(EventoFechaHora AS DATE)=@GrpFecha AND TipoMov=@GrpTipoMov);

            UPDATE dbo.AsistenciaMarcaje SET TieneMovimientos=1
            WHERE AsistenciaMarcajeID IN (
                SELECT AsistenciaMarcajeID FROM @batch
                WHERE BaseDatos=@GrpDB AND PersonaID=@GrpPersonaID
                  AND CAST(EventoFechaHora AS DATE)=@GrpFecha AND TipoMov=@GrpTipoMov);

            COMMIT TRANSACTION;

        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
            IF CURSOR_STATUS('local', 'curMarcajes') >= 0  CLOSE curMarcajes;
            IF CURSOR_STATUS('local', 'curMarcajes') >= -1 DEALLOCATE curMarcajes;
            SET @ErrMsg = ERROR_MESSAGE();
            UPDATE dbo.MarcajeDispatchQueue
            SET Estatus=3, UltimoError=LEFT(@ErrMsg,4000), UltimoCambio=SYSDATETIME()
            WHERE MarcajeDispatchQueueID IN (
                SELECT MarcajeDispatchQueueID FROM @batch
                WHERE BaseDatos=@GrpDB AND PersonaID=@GrpPersonaID
                  AND CAST(EventoFechaHora AS DATE)=@GrpFecha AND TipoMov=@GrpTipoMov);
        END CATCH;

        FETCH NEXT FROM curGrupos INTO @GrpDB, @GrpEmpresaID, @GrpCode, @GrpPersonaID, @GrpFecha, @GrpTipoMov;
    END;

    CLOSE curGrupos; DEALLOCATE curGrupos;
END;
GO
PRINT '✓ SP dbo.sp_ProcessMarcajeQueue creado/actualizado (modelo 4 movimientos por empleado por día)';
GO

PRINT '';
PRINT '══ SETUP COMPLETADO ══';
PRINT 'Siguiente paso: ejecutar pilot_02_backfill.sql para encolar el historial de cotailordev.';
GO
