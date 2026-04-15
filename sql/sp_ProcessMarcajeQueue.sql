CREATE OR ALTER PROCEDURE dbo.sp_ProcessMarcajeQueue
    @BatchSize INT = 50
AS
/*
    Orquestador de despacho de marcajes MB160 → ERPs (misma instancia SQL Server).

    Por cada registro pendiente en MarcajeDispatchQueue:
      1. Obtiene el código de empresa desde la DB destino (SELECT TOP 1 Empresa FROM Empresa).
      2. Inserta el encabezado en [BaseDatos].dbo.Asiste (Estatus='SIN AFECTAR').
      3. Inserta el detalle en [BaseDatos].dbo.AsisteD.
      4. Llama a [BaseDatos].dbo.spAfectar para afectar el movimiento.
      5. Marca el registro como Hecho (Estatus=2) y actualiza TieneMovimientos en AsistenciaMarcaje.

    En caso de error por registro, lo marca como Error (Estatus=3) con el mensaje en UltimoError.
    Los registros en Estatus=3 son reintentados en la siguiente ejecución del job.

    Punch válidos:
      0 → Entrada
      1 → Salida
      4 → Comida
*/
BEGIN
    SET NOCOUNT ON;

    -- -------------------------------------------------------------------------
    -- 1. Tomar batch y marcarlo como Procesando (READPAST evita bloqueos entre
    --    ejecuciones concurrentes del job si el intervalo es muy corto).
    -- -------------------------------------------------------------------------
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
        WHERE Estatus IN (0, 3)          -- Pendiente o Error (reintento)
        ORDER BY MarcajeDispatchQueueID
    )
    UPDATE q
    SET
        Estatus      = 1,                -- Procesando
        Intentos     = Intentos + 1,
        UltimoCambio = SYSDATETIME()
    OUTPUT
        inserted.MarcajeDispatchQueueID,
        inserted.AsistenciaMarcajeID,
        inserted.BaseDatos,
        inserted.EmpresaID,
        inserted.PersonaID,
        inserted.Punch,
        inserted.EventoFechaHora
    INTO @batch
    FROM dbo.MarcajeDispatchQueue q
    INNER JOIN cte ON cte.MarcajeDispatchQueueID = q.MarcajeDispatchQueueID;

    -- Salir si no hay nada que procesar
    IF NOT EXISTS (SELECT 1 FROM @batch)
        RETURN;

    -- -------------------------------------------------------------------------
    -- 2. Iterar registro a registro (cursor local, fast-forward).
    --    Se hace individualmente para capturar el error por fila.
    -- -------------------------------------------------------------------------
    DECLARE
        @QueueID      BIGINT,
        @MarcajeID    BIGINT,
        @DB           SYSNAME,
        @EmpresaID    INT,
        @PersonaID    INT,
        @Punch        TINYINT,
        @FechaEvento  DATETIME2(0),

        @SQL          NVARCHAR(MAX),
        @Params       NVARCHAR(MAX),

        @AsisteID     INT,
        @EmpresaCode  NVARCHAR(50),
        @TipoRegistro NVARCHAR(20),
        @HoraStr      NCHAR(5),
        @ErrMsg       NVARCHAR(4000);

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            MarcajeDispatchQueueID,
            AsistenciaMarcajeID,
            BaseDatos,
            EmpresaID,
            PersonaID,
            Punch,
            EventoFechaHora
        FROM @batch
        ORDER BY MarcajeDispatchQueueID;

    OPEN cur;
    FETCH NEXT FROM cur INTO
        @QueueID, @MarcajeID, @DB, @EmpresaID,
        @PersonaID, @Punch, @FechaEvento;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY

            -- Tipo de registro según Punch
            SET @TipoRegistro = CASE @Punch
                WHEN 0 THEN N'Entrada'
                WHEN 1 THEN N'Salida'
                WHEN 4 THEN N'Comida'
            END;

            -- Hora en formato HH:mm (campo HoraRegistro de AsisteD)
            SET @HoraStr = CONVERT(NCHAR(5), CAST(@FechaEvento AS TIME), 108);

            -- ------------------------------------------------------------------
            -- 2a. Obtener código de empresa desde la DB destino
            -- ------------------------------------------------------------------
            SET @SQL    = N'SELECT TOP 1 @Code = Empresa FROM [' + @DB + N'].dbo.Empresa;';
            SET @Params = N'@Code NVARCHAR(50) OUTPUT';
            EXEC sp_executesql @SQL, @Params, @Code = @EmpresaCode OUTPUT;

            -- ------------------------------------------------------------------
            -- 2b. INSERT en Asiste (encabezado del movimiento)
            -- ------------------------------------------------------------------
            SET @SQL = N'
                INSERT INTO [' + @DB + N'].dbo.Asiste
                (
                    Empresa, Mov, MovID,
                    FechaEmision, FechaAplicacion,
                    Estatus, Usuario,
                    Ejercicio, Periodo,
                    FechaRegistro,
                    Sucursal, GenerarPoliza,
                    SincroC, SucursalOrigen,
                    Logico1, Logico2, Logico3, Logico4, Logico5,
                    Logico6, Logico7, Logico8, Logico9
                )
                VALUES
                (
                    @EmpresaCode, ''Registro'', ''AVC1'',
                    CAST(@Fecha AS DATE), CAST(@Fecha AS DATE),
                    ''SIN AFECTAR'', ''INTELISIS'',
                    YEAR(@Fecha), MONTH(@Fecha),
                    SYSDATETIME(),
                    1, 0,
                    1, 1,
                    0, 0, 0, 0, 0,
                    0, 0, 0, 0
                );
                SET @AsisteID = SCOPE_IDENTITY();';

            SET @Params = N'@EmpresaCode NVARCHAR(50), @Fecha DATETIME2(0), @AsisteID INT OUTPUT';
            EXEC sp_executesql @SQL, @Params,
                @EmpresaCode = @EmpresaCode,
                @Fecha       = @FechaEvento,
                @AsisteID    = @AsisteID OUTPUT;

            -- ------------------------------------------------------------------
            -- 2c. INSERT en AsisteD (detalle / renglón del movimiento)
            --     Renglon es IDENTITY en el ERP, no se especifica.
            -- ------------------------------------------------------------------
            SET @SQL = N'
                INSERT INTO [' + @DB + N'].dbo.AsisteD
                (
                    ID,
                    Personal, Registro, HoraRegistro,
                    FechaD, FechaA, Fecha,
                    Sucursal,
                    Logico1, Logico2, Logico3, Logico4, Logico5
                )
                VALUES
                (
                    @AsisteID,
                    @PersonaID, @TipoReg, @Hora,
                    CAST(@Fecha AS DATE), CAST(@Fecha AS DATE), CAST(@Fecha AS DATE),
                    1,
                    0, 0, 0, 0, 0
                );';

            SET @Params = N'@AsisteID INT, @PersonaID INT, @TipoReg NVARCHAR(20),
                            @Hora NCHAR(5), @Fecha DATETIME2(0)';
            EXEC sp_executesql @SQL, @Params,
                @AsisteID  = @AsisteID,
                @PersonaID = @PersonaID,
                @TipoReg   = @TipoRegistro,
                @Hora      = @HoraStr,
                @Fecha     = @FechaEvento;

            -- ------------------------------------------------------------------
            -- 2d. Afectar el movimiento en el ERP
            -- ------------------------------------------------------------------
            SET @SQL = N'
                EXEC [' + @DB + N'].dbo.spAfectar
                    ''ASIS'', @AsisteID, ''AFECTAR'', ''Todo'',
                    NULL, ''INTELISIS'',
                    @Estacion = 1, @ensilencio = 1;';

            SET @Params = N'@AsisteID INT';
            EXEC sp_executesql @SQL, @Params,
                @AsisteID = @AsisteID;

            -- ------------------------------------------------------------------
            -- 2e. Marcar como Hecho en la cola
            -- ------------------------------------------------------------------
            UPDATE dbo.MarcajeDispatchQueue
            SET
                Estatus      = 2,               -- Hecho
                AsisteID     = @AsisteID,
                ProcesadoEn  = SYSDATETIME(),
                UltimoError  = NULL,
                UltimoCambio = SYSDATETIME()
            WHERE MarcajeDispatchQueueID = @QueueID;

            -- ------------------------------------------------------------------
            -- 2f. Marcar el marcaje original como procesado
            -- ------------------------------------------------------------------
            UPDATE dbo.AsistenciaMarcaje
            SET TieneMovimientos = 1
            WHERE AsistenciaMarcajeID = @MarcajeID;

        END TRY
        BEGIN CATCH

            SET @ErrMsg = ERROR_MESSAGE();

            UPDATE dbo.MarcajeDispatchQueue
            SET
                Estatus      = 3,               -- Error (reintentable)
                UltimoError  = LEFT(@ErrMsg, 4000),
                UltimoCambio = SYSDATETIME()
            WHERE MarcajeDispatchQueueID = @QueueID;

        END CATCH;

        FETCH NEXT FROM cur INTO
            @QueueID, @MarcajeID, @DB, @EmpresaID,
            @PersonaID, @Punch, @FechaEvento;
    END;

    CLOSE cur;
    DEALLOCATE cur;
END;
GO
