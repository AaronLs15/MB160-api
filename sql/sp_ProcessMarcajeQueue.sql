CREATE OR ALTER PROCEDURE dbo.sp_ProcessMarcajeQueue
    @Punch     TINYINT = NULL,   -- Filtro de tipo de corte: 0=Entrada, 1=Salida, 4=Comida. NULL = todos.
    @BatchSize INT     = 200
AS
/*
    Orquestador de despacho de marcajes MB160 → ERPs (misma instancia SQL Server).
    Invocado por los SQL Agent Jobs de corte de asistencia (Regla 6):
      - 12:00 → Corte de Entrada  (EXEC sp_ProcessMarcajeQueue @Punch = 0)
      - 16:00 → Corte de Comida   (EXEC sp_ProcessMarcajeQueue @Punch = 4)
      - 23:00 → Corte de Salida   (EXEC sp_ProcessMarcajeQueue @Punch = 1)

    Reglas aplicadas del documento "Reglas de reloj checador":
      Regla 3  → Horario de comida válido: 12:50–16:10. Marcajes de Punch=4 fuera
                 de esa ventana se descartan (Estatus=4, no reintentable).
      Regla 6  → Tres cortes diarios a hora fija; cada job ejecuta solo su tipo.
      Regla 7  → El ERP valida omisión de entrada al afectar; este SP solo despacha
                 los registros existentes.

    Por cada registro en cola:
      1. Obtiene el código de empresa desde la DB destino (SELECT TOP 1 Empresa FROM Empresa).
      2. Inserta encabezado en [BaseDatos].dbo.Asiste (Estatus='SIN AFECTAR').
      3. Inserta detalle en [BaseDatos].dbo.AsisteD.
      4. Llama a [BaseDatos].dbo.spAfectar.
      5. Marca la cola como Hecho (Estatus=2) y TieneMovimientos=1 en AsistenciaMarcaje.

    Estatus en MarcajeDispatchQueue:
      0 = Pendiente   1 = Procesando   2 = Hecho   3 = Error (reintentable)   4 = Descartado
*/
BEGIN
    SET NOCOUNT ON;

    -- -------------------------------------------------------------------------
    -- 1. Tomar batch y marcarlo como Procesando.
    --    @Punch NULL → toma todos; valor específico → solo ese tipo de corte.
    --    READPAST evita bloqueos si dos jobs coinciden (no debería ocurrir, pero
    --    protege ante ejecuciones manuales simultáneas).
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
        WHERE Estatus IN (0, 3)                          -- Pendiente o Error (reintento)
          AND (@Punch IS NULL OR Punch = @Punch)         -- Filtro por tipo de corte
        ORDER BY MarcajeDispatchQueueID
    )
    UPDATE q
    SET
        Estatus      = 1,
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

    IF NOT EXISTS (SELECT 1 FROM @batch)
        RETURN;

    -- -------------------------------------------------------------------------
    -- 2. Descartar marcajes de Comida fuera de la ventana horaria válida.
    --    Regla 3: horario de comida 12:50–16:10.
    --    Se marcan como Estatus=4 (Descartado, no reintentable).
    -- -------------------------------------------------------------------------
    UPDATE dbo.MarcajeDispatchQueue
    SET
        Estatus      = 4,   -- Descartado
        UltimoError  = N'Comida fuera de ventana horaria permitida (12:50–16:10). Regla 3.',
        UltimoCambio = SYSDATETIME()
    WHERE MarcajeDispatchQueueID IN (
        SELECT MarcajeDispatchQueueID FROM @batch
        WHERE Punch = 4
          AND (CAST(EventoFechaHora AS TIME) < '12:50:00'
            OR CAST(EventoFechaHora AS TIME) > '16:10:00')
    );

    -- Eliminar del batch los que fueron descartados
    DELETE FROM @batch
    WHERE Punch = 4
      AND (CAST(EventoFechaHora AS TIME) < '12:50:00'
        OR CAST(EventoFechaHora AS TIME) > '16:10:00');

    IF NOT EXISTS (SELECT 1 FROM @batch)
        RETURN;

    -- -------------------------------------------------------------------------
    -- 3. Procesar cada registro restante.
    -- -------------------------------------------------------------------------
    DECLARE
        @QueueID      BIGINT,
        @MarcajeID    BIGINT,
        @DB           SYSNAME,
        @EmpresaID    INT,
        @PersonaID    INT,
        @PunchVal     TINYINT,
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
        @PersonaID, @PunchVal, @FechaEvento;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY

            -- Tipo de registro según Punch (Regla 6)
            SET @TipoRegistro = CASE @PunchVal
                WHEN 0 THEN N'Entrada'
                WHEN 1 THEN N'Salida'
                WHEN 4 THEN N'Comida'
            END;

            -- Hora en formato HH:mm para el campo HoraRegistro de AsisteD
            SET @HoraStr = CONVERT(NCHAR(5), CAST(@FechaEvento AS TIME), 108);

            -- ------------------------------------------------------------------
            -- 3a. Obtener código de empresa desde la DB destino
            -- ------------------------------------------------------------------
            SET @SQL    = N'SELECT TOP 1 @Code = Empresa FROM [' + @DB + N'].dbo.Empresa;';
            SET @Params = N'@Code NVARCHAR(50) OUTPUT';
            EXEC sp_executesql @SQL, @Params, @Code = @EmpresaCode OUTPUT;

            -- ------------------------------------------------------------------
            -- 3b. INSERT en Asiste (encabezado del movimiento)
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
            -- 3c. INSERT en AsisteD (detalle / renglón del movimiento)
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
            -- 3d. Afectar el movimiento en el ERP
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
            -- 3e. Marcar como Hecho en la cola
            -- ------------------------------------------------------------------
            UPDATE dbo.MarcajeDispatchQueue
            SET
                Estatus      = 2,
                AsisteID     = @AsisteID,
                ProcesadoEn  = SYSDATETIME(),
                UltimoError  = NULL,
                UltimoCambio = SYSDATETIME()
            WHERE MarcajeDispatchQueueID = @QueueID;

            -- ------------------------------------------------------------------
            -- 3f. Marcar el marcaje original como procesado
            -- ------------------------------------------------------------------
            UPDATE dbo.AsistenciaMarcaje
            SET TieneMovimientos = 1
            WHERE AsistenciaMarcajeID = @MarcajeID;

        END TRY
        BEGIN CATCH

            SET @ErrMsg = ERROR_MESSAGE();

            UPDATE dbo.MarcajeDispatchQueue
            SET
                Estatus      = 3,           -- Error (reintentable en siguiente corte)
                UltimoError  = LEFT(@ErrMsg, 4000),
                UltimoCambio = SYSDATETIME()
            WHERE MarcajeDispatchQueueID = @QueueID;

        END CATCH;

        FETCH NEXT FROM cur INTO
            @QueueID, @MarcajeID, @DB, @EmpresaID,
            @PersonaID, @PunchVal, @FechaEvento;
    END;

    CLOSE cur;
    DEALLOCATE cur;
END;
GO
