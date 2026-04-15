CREATE OR ALTER PROCEDURE dbo.sp_ProcessMarcajeQueue
    @TipoCorte TINYINT = NULL,   -- Ventana horaria a procesar:
                                  --   0 = Entrada  (< 12:00)
                                  --   4 = Comida   (12:50 – 15:59)
                                  --   1 = Salida   (>= 16:00)
                                  -- NULL = todas las ventanas (corte semanal)
    @BatchSize INT     = 200
AS
/*
    Orquestador de despacho de marcajes MB160 → ERPs (misma instancia SQL Server).
    Invocado por los SQL Agent Jobs de corte de asistencia (Regla 6):
      - 12:00 → Corte de Entrada  (EXEC sp_ProcessMarcajeQueue @TipoCorte = 0)
      - 16:00 → Corte de Comida   (EXEC sp_ProcessMarcajeQueue @TipoCorte = 4)
      - 23:00 → Corte de Salida   (EXEC sp_ProcessMarcajeQueue @TipoCorte = 1)

    Clasificación de Registro por hora del evento (no por Punch del dispositivo):
      < 12:00:00              → 'Entrada'
      12:00:00 – 12:49:59     → Descartado (zona gris, sin categoría — Estatus=4)
      12:50:00 – 15:59:59     → 'Comida'
      >= 16:00:00             → 'Salida'

    Fixes aplicados:
      - CodigoEmpresa se lee de dbo.EmpresaConfig (no con SELECT dinámico al ERP).
      - AsisteID se captura con OUTPUT INSERTED.ID (no SCOPE_IDENTITY, que falla
        cuando el ID de Asiste es manejado por el ERP y no es un IDENTITY de SQL).
      - Renglon es consecutivo GLOBAL de toda la tabla AsisteD (sin filtro por ID).

    Estatus: 0=Pendiente 1=Procesando 2=Hecho 3=Error(reintentable) 4=Descartado
*/
BEGIN
    SET NOCOUNT ON;

    -- -------------------------------------------------------------------------
    -- 1. Tomar batch y marcarlo como Procesando
    --    Filtro por ventana horaria según @TipoCorte
    -- -------------------------------------------------------------------------
    DECLARE @batch TABLE
    (
        MarcajeDispatchQueueID BIGINT,
        AsistenciaMarcajeID    BIGINT,
        BaseDatos              SYSNAME,
        EmpresaID              INT,
        CodigoEmpresa          NVARCHAR(50),   -- leído de EmpresaConfig
        PersonaID              INT,
        Punch                  TINYINT,
        EventoFechaHora        DATETIME2(0)
    );

    ;WITH cte AS (
        SELECT TOP (@BatchSize) q.MarcajeDispatchQueueID
        FROM dbo.MarcajeDispatchQueue q
        WITH (READPAST, UPDLOCK, ROWLOCK)
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
    OUTPUT
        inserted.MarcajeDispatchQueueID,
        inserted.AsistenciaMarcajeID,
        inserted.BaseDatos,
        inserted.EmpresaID,
        ec.CodigoEmpresa,            -- ← viene de EmpresaConfig, no del ERP
        inserted.PersonaID,
        inserted.Punch,
        inserted.EventoFechaHora
    INTO @batch
    FROM dbo.MarcajeDispatchQueue q
    INNER JOIN cte ON cte.MarcajeDispatchQueueID = q.MarcajeDispatchQueueID
    INNER JOIN dbo.EmpresaConfig ec ON ec.EmpresaID = q.EmpresaID;

    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    -- -------------------------------------------------------------------------
    -- 2. Descartar marcajes en zona gris 12:00–12:49:59 (sin categoría horaria)
    --    Solo aplica cuando @TipoCorte IS NULL (corte semanal que toma todo)
    -- -------------------------------------------------------------------------
    UPDATE dbo.MarcajeDispatchQueue
    SET Estatus      = 4,
        UltimoError  = N'Marcaje en zona sin categoría (12:00–12:49). No corresponde a ningún corte.',
        UltimoCambio = SYSDATETIME()
    WHERE MarcajeDispatchQueueID IN (
        SELECT MarcajeDispatchQueueID FROM @batch
        WHERE CAST(EventoFechaHora AS TIME) >= '12:00:00'
          AND CAST(EventoFechaHora AS TIME) <  '12:50:00'
    );

    DELETE FROM @batch
    WHERE CAST(EventoFechaHora AS TIME) >= '12:00:00'
      AND CAST(EventoFechaHora AS TIME) <  '12:50:00';

    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    -- -------------------------------------------------------------------------
    -- 3. Procesar registro por registro
    -- -------------------------------------------------------------------------
    DECLARE
        @QueueID      BIGINT,
        @MarcajeID    BIGINT,
        @DB           SYSNAME,
        @EmpresaID    INT,
        @EmpresaCode  NVARCHAR(50),
        @PersonaID    INT,
        @PunchVal     TINYINT,
        @FechaEvento  DATETIME2(0),
        @SQL          NVARCHAR(MAX),
        @Params       NVARCHAR(MAX),
        @AsisteID     INT,
        @TipoRegistro NVARCHAR(20),
        @HoraEvento   TIME,
        @HoraStr      NCHAR(5),
        @ErrMsg       NVARCHAR(4000);

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT MarcajeDispatchQueueID, AsistenciaMarcajeID, BaseDatos,
               EmpresaID, CodigoEmpresa, PersonaID, Punch, EventoFechaHora
        FROM @batch
        ORDER BY MarcajeDispatchQueueID;

    OPEN cur;
    FETCH NEXT FROM cur INTO
        @QueueID, @MarcajeID, @DB, @EmpresaID, @EmpresaCode,
        @PersonaID, @PunchVal, @FechaEvento;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY

            -- Clasificar por hora del evento (Punch del dispositivo es solo referencia)
            SET @HoraEvento = CAST(@FechaEvento AS TIME);

            SET @TipoRegistro = CASE
                WHEN @HoraEvento <  '12:00:00'                             THEN N'Entrada'
                WHEN @HoraEvento >= '12:50:00' AND @HoraEvento < '16:00:00' THEN N'Comida'
                WHEN @HoraEvento >= '16:00:00'                             THEN N'Salida'
                ELSE NULL   -- zona gris ya descartada en el paso 2
            END;

            SET @HoraStr = CONVERT(NCHAR(5), @HoraEvento, 108);

            -- ------------------------------------------------------------------
            -- 3a. INSERT en Asiste + captura de ID con OUTPUT INSERTED.ID
            --     MovID se omite — spAfectar genera el folio real.
            --     (no se usa SCOPE_IDENTITY porque el ID puede ser manejado
            --      por el ERP con lógica propia, no como IDENTITY de SQL Server)
            -- ------------------------------------------------------------------
            SET @SQL = N'
                DECLARE @ids TABLE (ID INT);

                INSERT INTO [' + @DB + N'].dbo.Asiste
                (
                    Empresa, Mov,
                    FechaEmision, FechaAplicacion,
                    Estatus, Usuario,
                    Ejercicio, Periodo,
                    FechaRegistro,
                    Sucursal, GenerarPoliza,
                    SincroC, SucursalOrigen,
                    Logico1, Logico2, Logico3, Logico4, Logico5,
                    Logico6, Logico7, Logico8, Logico9
                )
                OUTPUT INSERTED.ID INTO @ids
                VALUES
                (
                    @EmpresaCode, ''Registro'',
                    CAST(@Fecha AS DATE), CAST(@Fecha AS DATE),
                    ''SIN AFECTAR'', ''INTELISIS'',
                    YEAR(@Fecha), MONTH(@Fecha),
                    SYSDATETIME(),
                    1, 0,
                    1, 1,
                    0, 0, 0, 0, 0,
                    0, 0, 0, 0
                );

                SELECT @AsisteID = ID FROM @ids;';

            SET @Params = N'@EmpresaCode NVARCHAR(50), @Fecha DATETIME2(0), @AsisteID INT OUTPUT';
            EXEC sp_executesql @SQL, @Params,
                @EmpresaCode = @EmpresaCode,
                @Fecha       = @FechaEvento,
                @AsisteID    = @AsisteID OUTPUT;

            -- Verificar que obtuvimos un ID válido antes de continuar
            IF @AsisteID IS NULL
                RAISERROR(N'OUTPUT INSERTED.ID regresó NULL — revisar si Asiste.ID es generado correctamente.', 16, 1);

            -- ------------------------------------------------------------------
            -- 3b. Afectar el movimiento en el ERP
            --     spAfectar genera MovID (folio real) y pone Estatus='AFECTADO'.
            --     AsisteD se inserta después para enlazarse al movimiento ya procesado.
            -- ------------------------------------------------------------------
            SET @SQL = N'
                EXEC [' + @DB + N'].dbo.spAfectar
                    ''ASIS'', @AsisteID, ''AFECTAR'', ''Todo'',
                    NULL, ''INTELISIS'',
                    @Estacion = 1, @ensilencio = 1;';

            EXEC sp_executesql @SQL, N'@AsisteID INT', @AsisteID = @AsisteID;

            -- ------------------------------------------------------------------
            -- 3c. INSERT en AsisteD
            --     ID = @AsisteID (mismo del Asiste ya afectado — el ID no cambia
            --     tras spAfectar, confirmado).
            --     Renglon: consecutivo GLOBAL de toda la tabla AsisteD.
            --     Personal: UsuarioDispositivo completo como INT.
            -- ------------------------------------------------------------------
            SET @SQL = N'
                DECLARE @renglon INT;
                SELECT @renglon = ISNULL(MAX(Renglon), 0) + 1
                FROM [' + @DB + N'].dbo.AsisteD;  -- global, sin filtro por ID

                INSERT INTO [' + @DB + N'].dbo.AsisteD
                (
                    ID, Renglon,
                    Personal, Registro, HoraRegistro,
                    FechaD, FechaA, Fecha,
                    Sucursal,
                    Logico1, Logico2, Logico3, Logico4, Logico5
                )
                VALUES
                (
                    @AsisteID, @renglon,
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
            -- 3d. Marcar como Hecho
            -- ------------------------------------------------------------------
            UPDATE dbo.MarcajeDispatchQueue
            SET Estatus      = 2,
                AsisteID     = @AsisteID,
                ProcesadoEn  = SYSDATETIME(),
                UltimoError  = NULL,
                UltimoCambio = SYSDATETIME()
            WHERE MarcajeDispatchQueueID = @QueueID;

            UPDATE dbo.AsistenciaMarcaje
            SET TieneMovimientos = 1
            WHERE AsistenciaMarcajeID = @MarcajeID;

        END TRY
        BEGIN CATCH
            SET @ErrMsg = ERROR_MESSAGE();
            UPDATE dbo.MarcajeDispatchQueue
            SET Estatus      = 3,
                UltimoError  = LEFT(@ErrMsg, 4000),
                UltimoCambio = SYSDATETIME()
            WHERE MarcajeDispatchQueueID = @QueueID;
        END CATCH;

        FETCH NEXT FROM cur INTO
            @QueueID, @MarcajeID, @DB, @EmpresaID, @EmpresaCode,
            @PersonaID, @PunchVal, @FechaEvento;
    END;

    CLOSE cur;
    DEALLOCATE cur;
END;
GO
