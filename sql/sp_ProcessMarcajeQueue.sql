CREATE OR ALTER PROCEDURE dbo.sp_ProcessMarcajeQueue
    @TipoCorte TINYINT = NULL,   -- Ventana horaria a procesar:
                                  --   0 = Entrada        (< 12:00)
                                  --   4 = Comida         (12:50 – 15:59) → genera SalidaComida y EntradaComida
                                  --   1 = Salida         (>= 16:00)
                                  -- NULL = todas las ventanas (corte semanal)
    @BatchSize INT     = 200
AS
/*
    Orquestador de despacho de marcajes MB160 → ERPs (misma instancia SQL Server).
    Invocado por los SQL Agent Jobs de corte de asistencia.

    Modelo: 1 Asiste por (empleado, día, TipoMov). Múltiples renglones AsisteD
    por Asiste (uno por marcaje del checador dentro de esa ventana/día).

    Clasificación de TipoMov por hora del evento:
      < 12:00:00              → Asiste.Mov = 'Entrada'
      12:00:00 – 12:49:59     → Descartado (zona gris, Estatus=4)
      12:50:00 – 15:59:59     → 1er marcaje del empleado en el día → 'SalidaComida'
                                 2do marcaje → 'EntradaComida'
                                 3er+ → Descartado (Estatus=4)
      >= 16:00:00             → Asiste.Mov = 'Salida'

    AsisteD.Registro = Asiste.Mov del encabezado.

    Flujo por grupo (BaseDatos, PersonaID, Fecha, TipoMov):
      INSERT Asiste (1 por grupo) → INSERT AsisteD (N renglones) → spAfectar → validar MovID

    Atomicidad: cada grupo se procesa en una transacción explícita.
    Si falla, todos los registros del grupo vuelven a Estatus=3 (reintentable).

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
        CodigoEmpresa          NVARCHAR(50),
        PersonaID              INT,
        Punch                  TINYINT,
        EventoFechaHora        DATETIME2(0),
        TipoMov                NVARCHAR(20)    -- asignado en paso 2
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
        ec.CodigoEmpresa,
        inserted.PersonaID,
        inserted.Punch,
        inserted.EventoFechaHora,
        NULL    -- TipoMov: se asigna en paso 2
    INTO @batch
    FROM dbo.MarcajeDispatchQueue q
    INNER JOIN cte ON cte.MarcajeDispatchQueueID = q.MarcajeDispatchQueueID
    INNER JOIN dbo.EmpresaConfig ec ON ec.EmpresaID = q.EmpresaID;

    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    -- -------------------------------------------------------------------------
    -- 2. Clasificar TipoMov en @batch
    --    2a. Clasificación base por ventana horaria
    -- -------------------------------------------------------------------------
    UPDATE @batch
    SET TipoMov =
        CASE
            WHEN CAST(EventoFechaHora AS TIME) < '12:00:00' THEN N'Entrada'
            WHEN CAST(EventoFechaHora AS TIME) < '12:50:00' THEN N'ZONAGRIS'
            WHEN CAST(EventoFechaHora AS TIME) < '16:00:00' THEN N'COMIDA_TBD'
            ELSE                                                  N'Salida'
        END;

    -- 2b. Split ventana comida: ROW_NUMBER por (DB, empleado, día) ordenado por hora.
    --     1er registro → 'SalidaComida' | 2do → 'EntradaComida' | 3ro+ → 'DESCARTAR'
    ;WITH comida AS (
        SELECT MarcajeDispatchQueueID,
               ROW_NUMBER() OVER (
                   PARTITION BY BaseDatos, PersonaID, CAST(EventoFechaHora AS DATE)
                   ORDER BY EventoFechaHora
               ) AS rn
        FROM @batch
        WHERE TipoMov = N'COMIDA_TBD'
    )
    UPDATE b
    SET b.TipoMov = CASE
        WHEN c.rn = 1 THEN N'SalidaComida'
        WHEN c.rn = 2 THEN N'Entradacomida'
        ELSE               N'DESCARTAR'
    END
    FROM @batch b
    INNER JOIN comida c ON c.MarcajeDispatchQueueID = b.MarcajeDispatchQueueID;

    -- -------------------------------------------------------------------------
    -- 3. Descartar zona gris (12:00–12:49) y excedente comida (3ro en adelante)
    -- -------------------------------------------------------------------------
    UPDATE dbo.MarcajeDispatchQueue
    SET Estatus      = 4,
        UltimoError  = CASE b.TipoMov
            WHEN N'ZONAGRIS'  THEN N'Marcaje en zona sin categoría (12:00–12:49). No corresponde a ningún corte.'
            WHEN N'DESCARTAR' THEN N'Más de 2 registros en ventana comida (12:50–15:59) para este empleado en el día. Tercero en adelante descartado.'
        END,
        UltimoCambio = SYSDATETIME()
    FROM dbo.MarcajeDispatchQueue mdq
    INNER JOIN @batch b ON b.MarcajeDispatchQueueID = mdq.MarcajeDispatchQueueID
    WHERE b.TipoMov IN (N'ZONAGRIS', N'DESCARTAR');

    DELETE FROM @batch WHERE TipoMov IN (N'ZONAGRIS', N'DESCARTAR');

    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    -- -------------------------------------------------------------------------
    -- 4. Procesar por grupos (BaseDatos, EmpresaID, PersonaID, Fecha, TipoMov)
    --    Un Asiste por grupo + múltiples AsisteD + spAfectar
    --    Cada grupo corre en su propia transacción para atomicidad.
    -- -------------------------------------------------------------------------
    DECLARE
        -- Cursor externo (grupo)
        @GrpDB           SYSNAME,
        @GrpEmpresaID    INT,
        @GrpCode         NVARCHAR(50),
        @GrpPersonaID    INT,
        @GrpFecha        DATE,
        @GrpTipoMov      NVARCHAR(20),
        @GrpRegistroCorto NVARCHAR(10),  -- valor para AsisteD.Registro (≤10 chars)
        -- Cursor interno (marcaje individual)
        @QueueID      BIGINT,
        @MarcajeID    BIGINT,
        @FechaEvento  DATETIME2(0),
        @HoraStr      NCHAR(5),
        -- Compartidas
        @SQL          NVARCHAR(MAX),
        @Params       NVARCHAR(MAX),
        @AsisteID     INT,
        @MovIDPost    NVARCHAR(50),
        @ErrMsg       NVARCHAR(4000);

    DECLARE curGrupos CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT
               BaseDatos,
               EmpresaID,
               CodigoEmpresa,
               PersonaID,
               CAST(EventoFechaHora AS DATE) AS Fecha,
               TipoMov
        FROM @batch
        ORDER BY BaseDatos, PersonaID, CAST(EventoFechaHora AS DATE), TipoMov;

    OPEN curGrupos;
    FETCH NEXT FROM curGrupos INTO
        @GrpDB, @GrpEmpresaID, @GrpCode, @GrpPersonaID, @GrpFecha, @GrpTipoMov;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;

            -- Mapear TipoMov → valor corto para AsisteD.Registro (columna ≤10 chars)
            -- Asiste.Mov siempre = 'Registro' (único Mov ASIS con consecutivo configurado)
            SET @GrpRegistroCorto = CASE @GrpTipoMov
                WHEN N'Entrada'       THEN N'Entrada'
                WHEN N'SalidaComida'  THEN N'SalComida'
                WHEN N'Entradacomida' THEN N'EntComida'
                WHEN N'Salida'        THEN N'Salida'
                ELSE @GrpTipoMov
            END;

            -- ------------------------------------------------------------------
            -- 4a. INSERT en Asiste (1 por grupo)
            --     Mov = 'Registro' siempre (único con folio configurado en movtipo)
            --     MovID lo genera spAfectar
            -- ------------------------------------------------------------------
            SET @AsisteID = NULL;
            SET @SQL = N'
                DECLARE @ids TABLE (ID INT);

                INSERT INTO [' + @GrpDB + N'].dbo.Asiste
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
                    @Fecha, @Fecha,
                    ''SINAFECTAR'', ''INTELISIS'',
                    YEAR(@Fecha), MONTH(@Fecha),
                    SYSDATETIME(),
                    1, 0,
                    1, 1,
                    0, 0, 0, 0, 0,
                    0, 0, 0, 0
                );

                SELECT @AsisteID = ID FROM @ids;';

            SET @Params = N'@EmpresaCode NVARCHAR(50), @Fecha DATE, @AsisteID INT OUTPUT';
            EXEC sp_executesql @SQL, @Params,
                @EmpresaCode = @GrpCode,
                @Fecha       = @GrpFecha,
                @AsisteID    = @AsisteID OUTPUT;

            IF @AsisteID IS NULL
                RAISERROR(N'OUTPUT INSERTED.ID regresó NULL — revisar si Asiste.ID es generado correctamente.', 16, 1);

            -- ------------------------------------------------------------------
            -- 4b. INSERT en AsisteD (un renglón por cada marcaje del grupo)
            --     Registro = @GrpRegistroCorto (≤10 chars, identifica el tipo)
            --     Renglon  = MAX global + 1 por cada inserción
            -- ------------------------------------------------------------------
            DECLARE curMarcajes CURSOR LOCAL FAST_FORWARD FOR
                SELECT MarcajeDispatchQueueID, AsistenciaMarcajeID, EventoFechaHora
                FROM @batch
                WHERE BaseDatos                    = @GrpDB
                  AND PersonaID                    = @GrpPersonaID
                  AND CAST(EventoFechaHora AS DATE) = @GrpFecha
                  AND TipoMov                      = @GrpTipoMov
                ORDER BY EventoFechaHora;

            OPEN curMarcajes;
            FETCH NEXT FROM curMarcajes INTO @QueueID, @MarcajeID, @FechaEvento;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @HoraStr = CONVERT(NCHAR(5), CAST(@FechaEvento AS TIME), 108);

                SET @SQL = N'
                    DECLARE @renglon INT;
                    SELECT @renglon = ISNULL(MAX(Renglon), 0) + 1
                    FROM [' + @GrpDB + N'].dbo.AsisteD;  -- global, sin filtro por ID

                    INSERT INTO [' + @GrpDB + N'].dbo.AsisteD
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
                        @PersonaID, @Registro, @Hora,
                        @Fecha, @Fecha, @Fecha,
                        1,
                        0, 0, 0, 0, 0
                    );';

                SET @Params = N'@AsisteID INT, @PersonaID INT, @Registro NVARCHAR(10),
                                @Hora NCHAR(5), @Fecha DATE';
                EXEC sp_executesql @SQL, @Params,
                    @AsisteID  = @AsisteID,
                    @PersonaID = @GrpPersonaID,
                    @Registro  = @GrpRegistroCorto,
                    @Hora      = @HoraStr,
                    @Fecha     = @GrpFecha;

                FETCH NEXT FROM curMarcajes INTO @QueueID, @MarcajeID, @FechaEvento;
            END;

            CLOSE curMarcajes; DEALLOCATE curMarcajes;

            -- ------------------------------------------------------------------
            -- 4c. Afectar el movimiento en el ERP
            --     spAfectar genera MovID (folio real) y pone Estatus='PROCESAR'.
            --     AsisteD ya existe antes de esta llamada (requerido por spAfectar).
            -- ------------------------------------------------------------------
            SET @SQL = N'
                EXEC [' + @GrpDB + N'].dbo.spAfectar
                    ''ASIS'', @AsisteID, ''AFECTAR'', ''Todo'',
                    NULL, ''INTELISIS'',
                    @Estacion = 1, @ensilencio = 1;';

            EXEC sp_executesql @SQL, N'@AsisteID INT', @AsisteID = @AsisteID;

            -- ------------------------------------------------------------------
            -- 4d. Validar que spAfectar generó el MovID
            --     Con @ensilencio=1 los fallos son silenciosos; si MovID sigue
            --     NULL el grupo debe reintentarse (Estatus=3), no marcarse Hecho.
            -- ------------------------------------------------------------------
            SET @MovIDPost = NULL;
            SET @SQL = N'SELECT @MovIDPost = MovID FROM [' + @GrpDB + N'].dbo.Asiste WHERE ID = @AsisteID;';
            EXEC sp_executesql @SQL,
                N'@AsisteID INT, @MovIDPost NVARCHAR(50) OUTPUT',
                @AsisteID  = @AsisteID,
                @MovIDPost = @MovIDPost OUTPUT;

            IF @MovIDPost IS NULL
                RAISERROR(N'spAfectar no generó MovID para Asiste.ID=%d — se reintentará en el siguiente ciclo.', 16, 1, @AsisteID);

            -- ------------------------------------------------------------------
            -- 4e. Marcar todo el grupo como Hecho + actualizar AsistenciaMarcaje
            -- ------------------------------------------------------------------
            UPDATE dbo.MarcajeDispatchQueue
            SET Estatus      = 2,
                AsisteID     = @AsisteID,
                ProcesadoEn  = SYSDATETIME(),
                UltimoError  = NULL,
                UltimoCambio = SYSDATETIME()
            WHERE MarcajeDispatchQueueID IN (
                SELECT MarcajeDispatchQueueID FROM @batch
                WHERE BaseDatos                    = @GrpDB
                  AND PersonaID                    = @GrpPersonaID
                  AND CAST(EventoFechaHora AS DATE) = @GrpFecha
                  AND TipoMov                      = @GrpTipoMov
            );

            UPDATE dbo.AsistenciaMarcaje
            SET TieneMovimientos = 1
            WHERE AsistenciaMarcajeID IN (
                SELECT AsistenciaMarcajeID FROM @batch
                WHERE BaseDatos                    = @GrpDB
                  AND PersonaID                    = @GrpPersonaID
                  AND CAST(EventoFechaHora AS DATE) = @GrpFecha
                  AND TipoMov                      = @GrpTipoMov
            );

            COMMIT TRANSACTION;

        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

            -- Cerrar cursor interno si quedó abierto por error dentro del loop
            IF CURSOR_STATUS('local', 'curMarcajes') >= 0
                CLOSE curMarcajes;
            IF CURSOR_STATUS('local', 'curMarcajes') >= -1
                DEALLOCATE curMarcajes;

            SET @ErrMsg = ERROR_MESSAGE();
            UPDATE dbo.MarcajeDispatchQueue
            SET Estatus      = 3,
                UltimoError  = LEFT(@ErrMsg, 4000),
                UltimoCambio = SYSDATETIME()
            WHERE MarcajeDispatchQueueID IN (
                SELECT MarcajeDispatchQueueID FROM @batch
                WHERE BaseDatos                    = @GrpDB
                  AND PersonaID                    = @GrpPersonaID
                  AND CAST(EventoFechaHora AS DATE) = @GrpFecha
                  AND TipoMov                      = @GrpTipoMov
            );
        END CATCH;

        FETCH NEXT FROM curGrupos INTO
            @GrpDB, @GrpEmpresaID, @GrpCode, @GrpPersonaID, @GrpFecha, @GrpTipoMov;
    END;

    CLOSE curGrupos;
    DEALLOCATE curGrupos;
END;
GO
