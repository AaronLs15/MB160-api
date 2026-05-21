/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  REPLAY COTAILORDEV — Días hasta 2026-05-13                                  ║
║                                                                              ║
║  Cambios:                                                                    ║
║   • SP redespliegue: errores ya NO cancelan el Asiste, queda como borrador  ║
║     con el error en Asiste.Observaciones.                                    ║
║   • Validación nueva: máx 1 Asiste activo por (BaseDatos, Mov, Fecha).      ║
║                                                                              ║
║  Plan:                                                                       ║
║   Sección A — Redesplegar sp_ProcessMarcajeQueue                            ║
║   Sección B — Limpiar Asistes CANCELADO en cotailordev (hasta 2026-05-13)   ║
║   Sección C — Resetear cola (Estatus=3 → 0) para reproceso                  ║
║   Sección D — Ejecutar sp_ProcessMarcajeQueue                                ║
║   Sección E — Verificar resultado                                            ║
║                                                                              ║
║  ⚠ Verificar que EmpresaConfig.BaseDatos para EmpresaID=5 = N'cotailordev'  ║
║    antes de ejecutar Sección D.                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

-- ═════════════════════════════════════════════════════════════════════════════
-- SECCIÓN A — REDESPLEGAR sp_ProcessMarcajeQueue en Checador
-- ═════════════════════════════════════════════════════════════════════════════

USE Checador;
GO

CREATE OR ALTER PROCEDURE dbo.sp_ProcessMarcajeQueue
    @TipoCorte TINYINT = NULL,
    @BatchSize INT     = 200
AS
/*
    Orquestador MB160 → ERP. Modelo: 1 Asiste por (BaseDatos, Fecha, TipoMov).

    Cambios v2:
      - Validación duplicado: aborta el grupo si ya existe Asiste activo del mismo tipo/día.
      - Errores post-spAfectar (sin MovID o excepción): NO cancela el Asiste.
        Queda como borrador (SINAFECTAR) con el error en Asiste.Observaciones.

    Estatus cola: 0=Pendiente 1=Procesando 2=Hecho 3=Error(reintentable) 4=Descartado
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

    -- 1. Tomar batch + marcar Procesando
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
        NULL
    INTO @batch
    FROM dbo.MarcajeDispatchQueue q
    INNER JOIN cte ON cte.MarcajeDispatchQueueID = q.MarcajeDispatchQueueID
    INNER JOIN dbo.EmpresaConfig ec ON ec.EmpresaID = q.EmpresaID;

    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    -- 1b. DEDUPE doble-tap (≤60s mismo empleado/día)
    UPDATE dbo.MarcajeDispatchQueue
    SET Estatus      = 4,
        UltimoError  = N'Marcaje duplicado (≤60s del anterior del mismo empleado en el día). Doble-tap del checador.',
        UltimoCambio = SYSDATETIME()
    FROM dbo.MarcajeDispatchQueue mdq
    INNER JOIN @batch b ON b.MarcajeDispatchQueueID = mdq.MarcajeDispatchQueueID
    INNER JOIN dbo.AsistenciaMarcaje am ON am.AsistenciaMarcajeID = b.AsistenciaMarcajeID
    WHERE EXISTS (
        SELECT 1 FROM dbo.AsistenciaMarcaje prev
        WHERE prev.UsuarioDispositivo = am.UsuarioDispositivo
          AND prev.EventoFechaHora    < am.EventoFechaHora
          AND CAST(prev.EventoFechaHora AS DATE) = CAST(am.EventoFechaHora AS DATE)
          AND DATEDIFF(SECOND, prev.EventoFechaHora, am.EventoFechaHora) <= 60
    );

    DELETE b
    FROM @batch b
    INNER JOIN dbo.AsistenciaMarcaje am ON am.AsistenciaMarcajeID = b.AsistenciaMarcajeID
    WHERE EXISTS (
        SELECT 1 FROM dbo.AsistenciaMarcaje prev
        WHERE prev.UsuarioDispositivo = am.UsuarioDispositivo
          AND prev.EventoFechaHora    < am.EventoFechaHora
          AND CAST(prev.EventoFechaHora AS DATE) = CAST(am.EventoFechaHora AS DATE)
          AND DATEDIFF(SECOND, prev.EventoFechaHora, am.EventoFechaHora) <= 60
    );

    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    -- 2. Clasificar TipoMov
    UPDATE @batch
    SET TipoMov =
        CASE
            WHEN CAST(EventoFechaHora AS TIME) < '12:00:00' THEN N'Entrada'
            WHEN CAST(EventoFechaHora AS TIME) < '12:50:00' THEN N'ZONAGRIS'
            WHEN CAST(EventoFechaHora AS TIME) < '16:00:00' THEN N'COMIDA_TBD'
            ELSE                                                  N'Salida'
        END;

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

    -- 3. Descartar zona gris y excedente comida
    UPDATE dbo.MarcajeDispatchQueue
    SET Estatus      = 4,
        UltimoError  = CASE b.TipoMov
            WHEN N'ZONAGRIS'  THEN N'Marcaje en zona sin categoría (12:00–12:49).'
            WHEN N'DESCARTAR' THEN N'Más de 2 registros en ventana comida (12:50–15:59) para este empleado en el día.'
        END,
        UltimoCambio = SYSDATETIME()
    FROM dbo.MarcajeDispatchQueue mdq
    INNER JOIN @batch b ON b.MarcajeDispatchQueueID = mdq.MarcajeDispatchQueueID
    WHERE b.TipoMov IN (N'ZONAGRIS', N'DESCARTAR');

    DELETE FROM @batch WHERE TipoMov IN (N'ZONAGRIS', N'DESCARTAR');
    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    -- 3b. Descartar empleados no autorizados
    UPDATE dbo.MarcajeDispatchQueue
    SET Estatus      = 4,
        UltimoError  = N'PersonaID no registrado en Intelisis (no existe en PersonalAutorizado).',
        UltimoCambio = SYSDATETIME()
    FROM dbo.MarcajeDispatchQueue mdq
    INNER JOIN @batch b ON b.MarcajeDispatchQueueID = mdq.MarcajeDispatchQueueID
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.PersonalAutorizado pa
        WHERE pa.EmpresaID = b.EmpresaID
          AND pa.PersonaID = b.PersonaID
    );

    DELETE b
    FROM @batch b
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.PersonalAutorizado pa
        WHERE pa.EmpresaID = b.EmpresaID
          AND pa.PersonaID = b.PersonaID
    );

    IF NOT EXISTS (SELECT 1 FROM @batch) RETURN;

    -- 4. Procesar por grupo
    DECLARE
        @GrpDB            SYSNAME,
        @GrpEmpresaID     INT,
        @GrpCode          NVARCHAR(50),
        @GrpFecha         DATE,
        @GrpTipoMov       NVARCHAR(20),
        @GrpRegistroCorto NVARCHAR(10),
        @InnerPersonaID   INT,
        @QueueID          BIGINT,
        @MarcajeID        BIGINT,
        @FechaEvento      DATETIME2(0),
        @HoraStr          NCHAR(5),
        @SQL              NVARCHAR(MAX),
        @Params           NVARCHAR(MAX),
        @AsisteID         INT,
        @ExistingAsisteID INT,
        @CancelOK         BIT,
        @InsertOK         BIT,
        @MovIDPost        NVARCHAR(50),
        @ErrMsg           NVARCHAR(4000),
        @ActiveCount      INT,
        @FechaStr         NVARCHAR(10);

    -- Orden crítico: Entrada → SalidaComida → Entradacomida → Salida
    -- spAfectar rechaza SalidaComida si Salida del mismo empleado ya afectada.
    DECLARE curGrupos CURSOR LOCAL FAST_FORWARD FOR
        SELECT BaseDatos, EmpresaID, CodigoEmpresa, Fecha, TipoMov
        FROM (
            SELECT DISTINCT
                   BaseDatos, EmpresaID, CodigoEmpresa,
                   CAST(EventoFechaHora AS DATE) AS Fecha,
                   TipoMov
            FROM @batch
        ) g
        ORDER BY BaseDatos,
                 Fecha,
                 CASE TipoMov
                     WHEN N'Entrada'       THEN 1
                     WHEN N'SalidaComida'  THEN 2
                     WHEN N'Entradacomida' THEN 3
                     WHEN N'Salida'        THEN 4
                     ELSE 99
                 END;

    OPEN curGrupos;
    FETCH NEXT FROM curGrupos INTO
        @GrpDB, @GrpEmpresaID, @GrpCode, @GrpFecha, @GrpTipoMov;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @CancelOK = 1;
        SET @InsertOK = 0;

        -- AsisteD.Registro:
        --   Asiste.Mov = 'Entradacomida' → AsisteD.Registro = 'Entrada'
        --   Asiste.Mov = 'SalidaComida'  → AsisteD.Registro = 'Salida'
        SET @GrpRegistroCorto = CASE @GrpTipoMov
            WHEN N'Entrada'       THEN N'Entrada'
            WHEN N'SalidaComida'  THEN N'Salida'
            WHEN N'Entradacomida' THEN N'Entrada'
            WHEN N'Salida'        THEN N'Salida'
            ELSE @GrpTipoMov
        END;

        -- ==================================================================
        -- FASE 0: Cancelar Asiste previo (fuera de transacción)
        -- ==================================================================
        BEGIN TRY
            SET @ExistingAsisteID = NULL;
            SET @SQL = N'
                SELECT TOP 1 @ExistingAsisteID = ID
                FROM [' + @GrpDB + N'].dbo.Asiste
                WHERE Empresa         = @EmpresaCode
                  AND Mov             = @TipoMov
                  AND FechaAplicacion = @Fecha
                  AND Usuario         = ''INTELISIS''
                  AND Estatus         NOT IN (''CANCELADO'', ''CANCELAR'');';
            SET @Params = N'@EmpresaCode NVARCHAR(50), @TipoMov NVARCHAR(20), @Fecha DATE, @ExistingAsisteID INT OUTPUT';
            EXEC sp_executesql @SQL, @Params,
                @EmpresaCode      = @GrpCode,
                @TipoMov          = @GrpTipoMov,
                @Fecha            = @GrpFecha,
                @ExistingAsisteID = @ExistingAsisteID OUTPUT;

            IF @ExistingAsisteID IS NOT NULL
            BEGIN
                SET @SQL = N'
                    EXEC [' + @GrpDB + N'].dbo.spAfectar
                        ''ASIS'', @AsisteID, ''CANCELAR'', ''Todo'',
                        NULL, ''INTELISIS'',
                        @Estacion = 1, @ensilencio = 1;';
                EXEC sp_executesql @SQL, N'@AsisteID INT', @AsisteID = @ExistingAsisteID;
            END;
        END TRY
        BEGIN CATCH
            SET @CancelOK = 0;
            SET @ErrMsg   = ERROR_MESSAGE();
            UPDATE dbo.MarcajeDispatchQueue
            SET Estatus      = 3,
                UltimoError  = LEFT(N'Fase 0 (cancel previo): ' + @ErrMsg, 4000),
                UltimoCambio = SYSDATETIME()
            WHERE MarcajeDispatchQueueID IN (
                SELECT MarcajeDispatchQueueID FROM @batch
                WHERE BaseDatos                    = @GrpDB
                  AND CAST(EventoFechaHora AS DATE) = @GrpFecha
                  AND TipoMov                      = @GrpTipoMov
            );
        END CATCH;

        -- ==================================================================
        -- FASE 1: Inserts (transaccional) — incluye validación duplicado
        -- ==================================================================
        IF @CancelOK = 1
        BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;

            -- 4.0 VALIDACIÓN: máximo 1 Asiste activo por (BaseDatos, Empresa, Mov, Fecha)
            SET @ActiveCount = 0;
            SET @SQL = N'
                SELECT @ActiveCount = COUNT(*)
                FROM [' + @GrpDB + N'].dbo.Asiste
                WHERE Empresa         = @EmpresaCode
                  AND Mov             = @TipoMov
                  AND FechaAplicacion = @Fecha
                  AND Usuario         = ''INTELISIS''
                  AND Estatus         NOT IN (''CANCELADO'', ''CANCELAR'');';
            SET @Params = N'@EmpresaCode NVARCHAR(50), @TipoMov NVARCHAR(20), @Fecha DATE, @ActiveCount INT OUTPUT';
            EXEC sp_executesql @SQL, @Params,
                @EmpresaCode = @GrpCode,
                @TipoMov     = @GrpTipoMov,
                @Fecha       = @GrpFecha,
                @ActiveCount = @ActiveCount OUTPUT;

            IF @ActiveCount > 0
            BEGIN
                SET @FechaStr = CONVERT(NVARCHAR(10), @GrpFecha, 23);
                RAISERROR(
                    N'Validación duplicado: ya existe %d Asiste(s) activo(s) para DB=%s, Mov=%s, Fecha=%s.',
                    16, 1, @ActiveCount, @GrpDB, @GrpTipoMov, @FechaStr);
            END;

            -- 4a. INSERT Asiste
            SET @AsisteID = NULL;
            SET @SQL = N'
                DECLARE @ids TABLE (ID INT);
                INSERT INTO [' + @GrpDB + N'].dbo.Asiste
                (Empresa, Mov, FechaEmision, FechaAplicacion,
                 Estatus, Usuario, Ejercicio, Periodo, FechaRegistro,
                 Sucursal, GenerarPoliza, SincroC, SucursalOrigen,
                 Logico1, Logico2, Logico3, Logico4, Logico5,
                 Logico6, Logico7, Logico8, Logico9)
                OUTPUT INSERTED.ID INTO @ids
                VALUES (@EmpresaCode, @TipoMov, @Fecha, @Fecha,
                        ''SINAFECTAR'', ''INTELISIS'', YEAR(@Fecha), MONTH(@Fecha), SYSDATETIME(),
                        1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
                SELECT @AsisteID = ID FROM @ids;';
            SET @Params = N'@EmpresaCode NVARCHAR(50), @TipoMov NVARCHAR(20), @Fecha DATE, @AsisteID INT OUTPUT';
            EXEC sp_executesql @SQL, @Params,
                @EmpresaCode = @GrpCode,
                @TipoMov     = @GrpTipoMov,
                @Fecha       = @GrpFecha,
                @AsisteID    = @AsisteID OUTPUT;

            IF @AsisteID IS NULL
                RAISERROR(N'OUTPUT INSERTED.ID regresó NULL.', 16, 1);

            -- 4b. INSERT AsisteD
            DECLARE curMarcajes CURSOR LOCAL FAST_FORWARD FOR
                SELECT MarcajeDispatchQueueID, AsistenciaMarcajeID, PersonaID, EventoFechaHora
                FROM @batch
                WHERE BaseDatos                    = @GrpDB
                  AND CAST(EventoFechaHora AS DATE) = @GrpFecha
                  AND TipoMov                      = @GrpTipoMov

                UNION ALL

                SELECT q.MarcajeDispatchQueueID, q.AsistenciaMarcajeID, q.PersonaID, q.EventoFechaHora
                FROM dbo.MarcajeDispatchQueue q
                WHERE q.BaseDatos                    = @GrpDB
                  AND CAST(q.EventoFechaHora AS DATE) = @GrpFecha
                  AND q.Estatus                       = 2
                  AND q.AsisteID                      = @ExistingAsisteID

                ORDER BY EventoFechaHora;

            OPEN curMarcajes;
            FETCH NEXT FROM curMarcajes INTO @QueueID, @MarcajeID, @InnerPersonaID, @FechaEvento;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @HoraStr = CONVERT(NCHAR(5), CAST(@FechaEvento AS TIME), 108);
                SET @SQL = N'
                    INSERT INTO [' + @GrpDB + N'].dbo.AsisteD
                    (ID, Renglon, Personal, Registro, HoraRegistro,
                     FechaD, FechaA, Fecha, Sucursal,
                     Logico1, Logico2, Logico3, Logico4, Logico5)
                    SELECT @AsisteID, ISNULL(MAX(Renglon), 0) + 1,
                           @PersonaID, @Registro, @Hora,
                           @Fecha, @Fecha, @Fecha, 1, 0, 0, 0, 0, 0
                    FROM [' + @GrpDB + N'].dbo.AsisteD WITH (UPDLOCK, HOLDLOCK);';
                SET @Params = N'@AsisteID INT, @PersonaID INT, @Registro NVARCHAR(10), @Hora NCHAR(5), @Fecha DATE';
                EXEC sp_executesql @SQL, @Params,
                    @AsisteID  = @AsisteID,
                    @PersonaID = @InnerPersonaID,
                    @Registro  = @GrpRegistroCorto,
                    @Hora      = @HoraStr,
                    @Fecha     = @GrpFecha;

                FETCH NEXT FROM curMarcajes INTO @QueueID, @MarcajeID, @InnerPersonaID, @FechaEvento;
            END;
            CLOSE curMarcajes; DEALLOCATE curMarcajes;

            COMMIT TRANSACTION;
            SET @InsertOK = 1;

        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
            IF CURSOR_STATUS('local', 'curMarcajes') >= 0  CLOSE      curMarcajes;
            IF CURSOR_STATUS('local', 'curMarcajes') >= -1 DEALLOCATE curMarcajes;

            SET @ErrMsg = ERROR_MESSAGE();
            UPDATE dbo.MarcajeDispatchQueue
            SET Estatus      = 3,
                UltimoError  = LEFT(@ErrMsg, 4000),
                UltimoCambio = SYSDATETIME()
            WHERE MarcajeDispatchQueueID IN (
                SELECT MarcajeDispatchQueueID FROM @batch
                WHERE BaseDatos                    = @GrpDB
                  AND CAST(EventoFechaHora AS DATE) = @GrpFecha
                  AND TipoMov                      = @GrpTipoMov
            );
        END CATCH;
        END;  -- IF @CancelOK

        -- ==================================================================
        -- FASE 2: spAfectar (sin transacción).
        --         Si falla: NO cancela, deja borrador con error en Observaciones.
        -- ==================================================================
        IF @InsertOK = 1
        BEGIN
            BEGIN TRY
                SET @SQL = N'
                    EXEC [' + @GrpDB + N'].dbo.spAfectar
                        ''ASIS'', @AsisteID, ''AFECTAR'', ''Todo'',
                        NULL, ''INTELISIS'',
                        @Estacion = 1;';
                EXEC sp_executesql @SQL, N'@AsisteID INT', @AsisteID = @AsisteID;

                SET @MovIDPost = NULL;
                SET @SQL = N'SELECT @MovIDPost = MovID FROM [' + @GrpDB + N'].dbo.Asiste WHERE ID = @AsisteID;';
                EXEC sp_executesql @SQL,
                    N'@AsisteID INT, @MovIDPost NVARCHAR(50) OUTPUT',
                    @AsisteID  = @AsisteID,
                    @MovIDPost = @MovIDPost OUTPUT;

                IF @MovIDPost IS NULL
                    RAISERROR(N'spAfectar no generó MovID para Asiste.ID=%d.', 16, 1, @AsisteID);

                -- Éxito: marcar cola Hecho
                UPDATE dbo.MarcajeDispatchQueue
                SET Estatus      = 2,
                    AsisteID     = @AsisteID,
                    ProcesadoEn  = SYSDATETIME(),
                    UltimoError  = NULL,
                    UltimoCambio = SYSDATETIME()
                WHERE MarcajeDispatchQueueID IN (
                    SELECT MarcajeDispatchQueueID FROM @batch
                    WHERE BaseDatos                    = @GrpDB
                      AND CAST(EventoFechaHora AS DATE) = @GrpFecha
                      AND TipoMov                      = @GrpTipoMov
                );

                UPDATE dbo.AsistenciaMarcaje
                SET TieneMovimientos = 1
                WHERE AsistenciaMarcajeID IN (
                    SELECT AsistenciaMarcajeID FROM @batch
                    WHERE BaseDatos                    = @GrpDB
                      AND CAST(EventoFechaHora AS DATE) = @GrpFecha
                      AND TipoMov                      = @GrpTipoMov
                );

                IF @ExistingAsisteID IS NOT NULL
                BEGIN
                    UPDATE dbo.MarcajeDispatchQueue
                    SET AsisteID     = @AsisteID,
                        UltimoCambio = SYSDATETIME()
                    WHERE AsisteID = @ExistingAsisteID
                      AND Estatus   = 2;
                END;

            END TRY
            BEGIN CATCH
                -- Fallo Fase 2: NO cancelar. Dejar como borrador + error en Observaciones.
                SET @ErrMsg = ERROR_MESSAGE();

                BEGIN TRY
                    SET @SQL = N'
                        UPDATE [' + @GrpDB + N'].dbo.Asiste
                        SET Observaciones = LEFT(
                            ISNULL(NULLIF(LTRIM(RTRIM(Observaciones)), N'''') + N'' | '', N'''')
                            + N''['' + CONVERT(NVARCHAR(19), SYSDATETIME(), 120) + N''] '' + @ErrMsg,
                            255)
                        WHERE ID = @AsisteID;';
                    EXEC sp_executesql @SQL,
                        N'@AsisteID INT, @ErrMsg NVARCHAR(4000)',
                        @AsisteID = @AsisteID,
                        @ErrMsg   = @ErrMsg;
                END TRY
                BEGIN CATCH
                    SET @ErrMsg = @ErrMsg + N' | UPDATE Observaciones falló: ' + ERROR_MESSAGE();
                END CATCH;

                UPDATE dbo.MarcajeDispatchQueue
                SET Estatus      = 3,
                    AsisteID     = @AsisteID,
                    UltimoError  = LEFT(@ErrMsg, 4000),
                    UltimoCambio = SYSDATETIME()
                WHERE MarcajeDispatchQueueID IN (
                    SELECT MarcajeDispatchQueueID FROM @batch
                    WHERE BaseDatos                    = @GrpDB
                      AND CAST(EventoFechaHora AS DATE) = @GrpFecha
                      AND TipoMov                      = @GrpTipoMov
                );
            END CATCH;
        END;

        FETCH NEXT FROM curGrupos INTO
            @GrpDB, @GrpEmpresaID, @GrpCode, @GrpFecha, @GrpTipoMov;
    END;

    CLOSE curGrupos;
    DEALLOCATE curGrupos;
END;
GO

PRINT '✓ SP dbo.sp_ProcessMarcajeQueue redesplegado (v2: no-cancel-on-error + validación duplicado)';
GO


-- ═════════════════════════════════════════════════════════════════════════════
-- SECCIÓN B — LIMPIEZA en cotailordev: borrar Asistes CANCELADO hasta día 13
-- ═════════════════════════════════════════════════════════════════════════════

USE cotailordev;
GO

-- B.1 Snapshot antes
PRINT '── Asistes a borrar (CANCELADO/sin MovID) hasta 2026-05-13 ──';
SELECT a.FechaAplicacion, a.Mov, a.Estatus, a.MovID, COUNT(*) AS Total
FROM dbo.Asiste a
WHERE a.Usuario        = 'INTELISIS'
  AND a.Mov            IN ('Entrada','SalidaComida','Entradacomida','Salida')
  AND a.FechaAplicacion <= '20260513'
  AND a.MovID          IS NULL
GROUP BY a.FechaAplicacion, a.Mov, a.Estatus, a.MovID
ORDER BY a.FechaAplicacion, a.Mov;
GO

-- B.2 Borrar AsisteD primero (los renglones huérfanos)
DELETE d
FROM dbo.AsisteD d
INNER JOIN dbo.Asiste a ON a.ID = d.ID
WHERE a.Usuario        = 'INTELISIS'
  AND a.Mov            IN ('Entrada','SalidaComida','Entradacomida','Salida')
  AND a.FechaAplicacion <= '20260513'
  AND a.MovID          IS NULL;   -- protege Asistes con MovID válido (Hechos)

PRINT '✓ AsisteD borrados';

-- B.3 Borrar Asiste
DELETE FROM dbo.Asiste
WHERE Usuario        = 'INTELISIS'
  AND Mov            IN ('Entrada','SalidaComida','Entradacomida','Salida')
  AND FechaAplicacion <= '20260513'
  AND MovID          IS NULL;

PRINT '✓ Asistes borrados';
GO


-- ═════════════════════════════════════════════════════════════════════════════
-- SECCIÓN C — RESET cola para reproceso (días <= 2026-05-13)
-- ═════════════════════════════════════════════════════════════════════════════

USE Checador;
GO

-- C.1 Limpiar referencias colgadas (AsisteID que ya no existe)
UPDATE dbo.MarcajeDispatchQueue
SET Estatus      = 0,
    Intentos     = 0,
    UltimoError  = NULL,
    AsisteID     = NULL,
    ProcesadoEn  = NULL,
    UltimoCambio = SYSDATETIME()
WHERE BaseDatos = N'cotailordev'
  AND CAST(EventoFechaHora AS DATE) <= '20260513'
  AND Estatus IN (1, 3);   -- Procesando colgado + Error reintentable
PRINT '✓ Cola reseteada (Estatus 1,3 → 0)';
GO

-- C.2 (Opcional) — Si quieres re-procesar también Descartados de zona gris/excedente,
--     descomenta. Por defecto se respetan los Descartados ya marcados.
-- UPDATE dbo.MarcajeDispatchQueue
-- SET Estatus = 0, Intentos = 0, UltimoError = NULL, AsisteID = NULL,
--     ProcesadoEn = NULL, UltimoCambio = SYSDATETIME()
-- WHERE BaseDatos = N'cotailordev'
--   AND CAST(EventoFechaHora AS DATE) <= '20260513'
--   AND Estatus = 4;


-- ═════════════════════════════════════════════════════════════════════════════
-- SECCIÓN D — EJECUTAR sp_ProcessMarcajeQueue
-- ═════════════════════════════════════════════════════════════════════════════

PRINT '── Ejecutando sp_ProcessMarcajeQueue (todas las ventanas) ──';
EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = NULL, @BatchSize = 1000;
PRINT '✓ Procesamiento completado';
GO


-- ═════════════════════════════════════════════════════════════════════════════
-- SECCIÓN E — VERIFICACIÓN
-- ═════════════════════════════════════════════════════════════════════════════

-- E.1 Estado cola por día
PRINT '── Cola por día y estatus ──';
SELECT
    CAST(EventoFechaHora AS DATE) AS Fecha,
    Estatus,
    CASE Estatus
        WHEN 0 THEN 'Pendiente'
        WHEN 1 THEN 'Procesando'
        WHEN 2 THEN 'Hecho'
        WHEN 3 THEN 'Error'
        WHEN 4 THEN 'Descartado'
    END AS EstatusDesc,
    COUNT(*) AS Total
FROM dbo.MarcajeDispatchQueue
WHERE BaseDatos = N'cotailordev'
  AND CAST(EventoFechaHora AS DATE) <= '20260513'
GROUP BY CAST(EventoFechaHora AS DATE), Estatus
ORDER BY Fecha, Estatus;

-- E.2 Movimientos generados (incluye borradores con error)
PRINT '── Movimientos en ERP cotailordev hasta 2026-05-13 ──';
SELECT
    a.FechaAplicacion AS Fecha,
    a.Mov,
    a.Estatus,
    a.MovID,
    a.ID AS AsisteID,
    LEFT(a.Observaciones, 200) AS Observaciones,
    (SELECT COUNT(*) FROM cotailordev.dbo.AsisteD d WHERE d.ID = a.ID) AS Renglones
FROM cotailordev.dbo.Asiste a
WHERE a.Usuario        = 'INTELISIS'
  AND a.Mov            IN ('Entrada','SalidaComida','Entradacomida','Salida')
  AND a.FechaAplicacion <= '20260513'
ORDER BY a.FechaAplicacion, a.Mov;

-- E.3 Validar regla 1-por-tipo-por-día (debe regresar 0 filas)
PRINT '── Validación: ningún (Fecha, Mov) debe tener > 1 Asiste activo ──';
SELECT
    a.FechaAplicacion AS Fecha,
    a.Mov,
    COUNT(*) AS Activos
FROM cotailordev.dbo.Asiste a
WHERE a.Usuario        = 'INTELISIS'
  AND a.Mov            IN ('Entrada','SalidaComida','Entradacomida','Salida')
  AND a.FechaAplicacion <= '20260513'
  AND a.Estatus        NOT IN ('CANCELADO','CANCELAR')
GROUP BY a.FechaAplicacion, a.Mov
HAVING COUNT(*) > 1;

-- E.4 Errores residuales (borradores con problema)
PRINT '── Borradores con error (Asiste SINAFECTAR + Observaciones llena) ──';
SELECT
    a.FechaAplicacion AS Fecha,
    a.Mov,
    a.ID AS AsisteID,
    LEFT(a.Observaciones, 250) AS Error
FROM cotailordev.dbo.Asiste a
WHERE a.Usuario        = 'INTELISIS'
  AND a.Mov            IN ('Entrada','SalidaComida','Entradacomida','Salida')
  AND a.FechaAplicacion <= '20260513'
  AND a.MovID          IS NULL
  AND a.Estatus         NOT IN ('CANCELADO','CANCELAR')
  AND a.Observaciones  IS NOT NULL
ORDER BY a.FechaAplicacion, a.Mov;
GO

PRINT '════════════════════════════════════════════════════════════════════════════';
PRINT '  Replay terminado.';
PRINT '  - Si E.3 está vacío → regla 1-por-tipo-por-día OK.';
PRINT '  - Si E.4 muestra filas → revisar Observaciones del Asiste para diagnosticar.';
PRINT '════════════════════════════════════════════════════════════════════════════';
GO
