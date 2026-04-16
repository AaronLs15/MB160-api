/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  PASO 5 — DESPLIEGUE Y PRUEBA CON NUEVAS REGLAS HORARIAS (cotailordev)      ║
║                                                                              ║
║  Ejecutar sección por sección en SSMS (F5 por sección o todo de una vez).  ║
║                                                                              ║
║  Sección A: despliega SP + trigger con clasificación por hora               ║
║  Sección B: muestra pendientes en cola por ventana horaria                  ║
║  Sección C: procesa 5 registros por ventana (batch de prueba)               ║
║  Sección D: validación cruzada — AsisteD.Registro vs hora esperada          ║
║                                                                              ║
║  ⚠ Reemplaza <NOMBRE_BASE_CHECADOR> antes de ejecutar.                      ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

USE <NOMBRE_BASE_CHECADOR>;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- SECCIÓN A — Desplegar SP y trigger con nuevas reglas horarias
-- ─────────────────────────────────────────────────────────────────────────────

-- Trigger: encola todos los marcajes de empresas activas (sin filtro por Punch)
CREATE OR ALTER TRIGGER dbo.tr_AsistenciaMarcaje_DispatchQueue
ON dbo.AsistenciaMarcaje
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    -- Formato: primer dígito = empresa, valor completo = Personal en AsisteD
    -- Ejemplo: '50076' → empresa '5' (cotailordev), Personal = 50076
    -- El tipo de Registro (Entrada/Comida/Salida) se determina por hora en sp_ProcessMarcajeQueue.
    INSERT INTO dbo.MarcajeDispatchQueue
        (AsistenciaMarcajeID, EmpresaID, BaseDatos, PersonaID, Punch, EventoFechaHora)
    SELECT
        i.AsistenciaMarcajeID,
        CAST(LEFT(i.UsuarioDispositivo, 1) AS INT)   AS EmpresaID,
        ec.BaseDatos,
        CAST(i.UsuarioDispositivo AS INT)            AS PersonaID,
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
PRINT '✓ Trigger tr_AsistenciaMarcaje_DispatchQueue actualizado';
GO

-- SP orquestador — modelo 4 movimientos por empleado por día
CREATE OR ALTER PROCEDURE dbo.sp_ProcessMarcajeQueue
    @TipoCorte TINYINT = NULL,   -- 0=Entrada (<12:00)  4=Comida (12:50-15:59)  1=Salida (>=16:00)  NULL=todos
    @BatchSize INT     = 200
AS
/*
    Modelo: 1 Asiste por (empleado, día, TipoMov). Múltiples AsisteD por Asiste.
    TipoMov: 'Entrada' | 'SalidaComida' | 'EntradaComida' | 'Salida'
    AsisteD.Registro = Asiste.Mov. Cada grupo en transacción explícita.
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

    -- Clasificación base
    UPDATE @batch SET TipoMov =
        CASE WHEN CAST(EventoFechaHora AS TIME) < '12:00:00' THEN N'Entrada'
             WHEN CAST(EventoFechaHora AS TIME) < '12:50:00' THEN N'ZONAGRIS'
             WHEN CAST(EventoFechaHora AS TIME) < '16:00:00' THEN N'COMIDA_TBD'
             ELSE                                                  N'Salida' END;

    -- Split comida (1ro=SalidaComida, 2do=EntradaComida, 3ro+=DESCARTAR)
    ;WITH comida AS (
        SELECT MarcajeDispatchQueueID,
               ROW_NUMBER() OVER (
                   PARTITION BY BaseDatos, PersonaID, CAST(EventoFechaHora AS DATE)
                   ORDER BY EventoFechaHora) AS rn
        FROM @batch WHERE TipoMov = N'COMIDA_TBD'
    )
    UPDATE b SET b.TipoMov = CASE WHEN c.rn=1 THEN N'SalidaComida'
                                  WHEN c.rn=2 THEN N'EntradaComida'
                                  ELSE             N'DESCARTAR' END
    FROM @batch b INNER JOIN comida c ON c.MarcajeDispatchQueueID = b.MarcajeDispatchQueueID;

    -- Descartar zona gris y excedente comida
    UPDATE dbo.MarcajeDispatchQueue
    SET Estatus=4,
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
        @GrpDB SYSNAME, @GrpEmpresaID INT, @GrpCode NVARCHAR(50),
        @GrpPersonaID INT, @GrpFecha DATE, @GrpTipoMov NVARCHAR(20),
        @QueueID BIGINT, @MarcajeID BIGINT, @FechaEvento DATETIME2(0), @HoraStr NCHAR(5),
        @SQL NVARCHAR(MAX), @Params NVARCHAR(MAX),
        @AsisteID INT, @MovIDPost NVARCHAR(50), @ErrMsg NVARCHAR(4000);

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

            -- INSERT Asiste (1 por grupo)
            SET @AsisteID = NULL;
            SET @SQL = N'
                DECLARE @ids TABLE (ID INT);
                INSERT INTO [' + @GrpDB + N'].dbo.Asiste
                (Empresa, Mov, FechaEmision, FechaAplicacion,
                 Estatus, Usuario, Ejercicio, Periodo, FechaRegistro,
                 Sucursal, GenerarPoliza, SincroC, SucursalOrigen,
                 Logico1, Logico2, Logico3, Logico4, Logico5, Logico6, Logico7, Logico8, Logico9)
                OUTPUT INSERTED.ID INTO @ids
                VALUES (@EmpresaCode, @TipoMov, @Fecha, @Fecha,
                        ''SINAFECTAR'', ''INTELISIS'', YEAR(@Fecha), MONTH(@Fecha), SYSDATETIME(),
                        1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
                SELECT @AsisteID = ID FROM @ids;';
            SET @Params = N'@EmpresaCode NVARCHAR(50), @TipoMov NVARCHAR(20), @Fecha DATE, @AsisteID INT OUTPUT';
            EXEC sp_executesql @SQL, @Params,
                @EmpresaCode=@GrpCode, @TipoMov=@GrpTipoMov, @Fecha=@GrpFecha, @AsisteID=@AsisteID OUTPUT;

            IF @AsisteID IS NULL RAISERROR(N'OUTPUT INSERTED.ID regresó NULL.', 16, 1);

            -- INSERT AsisteD: un renglón por marcaje del grupo
            DECLARE curMarcajes CURSOR LOCAL FAST_FORWARD FOR
                SELECT MarcajeDispatchQueueID, AsistenciaMarcajeID, EventoFechaHora
                FROM @batch
                WHERE BaseDatos=@GrpDB AND PersonaID=@GrpPersonaID
                  AND CAST(EventoFechaHora AS DATE)=@GrpFecha AND TipoMov=@GrpTipoMov
                ORDER BY EventoFechaHora;

            OPEN curMarcajes;
            FETCH NEXT FROM curMarcajes INTO @QueueID, @MarcajeID, @FechaEvento;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @HoraStr = CONVERT(NCHAR(5), CAST(@FechaEvento AS TIME), 108);
                SET @SQL = N'
                    DECLARE @renglon INT;
                    SELECT @renglon = ISNULL(MAX(Renglon),0)+1 FROM [' + @GrpDB + N'].dbo.AsisteD;
                    INSERT INTO [' + @GrpDB + N'].dbo.AsisteD
                    (ID, Renglon, Personal, Registro, HoraRegistro, FechaD, FechaA, Fecha, Sucursal,
                     Logico1, Logico2, Logico3, Logico4, Logico5)
                    VALUES (@AsisteID, @renglon, @PersonaID, @TipoMov, @Hora,
                            @Fecha, @Fecha, @Fecha, 1, 0, 0, 0, 0, 0);';
                SET @Params = N'@AsisteID INT, @PersonaID INT, @TipoMov NVARCHAR(20), @Hora NCHAR(5), @Fecha DATE';
                EXEC sp_executesql @SQL, @Params,
                    @AsisteID=@AsisteID, @PersonaID=@GrpPersonaID,
                    @TipoMov=@GrpTipoMov, @Hora=@HoraStr, @Fecha=@GrpFecha;

                FETCH NEXT FROM curMarcajes INTO @QueueID, @MarcajeID, @FechaEvento;
            END;
            CLOSE curMarcajes; DEALLOCATE curMarcajes;

            -- spAfectar
            SET @SQL = N'EXEC [' + @GrpDB + N'].dbo.spAfectar ''ASIS'', @AsisteID, ''AFECTAR'', ''Todo'', NULL, ''INTELISIS'', @Estacion=1, @ensilencio=1;';
            EXEC sp_executesql @SQL, N'@AsisteID INT', @AsisteID=@AsisteID;

            -- Validar MovID post-spAfectar
            SET @MovIDPost = NULL;
            SET @SQL = N'SELECT @MovIDPost = MovID FROM [' + @GrpDB + N'].dbo.Asiste WHERE ID = @AsisteID;';
            EXEC sp_executesql @SQL, N'@AsisteID INT, @MovIDPost NVARCHAR(50) OUTPUT',
                @AsisteID=@AsisteID, @MovIDPost=@MovIDPost OUTPUT;

            IF @MovIDPost IS NULL
                RAISERROR(N'spAfectar no generó MovID para Asiste.ID=%d — se reintentará en el siguiente ciclo.', 16, 1, @AsisteID);

            -- Marcar grupo como Hecho
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
            IF CURSOR_STATUS('local','curMarcajes') >= 0  CLOSE curMarcajes;
            IF CURSOR_STATUS('local','curMarcajes') >= -1 DEALLOCATE curMarcajes;
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
PRINT '✓ SP dbo.sp_ProcessMarcajeQueue actualizado (modelo 4 movimientos por empleado por día)';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- SECCIÓN B — Estado de la cola ANTES del procesamiento
--             Ver cuántos pendientes hay por ventana horaria
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── Pendientes en cola por ventana horaria (ANTES) ──';

SELECT
    CASE
        WHEN CAST(EventoFechaHora AS TIME) <  '12:00:00' THEN '0  Entrada  (< 12:00)'
        WHEN CAST(EventoFechaHora AS TIME) <  '12:50:00' THEN '!  Zona gris (12:00–12:49) → Descartado'
        WHEN CAST(EventoFechaHora AS TIME) <  '16:00:00' THEN '4  Comida   (12:50–15:59)'
        ELSE                                                   '1  Salida   (>= 16:00)'
    END                      AS VentanaHoraria,
    COUNT(*)                 AS Pendientes,
    MIN(EventoFechaHora)     AS MasAntiguo,
    MAX(EventoFechaHora)     AS MasReciente
FROM dbo.MarcajeDispatchQueue
WHERE Estatus IN (0, 3)
  AND BaseDatos = N'cotailordev'
GROUP BY
    CASE
        WHEN CAST(EventoFechaHora AS TIME) <  '12:00:00' THEN '0  Entrada  (< 12:00)'
        WHEN CAST(EventoFechaHora AS TIME) <  '12:50:00' THEN '!  Zona gris (12:00–12:49) → Descartado'
        WHEN CAST(EventoFechaHora AS TIME) <  '16:00:00' THEN '4  Comida   (12:50–15:59)'
        ELSE                                                   '1  Salida   (>= 16:00)'
    END
ORDER BY VentanaHoraria;
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- SECCIÓN C — Batch de prueba: 5 registros por ventana horaria
--             ⚠ Revisar Sección B primero. Si no hay pendientes, STOP.
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── Procesando batch de prueba: 5 registros por ventana ──';

EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 0, @BatchSize = 5;   -- Entrada  (< 12:00)
PRINT '✓ Batch Entrada procesado';

EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 4, @BatchSize = 5;   -- Comida   (12:50–15:59)
PRINT '✓ Batch Comida procesado';

EXEC dbo.sp_ProcessMarcajeQueue @TipoCorte = 1, @BatchSize = 5;   -- Salida   (>= 16:00)
PRINT '✓ Batch Salida procesado';
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- SECCIÓN D — Validación cruzada
--             Compara AsisteD.Registro con el tipo esperado según la hora
--             Cualquier fila con '*** MISMATCH ***' indica un problema
-- ─────────────────────────────────────────────────────────────────────────────
PRINT '── Validación cruzada: tipo esperado por hora vs insertado en AsisteD ──';

SELECT
    am.UsuarioDispositivo,
    am.EventoFechaHora,
    CAST(am.EventoFechaHora AS TIME)            AS HoraEvento,
    am.Punch                                    AS PunchDispositivo,
    CASE
        WHEN CAST(am.EventoFechaHora AS TIME) <  '12:00:00' THEN 'Entrada'
        WHEN CAST(am.EventoFechaHora AS TIME) <  '12:50:00' THEN '(zona gris)'
        WHEN CAST(am.EventoFechaHora AS TIME) <  '16:00:00' THEN 'Comida'
        ELSE                                                      'Salida'
    END                                         AS TipoEsperado,
    d.Registro                                  AS TipoInsertado,
    CASE
        WHEN d.Registro IS NULL THEN '⚠ Sin registro en AsisteD'
        WHEN d.Registro = CASE
                WHEN CAST(am.EventoFechaHora AS TIME) <  '12:00:00' THEN 'Entrada'
                WHEN CAST(am.EventoFechaHora AS TIME) <  '12:50:00' THEN '(zona gris)'
                WHEN CAST(am.EventoFechaHora AS TIME) <  '16:00:00' THEN 'Comida'
                ELSE 'Salida'
             END                               THEN 'OK'
        ELSE                                        '*** MISMATCH ***'
    END                                         AS Resultado,
    q.AsisteID,
    q.ProcesadoEn
FROM dbo.MarcajeDispatchQueue q
INNER JOIN dbo.AsistenciaMarcaje am ON am.AsistenciaMarcajeID = q.AsistenciaMarcajeID
LEFT  JOIN cotailordev.dbo.AsisteD d  ON d.ID = q.AsisteID
WHERE q.Estatus   = 2
  AND q.BaseDatos = N'cotailordev'
  AND q.AsisteID  IS NOT NULL
ORDER BY am.EventoFechaHora;
GO

-- Resumen de validación
PRINT '── Resumen de validación ──';

SELECT
    CASE
        WHEN d.Registro IS NULL THEN '⚠ Sin registro en AsisteD'
        WHEN d.Registro = CASE
                WHEN CAST(am.EventoFechaHora AS TIME) <  '12:00:00' THEN 'Entrada'
                WHEN CAST(am.EventoFechaHora AS TIME) <  '12:50:00' THEN '(zona gris)'
                WHEN CAST(am.EventoFechaHora AS TIME) <  '16:00:00' THEN 'Comida'
                ELSE 'Salida'
             END                               THEN 'OK'
        ELSE                                        '*** MISMATCH ***'
    END                                         AS Resultado,
    COUNT(*)                                    AS Total
FROM dbo.MarcajeDispatchQueue q
INNER JOIN dbo.AsistenciaMarcaje am ON am.AsistenciaMarcajeID = q.AsistenciaMarcajeID
LEFT  JOIN cotailordev.dbo.AsisteD d  ON d.ID = q.AsisteID
WHERE q.Estatus   = 2
  AND q.BaseDatos = N'cotailordev'
  AND q.AsisteID  IS NOT NULL
GROUP BY
    CASE
        WHEN d.Registro IS NULL THEN '⚠ Sin registro en AsisteD'
        WHEN d.Registro = CASE
                WHEN CAST(am.EventoFechaHora AS TIME) <  '12:00:00' THEN 'Entrada'
                WHEN CAST(am.EventoFechaHora AS TIME) <  '12:50:00' THEN '(zona gris)'
                WHEN CAST(am.EventoFechaHora AS TIME) <  '16:00:00' THEN 'Comida'
                ELSE 'Salida'
             END                               THEN 'OK'
        ELSE                                        '*** MISMATCH ***'
    END;
GO

PRINT '';
PRINT '══ Si el resumen muestra solo "OK" → nuevas reglas funcionan correctamente ══';
PRINT 'Siguiente: pilot_03_verificacion.sql para vista completa de la cola.';
GO
