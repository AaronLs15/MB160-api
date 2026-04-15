IF OBJECT_ID(N'dbo.MarcajeDispatchQueue', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.MarcajeDispatchQueue
    (
        MarcajeDispatchQueueID  BIGINT         IDENTITY(1,1) NOT NULL,
        AsistenciaMarcajeID     BIGINT         NOT NULL,           -- referencia a dbo.AsistenciaMarcaje
        EmpresaID               INT            NOT NULL,           -- 2, 3, 4 ó 5
        BaseDatos               SYSNAME        NOT NULL,           -- DB destino del ERP
        PersonaID               INT            NOT NULL,           -- últimos 6 dígitos de UsuarioDispositivo
        Punch                   TINYINT        NOT NULL,           -- 0=Entrada, 1=Salida, 4=Comida
        EventoFechaHora         DATETIME2(0)   NOT NULL,

        Estatus                 TINYINT        NOT NULL CONSTRAINT DF_MDQ_Estatus DEFAULT(0),
        -- 0=Pendiente, 1=Procesando, 2=Hecho, 3=Error

        Intentos                INT            NOT NULL CONSTRAINT DF_MDQ_Intentos DEFAULT(0),
        UltimoError             NVARCHAR(4000) NULL,
        AsisteID                INT            NULL,               -- ID generado en Asiste del ERP tras el INSERT

        FechaRegistro           DATETIME2(3)   NOT NULL CONSTRAINT DF_MDQ_FechaRegistro DEFAULT(SYSDATETIME()),
        UltimoCambio            DATETIME2(3)   NOT NULL CONSTRAINT DF_MDQ_UltimoCambio  DEFAULT(SYSDATETIME()),
        ProcesadoEn             DATETIME2(3)   NULL,

        CONSTRAINT PK_MarcajeDispatchQueue PRIMARY KEY CLUSTERED (MarcajeDispatchQueueID),

        -- Garantiza que el mismo marcaje no se despache dos veces
        CONSTRAINT UQ_MarcajeDispatchQueue_Marcaje UNIQUE (AsistenciaMarcajeID)
    );

    -- Índice para que el SP de procesamiento encuentre rápido los pendientes/errores
    CREATE INDEX IX_MDQ_Estatus
        ON dbo.MarcajeDispatchQueue (Estatus, MarcajeDispatchQueueID);
END;
GO
