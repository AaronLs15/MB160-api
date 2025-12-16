IF OBJECT_ID(N'dbo.MB160UserSyncQueue', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.MB160UserSyncQueue
    (
        MB160UserSyncQueueID BIGINT IDENTITY(1,1) NOT NULL,
        EmpresaID            INT NOT NULL,
        PersonaID            INT NOT NULL,
        UsuarioDispositivo   NVARCHAR(50) NOT NULL,
        UsuarioNombre        NVARCHAR(150) NOT NULL,

        Estatus              TINYINT NOT NULL CONSTRAINT DF_MB160UserSyncQueue_Estatus DEFAULT(0),
        -- 0=Pendiente, 1=Procesando, 2=Hecho, 3=Error

        Intentos             INT NOT NULL CONSTRAINT DF_MB160UserSyncQueue_Intentos DEFAULT(0),
        UltimoError          NVARCHAR(4000) NULL,

        FechaRegistro        DATETIME2(3) NOT NULL CONSTRAINT DF_MB160UserSyncQueue_FechaRegistro DEFAULT(SYSDATETIME()),
        UltimoCambio         DATETIME2(3) NOT NULL CONSTRAINT DF_MB160UserSyncQueue_UltimoCambio DEFAULT(SYSDATETIME()),
        ProcesadoEn          DATETIME2(3) NULL,

        CONSTRAINT PK_MB160UserSyncQueue PRIMARY KEY CLUSTERED (MB160UserSyncQueueID),
        CONSTRAINT UQ_MB160UserSyncQueue UNIQUE (EmpresaID, PersonaID)
    );

    CREATE INDEX IX_MB160UserSyncQueue_Estatus
    ON dbo.MB160UserSyncQueue (Estatus, MB160UserSyncQueueID);
END;
GO
