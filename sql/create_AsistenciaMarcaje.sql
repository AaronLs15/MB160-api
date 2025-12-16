IF OBJECT_ID(N'dbo.AsistenciaMarcaje', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.AsistenciaMarcaje
    (
        AsistenciaMarcajeID   BIGINT IDENTITY(1,1) NOT NULL,

        DispositivoSerial    NVARCHAR(50) NOT NULL,
        DispositivoIP        VARCHAR(45) NULL,
        UsuarioDispositivo   NVARCHAR(50) NOT NULL,

        EventoFechaHora      DATETIME2(0) NOT NULL,  -- HORA LOCAL
        Punch                TINYINT NOT NULL CONSTRAINT DF_AsistenciaMarcaje_Punch DEFAULT(0),
        Estado               TINYINT NOT NULL CONSTRAINT DF_AsistenciaMarcaje_Estado DEFAULT(0),

        WorkCode             INT NULL,

        TieneMovimientos     BIT NOT NULL CONSTRAINT DF_AsistenciaMarcaje_TieneMovimientos DEFAULT(0),
        FechaRegistro        DATETIME2(3) NOT NULL CONSTRAINT DF_AsistenciaMarcaje_FechaRegistro DEFAULT(SYSDATETIME()),
        UltimoCambio         DATETIME2(3) NOT NULL CONSTRAINT DF_AsistenciaMarcaje_UltimoCambio DEFAULT(SYSDATETIME()),
        RowVer               ROWVERSION NOT NULL,

        CONSTRAINT PK_AsistenciaMarcaje PRIMARY KEY CLUSTERED (AsistenciaMarcajeID),

        CONSTRAINT UQ_AsistenciaMarcaje_Dedupe UNIQUE
        (
            DispositivoSerial,
            UsuarioDispositivo,
            EventoFechaHora,
            Punch,
            Estado
        )
    );

    CREATE INDEX IX_AsistenciaMarcaje_Usuario_Fecha
        ON dbo.AsistenciaMarcaje (UsuarioDispositivo, EventoFechaHora DESC);

    CREATE INDEX IX_AsistenciaMarcaje_Device_Fecha
        ON dbo.AsistenciaMarcaje (DispositivoSerial, EventoFechaHora DESC);
END;
GO

ALTER TABLE dbo.AsistenciaMarcaje
ADD UsuarioNombre NVARCHAR(150) NULL;
GO