IF OBJECT_ID(N'dbo.EmpresaConfig', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.EmpresaConfig
    (
        EmpresaConfigID  INT          IDENTITY(1,1) NOT NULL,
        EmpresaID        INT          NOT NULL,         -- 2, 3, 4, 5
        EmpresaPrefix    CHAR(1)      NOT NULL,         -- '2', '3', '4', '5' (primer dígito de UsuarioDispositivo)
        BaseDatos        SYSNAME      NOT NULL,         -- nombre de la DB del ERP en esta misma instancia
        Activo           BIT          NOT NULL CONSTRAINT DF_EmpresaConfig_Activo DEFAULT(1),
        FechaRegistro    DATETIME2(3) NOT NULL CONSTRAINT DF_EmpresaConfig_FechaRegistro DEFAULT(SYSDATETIME()),

        CONSTRAINT PK_EmpresaConfig PRIMARY KEY CLUSTERED (EmpresaConfigID),
        CONSTRAINT UQ_EmpresaConfig_Prefix  UNIQUE (EmpresaPrefix),
        CONSTRAINT UQ_EmpresaConfig_EmpresaID UNIQUE (EmpresaID)
    );

    INSERT INTO dbo.EmpresaConfig (EmpresaID, EmpresaPrefix, BaseDatos) VALUES
    (2, '2', N'kingv7'),
    (3, '3', N'obsidianav7'),
    (4, '4', N'bbgv7'),
    (5, '5', N'cotailordev');
END;
GO
