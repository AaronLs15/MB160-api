IF OBJECT_ID(N'dbo.EmpresaConfig', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.EmpresaConfig
    (
        EmpresaConfigID  INT          IDENTITY(1,1) NOT NULL,
        EmpresaID        INT          NOT NULL,         -- 2, 3, 4, 5
        EmpresaPrefix    CHAR(3)      NOT NULL,         -- '002', '003', '004', '005'
        BaseDatos        SYSNAME      NOT NULL,         -- nombre de la DB del ERP en esta misma instancia
        Activo           BIT          NOT NULL CONSTRAINT DF_EmpresaConfig_Activo DEFAULT(1),
        FechaRegistro    DATETIME2(3) NOT NULL CONSTRAINT DF_EmpresaConfig_FechaRegistro DEFAULT(SYSDATETIME()),

        CONSTRAINT PK_EmpresaConfig PRIMARY KEY CLUSTERED (EmpresaConfigID),
        CONSTRAINT UQ_EmpresaConfig_Prefix  UNIQUE (EmpresaPrefix),
        CONSTRAINT UQ_EmpresaConfig_EmpresaID UNIQUE (EmpresaID)
    );

    INSERT INTO dbo.EmpresaConfig (EmpresaID, EmpresaPrefix, BaseDatos) VALUES
    (2, '002', N'kingv7'),
    (3, '003', N'obsidianav7'),
    (4, '004', N'bbgv7'),
    (5, '005', N'cotailordev');
END;
GO
