IF OBJECT_ID(N'dbo.EmpresaConfig', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.EmpresaConfig
    (
        EmpresaConfigID  INT            IDENTITY(1,1) NOT NULL,
        EmpresaID        INT            NOT NULL,         -- 2, 3, 4, 5
        EmpresaPrefix    CHAR(1)        NOT NULL,         -- '2','3','4','5' (primer dígito de UsuarioDispositivo)
        BaseDatos        SYSNAME        NOT NULL,         -- nombre de la DB del ERP en esta misma instancia
        CodigoEmpresa    NVARCHAR(50)   NOT NULL,         -- valor de Empresa en Asiste/AsisteD (ej. 'GNS')
        Activo           BIT            NOT NULL CONSTRAINT DF_EmpresaConfig_Activo DEFAULT(1),
        FechaRegistro    DATETIME2(3)   NOT NULL CONSTRAINT DF_EmpresaConfig_FechaRegistro DEFAULT(SYSDATETIME()),

        CONSTRAINT PK_EmpresaConfig           PRIMARY KEY CLUSTERED (EmpresaConfigID),
        CONSTRAINT UQ_EmpresaConfig_Prefix    UNIQUE (EmpresaPrefix),
        CONSTRAINT UQ_EmpresaConfig_EmpresaID UNIQUE (EmpresaID)
    );

    INSERT INTO dbo.EmpresaConfig (EmpresaID, EmpresaPrefix, BaseDatos, CodigoEmpresa) VALUES
    (2, '2', N'kingv7',       N'GNS'),
    (3, '3', N'obsidianav7',  N'GNS'),
    (4, '4', N'bbgv7',        N'GNS'),
    (5, '5', N'cotailordev',  N'GNS');
END
ELSE
BEGIN
    -- Agregar columna CodigoEmpresa si la tabla ya existía sin ella
    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'dbo.EmpresaConfig')
          AND name = 'CodigoEmpresa'
    )
    BEGIN
        ALTER TABLE dbo.EmpresaConfig
            ADD CodigoEmpresa NVARCHAR(50) NOT NULL
            CONSTRAINT DF_EmpresaConfig_CodigoEmpresa DEFAULT(N'GNS');

        -- Actualizar registros existentes con el código correcto
        UPDATE dbo.EmpresaConfig SET CodigoEmpresa = N'GNS';

        -- Quitar el DEFAULT temporal (ya no es necesario)
        ALTER TABLE dbo.EmpresaConfig
            DROP CONSTRAINT DF_EmpresaConfig_CodigoEmpresa;
    END;
END;
GO
