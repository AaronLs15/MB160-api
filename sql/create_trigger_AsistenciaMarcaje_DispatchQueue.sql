CREATE OR ALTER TRIGGER dbo.tr_AsistenciaMarcaje_DispatchQueue
ON dbo.AsistenciaMarcaje
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    -- Formato real de UsuarioDispositivo: primer dígito = empresa, resto = PersonaID
    -- Ejemplos: '50076' → empresa 5 (cotailordev), PersonaID 76
    --           '212345' → empresa 2 (kingv7), PersonaID 12345
    --
    -- Solo se encolan Punch válidos: 0=Entrada, 1=Salida, 4=Comida.
    -- El JOIN con EmpresaConfig filtra empresas desconocidas o inactivas.

    INSERT INTO dbo.MarcajeDispatchQueue
        (AsistenciaMarcajeID, EmpresaID, BaseDatos, PersonaID, Punch, EventoFechaHora)
    SELECT
        i.AsistenciaMarcajeID,
        CAST(LEFT(i.UsuarioDispositivo, 1) AS INT)                          AS EmpresaID,
        ec.BaseDatos,
        CAST(SUBSTRING(i.UsuarioDispositivo, 2, LEN(i.UsuarioDispositivo))  AS INT) AS PersonaID,
        i.Punch,
        i.EventoFechaHora
    FROM inserted i
    INNER JOIN dbo.EmpresaConfig ec
        ON  ec.EmpresaPrefix = LEFT(i.UsuarioDispositivo, 1)
        AND ec.Activo = 1
    WHERE i.Punch IN (0, 1, 4)
      AND LEN(i.UsuarioDispositivo) >= 2     -- al menos 1 dígito empresa + 1 de PersonaID
      AND ISNUMERIC(i.UsuarioDispositivo) = 1;  -- descartar IDs no numéricos
END;
GO
