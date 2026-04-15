CREATE OR ALTER TRIGGER dbo.tr_AsistenciaMarcaje_DispatchQueue
ON dbo.AsistenciaMarcaje
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    -- Formato de UsuarioDispositivo: primer dígito = empresa, valor completo = Personal en AsisteD
    -- Ejemplos: '50076' → empresa 5 (cotailordev), Personal = 50076
    --           '337'   → empresa 3 (obsidianav7),  Personal = 337
    --
    -- La relación con el ERP es: AsistenciaMarcaje.UsuarioDispositivo = AsisteD.Personal
    -- Por eso PersonaID guarda el UsuarioDispositivo completo convertido a INT.
    --
    -- Solo se encolan Punch válidos: 0=Entrada, 1=Salida, 4=Comida.

    INSERT INTO dbo.MarcajeDispatchQueue
        (AsistenciaMarcajeID, EmpresaID, BaseDatos, PersonaID, Punch, EventoFechaHora)
    SELECT
        i.AsistenciaMarcajeID,
        CAST(LEFT(i.UsuarioDispositivo, 1) AS INT)   AS EmpresaID,
        ec.BaseDatos,
        CAST(i.UsuarioDispositivo AS INT)            AS PersonaID,  -- ← valor completo
        i.Punch,
        i.EventoFechaHora
    FROM inserted i
    INNER JOIN dbo.EmpresaConfig ec
        ON  ec.EmpresaPrefix = LEFT(i.UsuarioDispositivo, 1)
        AND ec.Activo = 1
    WHERE i.Punch IN (0, 1, 4)
      AND LEN(i.UsuarioDispositivo) >= 2
      AND ISNUMERIC(i.UsuarioDispositivo) = 1;
END;
GO
