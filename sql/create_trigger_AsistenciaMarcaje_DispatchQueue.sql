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
    -- El tipo de Registro (Entrada/Comida/Salida) se determina por hora del evento
    -- en sp_ProcessMarcajeQueue — no por el valor Punch del dispositivo.
    -- Se encolan todos los marcajes de empresas activas (sin filtro por Punch).

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
    WHERE LEN(i.UsuarioDispositivo) >= 2
      AND ISNUMERIC(i.UsuarioDispositivo) = 1;
END;
GO
