CREATE OR ALTER TRIGGER dbo.tr_AsistenciaMarcaje_DispatchQueue
ON dbo.AsistenciaMarcaje
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    -- Solo se encolan los Punch que corresponden a un tipo de registro válido:
    --   0 = Entrada
    --   1 = Salida
    --   4 = Comida
    -- Los demás valores (2, 3, 5, etc.) se ignoran silenciosamente.
    --
    -- El JOIN con EmpresaConfig filtra marcajes cuyo prefijo de empresa
    -- no exista o esté inactivo (p. ej. dispositivos de prueba, etc.).
    --
    -- LEN = 9 descarta IDs mal formados antes de intentar el CAST.

    INSERT INTO dbo.MarcajeDispatchQueue
        (AsistenciaMarcajeID, EmpresaID, BaseDatos, PersonaID, Punch, EventoFechaHora)
    SELECT
        i.AsistenciaMarcajeID,
        CAST(LEFT(i.UsuarioDispositivo, 3) AS INT)  AS EmpresaID,
        ec.BaseDatos,
        CAST(RIGHT(i.UsuarioDispositivo, 6) AS INT) AS PersonaID,
        i.Punch,
        i.EventoFechaHora
    FROM inserted i
    INNER JOIN dbo.EmpresaConfig ec
        ON  ec.EmpresaPrefix = LEFT(i.UsuarioDispositivo, 3)
        AND ec.Activo = 1
    WHERE i.Punch IN (0, 1, 4)
      AND LEN(i.UsuarioDispositivo) = 9;
END;
GO
