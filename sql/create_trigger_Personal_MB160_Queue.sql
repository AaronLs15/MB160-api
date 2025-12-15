CREATE OR ALTER TRIGGER dbo.tr_Personal_MB160_Queue
ON dbo.Personal
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH src AS (
        SELECT
            i.Empresa,
            i.Personal,
            CONCAT(CAST(i.Nombre AS NVARCHAR(150)) + ' ', isnull(i.ApellidoPaterno,'') + ' ', ISNULL(i.ApellidoMaterno,'')  ) AS UsuarioNombre,
            RIGHT(REPLICATE('0', 3) + CAST(i.Empresa AS VARCHAR(10)), 3)
            + RIGHT(REPLICATE('0', 6) + CAST(i.Personal AS VARCHAR(10)), 6) AS UsuarioDispositivo
        FROM inserted i
        WHERE i.Empresa IS NOT NULL
          AND i.Personal IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(i.Nombre)), '') IS NOT NULL
    )
    MERGE dbo.MB160UserSyncQueue AS t
    USING src AS s
      ON t.EmpresaID = s.Empresa AND t.PersonaID = s.Personal
    WHEN MATCHED THEN
      UPDATE SET
        t.UsuarioDispositivo = s.UsuarioDispositivo,
        t.UsuarioNombre = s.UsuarioNombre,
        t.Estatus = 0,
        t.UltimoError = NULL,
        t.UltimoCambio = SYSDATETIME(),
        t.ProcesadoEn = NULL
    WHEN NOT MATCHED THEN
      INSERT (EmpresaID, PersonaID, UsuarioDispositivo, UsuarioNombre)
      VALUES (s.Empresa, s.Personal, s.UsuarioDispositivo, s.UsuarioNombre);
END;
GO
