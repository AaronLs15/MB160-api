# MB160 Service (SQL Server + Collector + API)

Incluye:
- Tests rápidos de conectividad/dedupe a SQL Server sin MB160.
- Collector que descarga marcajes del MB160 y los inserta en SQL Server (hora local) + lookup de nombre.
- Worker de sincronización de usuarios (cola + trigger en `dbo.Personal`).
- API (FastAPI) para consultar marcajes.

## Estructura rápida

```
.
├── src/mb160_service/
│   ├── api/main.py              # FastAPI app
│   ├── collector/poller.py      # descarga marcajes MB160
│   ├── collector/user_sync.py   # crea/actualiza usuarios en MB160
│   ├── config.py                # settings desde .env
│   ├── db.py                    # SQLAlchemy engine helper
│   ├── logging.py               # logger común
│   └── utils/simulator.py
├── scripts/
│   ├── run_api.py
│   ├── run_collector.py
│   ├── run_daily_pull.py       # ejecuta un pull unico (para cron)
│   ├── run_scheduled_pull.py   # scheduler diario con bajo consumo
│   ├── run_health_check.py
│   └── run_live_ingest.py       # opcional: prueba live_capture
├── sql/
│   ├── create_AsistenciaMarcaje.sql
│   ├── create_MB160UserSyncQueue.sql
│   └── create_trigger_Personal_MB160_Queue.sql
├── tests/
│   └── test_db_insert.py (+ pruebas MB160_*)
└── logs/ (gitignored)
```

---

## 1) SQL Server

### 1.1 Tabla de marcajes
Ejecuta `sql/create_AsistenciaMarcaje.sql`

Asegúrate de que exista el UNIQUE para dedupe: `UQ_AsistenciaMarcaje_Dedupe`.

### 1.2 Agregar `UsuarioNombre` a marcajes (si aún no existe)

```sql
ALTER TABLE dbo.AsistenciaMarcaje
ADD UsuarioNombre NVARCHAR(150) NULL;
GO
```

### 1.3 Cola para alta automática de usuarios en MB160

```sql
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
```

### 1.4 Trigger en `dbo.Personal` para encolar alta en MB160

```sql
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
```

---

## 2) Variables de entorno

1. Copia `.env.example` a `.env` (no comitear).
2. Llena credenciales.

Ejemplo completo:

```env
# ---- SQL Server ----
SQLSERVER_HOST=10.20.30.40
SQLSERVER_PORT=1433
SQLSERVER_DB=db_name
SQLSERVER_USER=miusuario
SQLSERVER_PASSWORD=super_secreto

SQLSERVER_DRIVER=ODBC Driver 18 for SQL Server
SQLSERVER_ENCRYPT=yes
SQLSERVER_TRUST_CERT=yes

# ---- MB160 ----
MB160_IP=192.168.1.50
MB160_PORT=4370

# ---- Attendance collector ----
PULL_INTERVAL_SECONDS=60

# ---- User sync worker ----
USER_SYNC_INTERVAL_SECONDS=10
USER_SYNC_BATCH_SIZE=20

# ---- API ----
API_PORT=8000
```

> VPN: si el SQL Server está en red remota, conecta la VPN antes de correr pruebas/servicio/API.

---

## 3) Instalación y smoke tests (macOS / Windows)

Requisitos: Python 3.11+, ODBC Driver 18 para SQL Server. En macOS instala con brew:

```bash
brew update
brew install unixodbc
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install msodbcsql18
```

Crear venv e instalar deps (macOS/Linux):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

En Windows (PowerShell):

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Smoke tests (con `.env` listo):

```bash
python scripts/run_health_check.py
python tests/test_db_insert.py
```

Salida esperada:

* `OK: DB=...`
* `OK: insert 1 realizado`
* `OK: deduplicación funciona (IntegrityError por UNIQUE)`

---

## 4) API (FastAPI)

Dev server:

```bash
python scripts/run_api.py
```

Por defecto:

* API: http://localhost:8000
* Swagger UI: http://localhost:8000/docs

### Endpoints

* `GET /health` → verifica conectividad a SQL Server
* `GET /marks` → lista marcajes con filtros `user_id`, `device_serial`, `dt_from`, `dt_to`, `limit`, `offset`
* `GET /marks/{mark_id}` → obtiene un marcaje por `AsistenciaMarcajeID`

---

## 5) Collector + user sync (dev)

Ejecuta ambos workers en paralelo (descarga marcajes + crea usuarios en MB160):

```bash
python scripts/run_collector.py
```

Logs: `logs/service.log`

¿Qué hace?

* Conecta al MB160 (TCP/IP)
* Descarga marcajes (`get_attendance()`), inserta en `dbo.AsistenciaMarcaje` (hora local) y deduplica por `UQ_AsistenciaMarcaje_Dedupe`
* En user sync: lee pendientes en `dbo.MB160UserSyncQueue` y llama `set_user()` en el MB160 con `UsuarioDispositivo` y `UsuarioNombre`

---

## 5.1) Ejecucion programada diaria (8pm)

Si no quieres el collector corriendo todo el tiempo, usa el runner de una sola ejecucion:

```bash
python scripts/run_daily_pull.py
```

Linux/macOS (cron):

```bash
crontab -e
# Agrega (reemplaza RUTA_REPO):
0 20 * * * cd /RUTA_REPO && /RUTA_REPO/.venv/bin/python scripts/run_daily_pull.py >> logs/daily_pull.log 2>&1
```

Windows (Task Scheduler):

* Program/script: `C:\RUTA_REPO\.venv\Scripts\python.exe`
* Add arguments: `scripts\run_daily_pull.py`
* Start in: `C:\RUTA_REPO`
* Trigger: diario 20:00

Servicio residente con bajo consumo (Windows):

```bash
python scripts/run_scheduled_pull.py
```

Este scheduler duerme hasta la hora configurada y solo abre conexiones cuando toca el pull.
Si quieres cambiar la hora, usa en `.env`:

```env
DAILY_PULL_HOUR=20
DAILY_PULL_MINUTE=0
```

---

## 6) Live capture opcional

Para escuchar eventos en vivo mientras pruebas el dispositivo:

```bash
python scripts/run_live_ingest.py
```

---

## 7) Instalar como servicio en Windows (NSSM)

1) Carpeta: `C:\Servicios\mb160\mb160-api\` (clona repo y crea `.env`).

2) venv + deps (PowerShell):

```powershell
cd C:\Servicios\mb160\mb160-api
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3) Crea `run_service.bat` (elige una opcion):

```bat
@echo off
cd /d C:\Servicios\mb160\mb160-api
call .\.venv\Scripts\activate.bat
REM Opcion A: servicio 24/7 (collector + user sync)
REM python scripts\run_collector.py

REM Opcion B: servicio diario con bajo consumo (solo pull)
python scripts\run_scheduled_pull.py
```

4) Instala NSSM y registra servicio (PowerShell admin):

```powershell
C:\Tools\nssm\nssm.exe install MB160Service "C:\Servicios\mb160\mb160-api\run_service.bat"
C:\Tools\nssm\nssm.exe set MB160Service AppDirectory "C:\Servicios\mb160\mb160-api"
C:\Tools\nssm\nssm.exe set MB160Service Start SERVICE_AUTO_START
C:\Tools\nssm\nssm.exe start MB160Service
```

5) Verifica estado: `Get-Service MB160Service`

6) Logs: `C:\Servicios\mb160\mb160-api\logs\service.log`

> El servicio necesita VPN activa si SQL Server está en red remota.

---

## 8) Operación recomendada

1. Crear tabla de marcajes (+ columna `UsuarioNombre`).
2. Crear cola `MB160UserSyncQueue` y trigger en `dbo.Personal`.
3. Configurar `.env` (SQL + MB160 + VPN).
4. `python scripts/run_health_check.py`
5. `python tests/test_db_insert.py`
6. `python scripts/run_api.py`
7. `python scripts/run_collector.py` (24/7) o `python scripts/run_scheduled_pull.py` (diario 20:00)
8. En VM Windows, instalar NSSM y dejar `MB160Service` como servicio.

---

## 9) Troubleshooting rápido

* **No conecta a SQL Server:** verifica VPN, `SQLSERVER_HOST/PORT`, firewall, SQL Auth.
* **Problemas de certificado:** para entorno interno usa `SQLSERVER_ENCRYPT=yes` y `SQLSERVER_TRUST_CERT=yes`.
* **El dedupe no falla en el duplicado:** confirma que exista `UQ_AsistenciaMarcaje_Dedupe` y que el test inserte los mismos campos del UNIQUE.
* **User sync no crea usuarios:** revisa que el trigger exista y que `dbo.MB160UserSyncQueue` tenga filas; checa `UltimoError`.
* **Sin logs de asistencia:** confirma que el MB160 tenga registros (`get_attendance()` > 0) o haz un marcaje manual.

---

## 10) Valores de estatus (AttState típicos)

* 0 = Check-In
* 1 = Check-Out
* 2 = Break-Out
* 3 = Break-In
* 4 = OT-In
* 5 = OT-Out
