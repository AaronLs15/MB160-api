```md
# MB160 Service (SQL Server + Collector + API)

Este repositorio contiene:
- Un **test** de conectividad e integridad a SQL Server (dedupe) sin necesidad del MB160.
- Un **collector** (cuando tengas el MB160) que descarga marcajes y los inserta en SQL Server.
- Una **API (FastAPI)** para consultar marcajes.

---

## 0) Estructura del repo

```

mb160-service/
api.py
collector.py
db.py
health_check.py
service_main.py
test_db_insert.py
requirements.txt
.env.example
sql/
create_AsistenciaMarcaje.sql

````

---

## 1) Crear tabla en SQL Server

Ejecuta el script:

`sql/create_AsistenciaMarcaje.sql`

Asegúrate de que el constraint UNIQUE exista (dedupe):
`UQ_AsistenciaMarcaje_Dedupe`

---

## 2) Configurar variables de entorno

1. Copia `.env.example` a `.env`
2. Llena tus credenciales (**NO comitear** `.env`)

**Ejemplo mínimo**
- `SQLSERVER_HOST`
- `SQLSERVER_PORT`
- `SQLSERVER_DB=db_name`
- `SQLSERVER_USER`
- `SQLSERVER_PASSWORD`

**Ejemplo completo** (`.env`)
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

# ---- MB160 (cuando ya lo tengas) ----
MB160_IP=192.168.1.50
MB160_PORT=4370
PULL_INTERVAL_SECONDS=60

# ---- API ----
API_PORT=8000
````

> **VPN:** si tu SQL Server está en una red remota, asegúrate de que la VPN esté conectada antes de correr tests/servicio/API.

---

## 3) Probar en macOS (sin MB160)

### Requisitos

* VPN conectada hacia la red donde vive SQL Server
* Python 3.11+
* ODBC Driver 18 para SQL Server (`msodbcsql18`)

### Instalar ODBC Driver (macOS con brew)

```bash
brew update
brew install unixodbc
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install msodbcsql18
```

### Crear venv e instalar dependencias

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Smoke test: conexión a DB

```bash
python health_check.py
```

### Test DB + dedupe (Opción 2)

```bash
python test_db_insert.py
```

Salida esperada:

* `OK: conexión a SQL Server funciona`
* `OK: insert 1 realizado`
* `OK: deduplicación funciona (IntegrityError por UNIQUE)`

---

## 4) Probar en Windows (sin MB160)

### Requisitos

* VPN conectada
* Python 3.11+ (marcar “Add Python to PATH”)
* Microsoft **ODBC Driver 18 for SQL Server** instalado

### Crear venv e instalar dependencias

PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Smoke test: conexión a DB

```powershell
python health_check.py
```

### Test DB + dedupe

```powershell
python test_db_insert.py
```

---

## 5) Correr la API (FastAPI)

### Instalar dependencias

(ya incluidas en `requirements.txt`)

### Ejecutar API en modo dev

macOS o Windows:

```bash
python api.py
```

Por defecto:

* API: `http://localhost:8000`
* Swagger UI: `http://localhost:8000/docs`

> Si quieres cambiar el puerto, ajusta `API_PORT` en `.env`.

### Endpoints disponibles

#### `GET /health`

Verifica conectividad a SQL Server.
**Respuesta**:

```json
{ "status": "ok", "db": "db_name" }
```

#### `GET /marks`

Lista marcajes (paginado) con filtros opcionales.

**Query params:**

* `user_id` → `UsuarioDispositivo` (enroll / user_id del reloj)
* `device_serial` → `DispositivoSerial`
* `dt_from` → desde (datetime ISO, hora local)
* `dt_to` → hasta (datetime ISO, hora local)
* `limit` → 1..2000 (default 200)
* `offset` → >=0 (default 0)

**Ejemplos**

* Últimos 50 marcajes:

  * `/marks?limit=50`
* Marcajes del usuario 1001:

  * `/marks?user_id=1001&limit=100`
* Rango de fechas:

  * `/marks?dt_from=2025-12-01T00:00:00&dt_to=2025-12-02T23:59:59&limit=200`

#### `GET /marks/{mark_id}`

Obtiene un marcaje por `AsistenciaMarcajeID`.

---

## 6) Correr el collector (ya con MB160)

### 6.1 Configurar `.env`

Asegúrate de tener:

* `MB160_IP`
* `MB160_PORT` (default 4370)
* `PULL_INTERVAL_SECONDS` (default 60)

Ejemplo:

```env
MB160_IP=192.168.1.50
MB160_PORT=4370
PULL_INTERVAL_SECONDS=60
```

### 6.2 Ejecutar collector en consola (dev)

```bash
python service_main.py
```

Logs:

* `logs/collector.log`

### 6.3 ¿Qué hace el collector?

* Se conecta al MB160 por TCP/IP
* Descarga los marcajes (`get_attendance()`)
* Inserta en `dbo.AsistenciaMarcaje` usando `EventoFechaHora` en **hora local**
* Deduplica por el UNIQUE `UQ_AsistenciaMarcaje_Dedupe`
* Corre en loop cada `PULL_INTERVAL_SECONDS`

---

## 7) Instalar el collector como Servicio en Windows (NSSM)

> Recomendado para correr 24/7 en una VM.

### 7.1 Preparar carpeta

Ejemplo:
`C:\Servicios\mb160\mb160-service\`

Clona el repo allí y crea tu `.env`.

### 7.2 Crear venv e instalar deps

PowerShell:

```powershell
cd C:\Servicios\mb160\mb160-service
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 7.3 Crear `run_service.bat`

Crea `C:\Servicios\mb160\mb160-service\run_service.bat`:

```bat
@echo off
cd /d C:\Servicios\mb160\mb160-service
call .\.venv\Scripts\activate.bat
python service_main.py
```

### 7.4 Instalar NSSM

Descarga NSSM y descomprime en:
`C:\Tools\nssm\`

### 7.5 Crear el servicio (PowerShell como Admin)

```powershell
C:\Tools\nssm\nssm.exe install MB160Collector "C:\Servicios\mb160\mb160-service\run_service.bat"
C:\Tools\nssm\nssm.exe set MB160Collector AppDirectory "C:\Servicios\mb160\mb160-service"
C:\Tools\nssm\nssm.exe set MB160Collector Start SERVICE_AUTO_START
C:\Tools\nssm\nssm.exe start MB160Collector
```

### 7.6 Verificar estado

```powershell
Get-Service MB160Collector
```

### 7.7 Ver logs

* `C:\Servicios\mb160\mb160-service\logs\collector.log`

### 7.8 Detener / arrancar

```powershell
C:\Tools\nssm\nssm.exe stop MB160Collector
C:\Tools\nssm\nssm.exe start MB160Collector
```

### Nota VPN (importante)

* El servicio requiere que la VPN esté activa para llegar a SQL Server.
* Si la VPN se cae, el collector puede fallar temporalmente; al volver la VPN, se recupera.
* Ideal: configurar VPN “Always On”/auto-connect en la VM.

---

## 8) Operación recomendada (uso completo)

1. Ejecuta el SQL para crear tabla en `base de datos`
2. Configura `.env` (SQL + VPN)
3. Corre `health_check.py`
4. Corre `test_db_insert.py` (dedupe)
5. Corre `api.py` (consulta marcajes)
6. (Con MB160) corre `service_main.py` para empezar a poblar `dbo.AsistenciaMarcaje`
7. En VM Windows, instala NSSM y deja `MB160Collector` como servicio

---

## 9) Troubleshooting rápido

* **No conecta a SQL Server**

  * Verifica VPN
  * Verifica `SQLSERVER_HOST` y `SQLSERVER_PORT`
  * Verifica firewall y que SQL acepte SQL Auth

* **Problemas de certificado**

  * Para entorno interno: `SQLSERVER_ENCRYPT=yes` y `SQLSERVER_TRUST_CERT=yes`
  * Para cert válido: `SQLSERVER_TRUST_CERT=no`

* **El test de dedupe no falla en el duplicado**

  * Asegúrate de que exista `UQ_AsistenciaMarcaje_Dedupe`
  * Asegúrate de que el test inserte exactamente los mismos campos del UNIQUE

* **No hay logs**

  * Revisa que exista carpeta `logs/`
  * Corre `python service_main.py` en consola para ver errores en pantalla

---
