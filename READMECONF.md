# Configuracion del servicio diario en Windows (RDP)

Esta guia configura `scripts/run_scheduled_pull.py` como servicio de Windows en un servidor remoto.
El servicio duerme hasta la hora programada y solo consume recursos durante el pull.

## Requisitos

- Windows Server con acceso RDP.
- Python 3.11+ instalado.
- ODBC Driver 18 for SQL Server instalado.
- Acceso al repo en `C:\Servicios\mb160\mb160-api\`.
- Credenciales de SQL Server y IP del MB160.

## Pasos

1) Conecta por RDP y abre PowerShell.

2) Clona o copia el repo:

```powershell
cd C:\Servicios
git clone <URL_DEL_REPO> mb160-api
```

3) Crea el venv e instala dependencias:

```powershell
cd C:\Servicios\mb160\mb160-api
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

4) Crea `.env` (desde `.env.example`) y define variables:

```env
SQLSERVER_HOST=...
SQLSERVER_PORT=1433
SQLSERVER_DB=...
SQLSERVER_USER=...
SQLSERVER_PASSWORD=...

MB160_IP=...
MB160_PORT=4370

DAILY_PULL_HOUR=20
DAILY_PULL_MINUTE=0
```

5) Prueba manualmente una ejecucion:

```powershell
.\.venv\Scripts\python.exe scripts\run_scheduled_pull.py
```

Espera a que imprima "Scheduler iniciado" y cancela con Ctrl+C.

6) Crea un `run_service.bat`:

```bat
@echo off
cd /d C:\Servicios\mb160\mb160-api
call .\.venv\Scripts\activate.bat
python scripts\run_scheduled_pull.py
```

7) Instala NSSM (si no existe) y registra el servicio:

```powershell
C:\Tools\nssm\nssm.exe install MB160DailyPull "C:\Servicios\mb160\mb160-api\run_service.bat"
C:\Tools\nssm\nssm.exe set MB160DailyPull AppDirectory "C:\Servicios\mb160\mb160-api"
C:\Tools\nssm\nssm.exe set MB160DailyPull Start SERVICE_AUTO_START
C:\Tools\nssm\nssm.exe start MB160DailyPull
```

8) Verifica estado:

```powershell
Get-Service MB160DailyPull
```

## Logs

- `C:\Servicios\mb160\mb160-api\logs\service.log`

## Notas

- Si SQL Server esta en red remota, asegura la VPN activa para el usuario/servicio.
- Para cambiar la hora del pull, edita `DAILY_PULL_HOUR` y `DAILY_PULL_MINUTE` en `.env`.

## Variante multi checador (22 IPs cada 5 minutos)

Si tu escenario es varios dispositivos, usa `scripts/run_collector_multiple_apis.py`.

Variables `.env` recomendadas:

```env
MB160_IPS=192.168.1.50,192.168.1.51,192.168.1.52
MB160_PORT=4370
MULTI_PULL_INTERVAL_SECONDS=300
MULTI_PULL_MAX_WORKERS=6
```

`run_service.bat` para esta variante:

```bat
@echo off
cd /d C:\Servicios\mb160\mb160-api
call .\.venv\Scripts\activate.bat
python scripts\run_collector_multiple_apis.py
```
