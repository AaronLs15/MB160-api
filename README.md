# MB160 Service (SQL Server + Collector + API)

Este repositorio contiene:
- Un **test** de conectividad e integridad a SQL Server (dedupe) sin necesidad del MB160.
- Un **collector** (cuando tengas el MB160) que descarga marcajes y los inserta en SQL Server.
- Una **API (FastAPI)** para consultar marcajes.

> DB objetivo: `Bigbang 6`  
> Tabla: `dbo.AsistenciMarcaje`  
> Guardado: **hora local** (`EventoFechaHora`)

---

## 1) Crear tabla en SQL Server

Ejecuta el script:

`sql/create_AsistenciMarcaje.sql`

Asegúrate de que el constraint UNIQUE exista:
`UQ_AsistenciMarcaje_Dedupe`

---

## 2) Configurar variables de entorno

1. Copia `.env.example` a `.env`
2. Llena tus credenciales (NO comitear `.env`)

Ejemplo mínimo:
- SQLSERVER_HOST
- SQLSERVER_PORT
- SQLSERVER_DB=Bigbang 6
- SQLSERVER_USER
- SQLSERVER_PASSWORD

---

## 3) Probar en macOS (sin MB160)

### Requisitos
- VPN conectada hacia la red donde vive SQL Server
- Python 3.11+
- ODBC Driver 18 para SQL Server (msodbcsql18)

#### Instalar ODBC Driver (macOS con brew)
```bash
brew update
brew install unixodbc
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install msodbcsql18
