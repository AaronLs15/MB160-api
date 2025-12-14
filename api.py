import os
from datetime import datetime
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import text

from db import build_engine, test_connection

load_dotenv()

app = FastAPI(
    title="MB160 Attendance Service",
    version="1.0.0",
    description="API para consultar marcajes insertados desde dispositivos ZKTeco (MB160) hacia SQL Server.",
)

engine = build_engine()


@app.get("/health")
def health() -> Dict[str, Any]:
    test_connection(engine)
    return {"status": "ok", "db": os.environ.get("SQLSERVER_DB")}


@app.get("/marks", response_model=list[dict])
def list_marks(
    user_id: Optional[str] = Query(None, description="UsuarioDispositivo (enroll/user_id del reloj)"),
    device_serial: Optional[str] = Query(None, description="DispositivoSerial"),
    dt_from: Optional[datetime] = Query(None, description="Fecha/hora local desde (inclusive)"),
    dt_to: Optional[datetime] = Query(None, description="Fecha/hora local hasta (inclusive)"),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    where = []
    params: Dict[str, Any] = {"limit": limit, "offset": offset}

    if user_id:
        where.append("UsuarioDispositivo = :user_id")
        params["user_id"] = user_id
    if device_serial:
        where.append("DispositivoSerial = :device_serial")
        params["device_serial"] = device_serial
    if dt_from:
        where.append("EventoFechaHora >= :dt_from")
        params["dt_from"] = dt_from
    if dt_to:
        where.append("EventoFechaHora <= :dt_to")
        params["dt_to"] = dt_to

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    q = text(f"""
        SELECT
            AsistenciMarcajeID,
            DispositivoSerial,
            DispositivoIP,
            UsuarioDispositivo,
            EventoFechaHora,
            Punch,
            Estado,
            WorkCode,
            FechaRegistro
        FROM dbo.AsistenciMarcaje
        {where_sql}
        ORDER BY EventoFechaHora DESC
        OFFSET :offset ROWS
        FETCH NEXT :limit ROWS ONLY
    """)

    with engine.connect() as conn:
        rows = conn.execute(q, params).mappings().all()

    return [dict(r) for r in rows]


@app.get("/marks/{mark_id}", response_model=dict)
def get_mark(mark_id: int) -> Dict[str, Any]:
    q = text("""
        SELECT
            AsistenciMarcajeID,
            DispositivoSerial,
            DispositivoIP,
            UsuarioDispositivo,
            EventoFechaHora,
            Punch,
            Estado,
            WorkCode,
            FechaRegistro
        FROM dbo.AsistenciMarcaje
        WHERE AsistenciMarcajeID = :id
    """)
    with engine.connect() as conn:
        row = conn.execute(q, {"id": mark_id}).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Mark not found")

    return dict(row)


if __name__ == "__main__":
    # Dev runner: python api.py
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=int(os.environ.get("API_PORT", "8000")), reload=True)
