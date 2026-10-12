"""
Capa Gold: carga la silver.lectura_5min a PostgreSQL y construye
el fact dwh.fact_energia_dia con energia diaria por dispositivo.

Ambas cargas usan UPSERT (INSERT ... ON CONFLICT DO UPDATE) para que
la ejecucion sea idempotente: correr el script dos veces produce las
mismas filas y los mismos valores.
"""

from pathlib import Path

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

SILVER_CSV = Path("data/silver/lectura_5min.csv")
SCHEMA_SQL = Path("sql/01_schema.sql")

COLUMNAS_CRITICAS = ["p_ac_kw", "irradiancia_wm2", "temp_modulo_c"]
LECTURAS_POR_DIA = 24 * 12  # 288 lecturas de 5 minutos


def construir_motor_base_datos():
    """Crea la conexión con la base de datos a partir de nuestro
    connection String, que une los datos de nuestras variables de entorno"""
    load_dotenv()
    url = (
        f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
        f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}"
    )
    return create_engine(url, future=True)


def aplicar_schema(engine) -> None:
    """Crea schemas y tablas si no existen."""
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))


def cargar_silver(engine, df: pd.DataFrame) -> int:
    """UPSERT a silver.lectura_5min. Devuelve filas procesadas."""
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"])
    registros = df.to_dict(orient="records")

    sql = text(
        """
        INSERT INTO silver.lectura_5min
            (ts, dispositivo_id, p_ac_kw, irradiancia_wm2, temp_modulo_c)
        VALUES
            (:ts, :dispositivo_id, :p_ac_kw, :irradiancia_wm2, :temp_modulo_c)
        ON CONFLICT (ts, dispositivo_id) DO UPDATE SET
            p_ac_kw         = EXCLUDED.p_ac_kw,
            irradiancia_wm2 = EXCLUDED.irradiancia_wm2,
            temp_modulo_c   = EXCLUDED.temp_modulo_c
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, registros)
    return len(registros)


def construir_fact_energia_dia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega la silver a nivel dia-dispositivo.

    - energia_kwh: suma de potencia * (5/60) h.
    - pct_datos_validos: cuantas lecturas tiene el dia frente a las
      288 esperadas (24h * 12 lecturas/h).
    """
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"])
    df["fecha_key"] = df["ts"].dt.date

    agregado = (
        df.groupby(["fecha_key", "dispositivo_id"], as_index=False)
        .agg(
            energia_kwh=("p_ac_kw", lambda s: round(s.sum() * (5 / 60), 3)),
            lecturas=("p_ac_kw", "count"),
        )
        .rename(columns={"dispositivo_id": "dispositivo_key"})
    )
    agregado["pct_datos_validos"] = (
        agregado["lecturas"] / LECTURAS_POR_DIA * 100
    ).round(2)
    return agregado[
        ["fecha_key", "dispositivo_key", "energia_kwh", "pct_datos_validos"]
    ]


def cargar_fact(engine, fact: pd.DataFrame) -> int:
    """UPSERT a dwh.fact_energia_dia. Devuelve filas procesadas."""
    registros = fact.to_dict(orient="records")
    sql = text(
        """
        INSERT INTO dwh.fact_energia_dia
            (fecha_key, dispositivo_key, energia_kwh, pct_datos_validos)
        VALUES
            (:fecha_key, :dispositivo_key, :energia_kwh, :pct_datos_validos)
        ON CONFLICT (fecha_key, dispositivo_key) DO UPDATE SET
            energia_kwh       = EXCLUDED.energia_kwh,
            pct_datos_validos = EXCLUDED.pct_datos_validos
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, registros)
    return len(registros)


def contar(engine, tabla: str) -> int:
    with engine.begin() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar_one()


def main() -> None:
    engine = construir_motor_base_datos()
    aplicar_schema(engine)

    df_silver = pd.read_csv(SILVER_CSV)
    n_silver = cargar_silver(engine, df_silver)

    fact = construir_fact_energia_dia(df_silver)
    n_fact = cargar_fact(engine, fact)

    total_silver = contar(engine, "silver.lectura_5min")
    total_fact = contar(engine, "dwh.fact_energia_dia")

    print("=" * 48)
    print("CARGA GOLD (idempotente)")
    print("=" * 48)
    print(f"UPSERT silver.lectura_5min:   {n_silver:>5} filas procesadas")
    print(f"UPSERT dwh.fact_energia_dia:  {n_fact:>5} filas procesadas")
    print("-" * 48)
    print(f"Total silver.lectura_5min:    {total_silver:>5}")
    print(f"Total dwh.fact_energia_dia:   {total_fact:>5}")
    print("=" * 48)


if __name__ == "__main__":
    main()
