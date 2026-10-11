"""
Capa Silver: aplica reglas de calidad a la telemetría cruda (Bronze)
y produce un dataset limpio listo para la capa Gold.

Reglas aplicadas (en orden):
    1. Datos faltantes en columnas críticas.
    2. Rango físico válido (potencia, irradiancia, temperatura).
    3. Duplicados por (ts, dispositivo_id).

Salida: data/silver/lectura_5min.csv
"""

from pathlib import Path
import pandas as pd
from dataclasses import dataclass

ARCHIVO_BRONZE = Path("data/bronze/telemetria.csv")
ARCHIVO_SILVER = Path("data/silver/lectura_5min.csv")

COLUMNAS_CRITICAS = ["p_ac_kw", "irradiancia_wm2", "temp_modulo_c"]

@dataclass
class ReporteSilver:
    # Esquema del reporte Silver
    total_leidas: int = 0
    rechazados_faltantes: int = 0
    rechazados_rango: int = 0
    rechazados_duplicados: int = 0
    validas: int = 0
    porcentaje_validos: float | int = 0

REPORTE = ReporteSilver()

def crear_carpeta_silver() -> None:
    ARCHIVO_SILVER.parent.mkdir(parents=True, exist_ok=True)
    print(f"Documento de salida: {ARCHIVO_SILVER}")

def formatear_columnas(df: pd.DataFrame) -> pd.DataFrame:
    # Selecciona la columna llamada "ts" (time series) del DataFrame
    # Errors evita que la función arroje una excepción con datos no convertibles a fecha
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

    for columna in COLUMNAS_CRITICAS:
        df[columna] = pd.to_numeric(df[columna], errors="coerce")

    return df

def eliminar_faltantes(df: pd.DataFrame):
    """
    Regla #1: Eliminar registros faltantes del archivo Bronze

    :param df: DataFrame a procesar
    :return: Retorna un DataFrame sin nulos
    """
    archivo_original = df[COLUMNAS_CRITICAS + ["ts"]]
    mascara_faltantes = archivo_original.isna().any(axis=1)

    # Cuenta cuantos registros se eliminan por datos incompletos
    REPORTE.rechazados_faltantes = mascara_faltantes.sum()

    # Retornamos solamente los campos del DataFrame sin faltantes
    return df.loc[~mascara_faltantes].copy()

def restringir_datos_fuera_de_rango(df: pd.DataFrame) -> pd.DataFrame:
    rango_potencia_ac_kw     = (0.0, 6.0)       # Rango de potencia del inversor (kW)
    rango_irradiancia        = (0.0, 1500.0)    # Cuanta luz solar cubre la superficie medida (W/m2)
    rango_temperatura_modulo = (-20.0, 85.0)    # Temperatura superficial del panel

    mascara_rango = (
        df["p_ac_kw"].between(*rango_potencia_ac_kw, inclusive= "both")  # Identifica que la potencia esté en el rango soportado por el inversor
        & df["irradiancia_wm2"].between(*rango_irradiancia, inclusive= "both")
        & df["temp_modulo_c"].between(*rango_temperatura_modulo, inclusive= "both")
    )

    REPORTE.rechazados_rango = int((~mascara_rango).sum())
    return df.loc[mascara_rango].copy()

def eliminar_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Regla 3: Eliminar registros duplicados
    :param df: DataFrame a procesar
    :return: Retorna un DataFrame sin duplicados
    """

    # Se define un duplicado como aquellos registros en los que el timestamp y id del dispositivo es igual
    # Es decir, un dispositivo no puede enviar dos medidas al mismo tiempo
    mascara_duplicados = df.duplicated(subset=["ts", "dispositivo_id"], keep="first")
    REPORTE.rechazados_duplicados = int(mascara_duplicados.sum())
    # Crea una copia del dataframe original, accediendo solo a las columnas no duplicadas
    # la máscara retorna una Serie de pandas, la cual loc usa como filtro para buscar las filas duplicadas
    return df.loc[~mascara_duplicados].copy()

def imprimir_reporte() -> None:
    print("=" * 48)
    print("REPORTE DE CALIDAD - CAPA SILVER")
    print("=" * 48)
    print(f"Filas leídas:              {REPORTE.total_leidas:>6}")
    print(f"Rechazadas (faltantes):    {REPORTE.rechazados_faltantes:>6}")
    print(f"Rechazadas (rango):        {REPORTE.rechazados_rango:>6}")
    print(f"Rechazadas (duplicados):   {REPORTE.rechazados_duplicados:>6}")
    print(f"Filas validas:             {REPORTE.validas:>6}")
    print(f"% datos validos:           {REPORTE.porcentaje_validos:>6.2f}%")
    print("=" * 48)

def main() -> None:

    crear_carpeta_silver()

    df = pd.read_csv(ARCHIVO_BRONZE)
    REPORTE.total_leidas = len(df)

    df = formatear_columnas(df)

    # Aplicación regla 1
    df = eliminar_faltantes(df)

    # Aplicación regla 2
    df = restringir_datos_fuera_de_rango(df)

    # Aplicación regla 3
    df = eliminar_duplicados(df)

    df = df.sort_values(["dispositivo_id", "ts"]).reset_index(drop=True)
    df.to_csv(ARCHIVO_SILVER, index=False)

    REPORTE.validas = len(df)
    REPORTE.porcentaje_validos = (REPORTE.validas / REPORTE.total_leidas * 100) if REPORTE.total_leidas else 0.0
    imprimir_reporte()


if __name__ == "__main__":
    main()
