"""
Trabajo Práctico 2 - Procesamiento de Datos

INSTRUCCIONES PARA EJECUTAR ESTE TP
1. Antes de ejecutar este TP2, debe haberse ejecutado el TP1,
   que genera los datos en BRONZE (digital_currency_daily y
   currency_exchange_rate). ejecutar python FranciscoPremuz_TP1.py

2. Si se usa almacenamiento local:
      Se debe tener creada la carpeta:
      data/bronze/alphavantage/
      con las tablas Delta generadas por el TP1.

3. Si se usa MinIO:
      · MinIO debe estar levantado.
      · El archivo config/storage.conf debe contener las claves.
      · Las rutas S3 se crearán automáticamente.

4. Ejecutar:
      python FranciscoPremuz_TP2.py

Este TP:
· Lee datos desde BRONZE
· Los transforma y limpia → SILVER
· Calcula agregados mensuales → GOLD
"""

import os
import pandas as pd
from deltalake import write_deltalake, DeltaTable
import configparser

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ===========================================================
# CARGA CONFIGURACIONES DE STORAGE (LOCAL o MINIO)
# ===========================================================
config = configparser.ConfigParser()
config.read(os.path.join(BASE_DIR, "config", "storage.conf"))

storage_options = None
bucket_name = None

if "minio" in config:
    print("Usando almacenamiento: MINIO\n")
    minio = config["minio"]
    endpoint = minio["AWS_ENDPOINT_URL"]
    key      = minio["AWS_ACCESS_KEY_ID"]
    secret   = minio["AWS_SECRET_ACCESS_KEY"]

    storage_options = {
        "AWS_ACCESS_KEY_ID": key,
        "AWS_SECRET_ACCESS_KEY": secret,
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ALLOW_HTTP": "true"
    }
    bucket_name = minio["bucket_name"]
else:
    print("Usando almacenamiento: LOCAL\n")

# ===========================================================
# DEFINICIÓN DE RUTAS LOCAL + S3
# ===========================================================
BRONZE_LOCAL = os.path.join(BASE_DIR, "data", "bronze", "alphavantage")
SILVER_LOCAL = os.path.join(BASE_DIR, "data", "silver", "alphavantage")
GOLD_LOCAL   = os.path.join(BASE_DIR, "data", "gold", "alphavantage")

# Si no hay MinIO, crear carpetas localmente
if bucket_name is None:
    os.makedirs(SILVER_LOCAL, exist_ok=True)
    os.makedirs(GOLD_LOCAL, exist_ok=True)

# Rutas BRONZE locales
CRYPTO_BRONZE_PATH_LOCAL = os.path.join(BRONZE_LOCAL, "digital_currency_daily")
FX_BRONZE_PATH_LOCAL     = os.path.join(BRONZE_LOCAL, "currency_exchange_rate")

# Rutas SILVER y GOLD locales
CRYPTO_SILVER_PATH_LOCAL = os.path.join(SILVER_LOCAL, "crypto_daily_clean")
FX_SILVER_PATH_LOCAL     = os.path.join(SILVER_LOCAL, "exchange_rate_clean")

CRYPTO_GOLD_PATH_LOCAL = os.path.join(GOLD_LOCAL, "crypto_monthly_summary")
FX_GOLD_PATH_LOCAL     = os.path.join(GOLD_LOCAL, "exchange_rate_latest")

# Rutas S3 si hay MinIO
CRYPTO_BRONZE_PATH_S3 = f"s3://{bucket_name}/bronze/alphavantage/digital_currency_daily" if bucket_name else None
FX_BRONZE_PATH_S3     = f"s3://{bucket_name}/bronze/alphavantage/currency_exchange_rate" if bucket_name else None

CRYPTO_SILVER_PATH_S3 = f"s3://{bucket_name}/silver/alphavantage/crypto_daily_clean" if bucket_name else None
FX_SILVER_PATH_S3     = f"s3://{bucket_name}/silver/alphavantage/exchange_rate_clean" if bucket_name else None

CRYPTO_GOLD_PATH_S3 = f"s3://{bucket_name}/gold/alphavantage/crypto_monthly_summary" if bucket_name else None
FX_GOLD_PATH_S3     = f"s3://{bucket_name}/gold/alphavantage/exchange_rate_latest" if bucket_name else None


# ===========================================================
# FUNCIONES AUXILIARES
# ===========================================================
def safe_read_delta(local_path, s3_path=None, storage_options=None):
    """
    Intenta leer Delta desde MinIO (si está configurado).
    Si falla, intenta leer desde el almacenamiento local.
    """
    # 1. Intento MinIO
    if s3_path:
        try:
            print(f"Intentando leer desde MinIO: {s3_path}")
            dt = DeltaTable(s3_path, storage_options=storage_options)
            df = dt.to_pandas()
            print(f"Leído desde MinIO.")
            return df
        except Exception as e:
            print(f"Falló lectura MinIO: {e}")

    # 2. Intento local
    try:
        if os.path.exists(local_path):
            print(f"Intentando leer localmente: {local_path}")
            dt = DeltaTable(local_path)
            df = dt.to_pandas()
            print(f"Leído localmente.")
            return df
    except Exception as e:
        print(f"Falló lectura local: {e}")

    return None


def write_delta(df, local_path=None, s3_path=None, mode="overwrite",
                partition_by=None, storage_options=None):
    """
    Escribe una tabla Delta en MinIO si está configurado.
    Si falla, escribe localmente.
    """

    df = df.reset_index(drop=True)

    # Intento escribir en MinIO
    if s3_path and storage_options:
        try:
            print(f"Intentando escribir en MinIO: {s3_path}")
            write_deltalake(
                s3_path,
                df,
                mode=mode,
                partition_by=partition_by,
                storage_options=storage_options
            )
            print(f"Delta escrito en MinIO.")
            return
        except Exception as e:
            print(f"No se pudo escribir en MinIO: {e}")

    # Fallback → escritura local
    if local_path:
        os.makedirs(local_path, exist_ok=True)
        try:
            print(f"Escribiendo localmente: {local_path}")
            write_deltalake(local_path, df, mode=mode, partition_by=partition_by)
            print(f"Delta escrito localmente.")
        except Exception as e:
            print(f"Falló escritura local: {e}")


# ===========================================================
# LECTURA DESDE BRONZE
# ===========================================================
print("Inicio del procesamiento (Bronze → Silver → Gold)\n")

df_crypto = safe_read_delta(CRYPTO_BRONZE_PATH_LOCAL, CRYPTO_BRONZE_PATH_S3, storage_options)
df_fx     = safe_read_delta(FX_BRONZE_PATH_LOCAL, FX_BRONZE_PATH_S3, storage_options)

if df_crypto is None:
    print("No se encontró BRONZE. Ejecutar TP1 primero.")
    exit(0)

print("\n--- Estado inicial BRONZE (crypto) ---")
print(f"Filas: {len(df_crypto)}")
print(df_crypto.head())


# ===========================================================
# TRANSFORMACIONES SILVER – CRYPTO
# ===========================================================
print("\nConstruyendo SILVER para crypto...")

df = df_crypto.copy()

# procesamiento incremental
df_silver_prev = safe_read_delta(CRYPTO_SILVER_PATH_LOCAL, CRYPTO_SILVER_PATH_S3, storage_options)

if df_silver_prev is not None:
    max_dt = df_silver_prev["datetime"].max()
    print(f"Procesando incrementalmente desde {max_dt}...")
    df = df[df["datetime"] > max_dt]
else:
    print("Carga inicial: Se procesará toda la historia de Bronze.")

# Normaliza nombres de columnas
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

# columna datetime a tipo fecha
df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

# Convierte columnas numéricas a float
num_cols = [c for c in df.columns if any(k in c for k in ["open","high","low","close","volume"])]
for c in num_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# elimina duplicados y ordena por fecha
df = df.drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime")

# elimina filas sin fecha valida
df = df.dropna(subset=["datetime"])

# relleno valores faltantes
df[num_cols] = df[num_cols].ffill().bfill()

# Elimina precios inválidos (close <= 0)")
df = df[df["close"] > 0]

# crea columnas date y month
df["date"] = df["datetime"].dt.date.astype(str)
df["month"] = df["datetime"].dt.to_period("M").astype(str)

# Control de duplicados antes de escribir (incremental append seguro)
df = df.drop_duplicates(subset=["datetime"], keep="last")

# ===========================================================
# ENRIQUECIMIENTO FX 
# ===========================================================
print("\nProcesando tasa FX para enriquecer precios...")

fx_value = None
if df_fx is not None:
    fx = df_fx.copy()
    fx.columns = [c.strip().lower().replace(" ", "_") for c in fx.columns]

    # Busca columna numérica que represente el tipo de cambio
    candidates = [c for c in fx.columns if "rate" in c or "exchange" in c]
    numeric_candidates = []

    for c in candidates:
        numeric_candidates.append(pd.to_numeric(fx[c], errors="coerce"))

    if numeric_candidates:
        fx_value = numeric_candidates[0].dropna().iloc[-1]
        df["close_fx"] = df["close"] * fx_value
        print(f"Tasa FX aplicada. Valor: {fx_value}")
    else:
        print("No se encontró columna FX válida.")


# ===========================================================
# GUARDAR SILVER – crypto
# ===========================================================
write_delta(df, CRYPTO_SILVER_PATH_LOCAL, CRYPTO_SILVER_PATH_S3,
            partition_by=["date"], mode="append", storage_options=storage_options)

print("\n--- SILVER (crypto) ---")
print(df.head())
print(f"Filas Silver Crypto: {len(df)}")


# ===========================================================
# SILVER – FX LIMPIO
# ===========================================================
df_fx_s = None
if df_fx is not None:
    print("\nConstruyendo SILVER para FX...")

    df_fx_s = df_fx.copy()
    df_fx_s.columns = [c.strip().lower().replace(" ", "_") for c in df_fx_s.columns]

    # elimina duplicados por fecha FX
    if "last_refreshed" in df_fx_s.columns:
        df_fx_s = df_fx_s.drop_duplicates(subset=["last_refreshed"], keep="last")
    else:
        df_fx_s = df_fx_s.drop_duplicates()

    write_delta(df_fx_s, FX_SILVER_PATH_LOCAL, FX_SILVER_PATH_S3, mode="append", storage_options=storage_options)

    print("\n--- SILVER (fx) ---")
    print(df_fx_s.head())


# ===========================================================
# GOLD – Resumen mensual crypto
# ===========================================================
print("\nConstruyendo GOLD mensual para crypto...")

df_silver_crypto = safe_read_delta(CRYPTO_SILVER_PATH_LOCAL, CRYPTO_SILVER_PATH_S3, storage_options)

df_gold_crypto = df_silver_crypto.groupby("month", as_index=False).agg(
    avg_close=("close", "mean"),
    max_close=("close", "max"),
    min_close=("close", "min")
)

# Evita duplicados por mes en append
df_gold_crypto = df_gold_crypto.drop_duplicates(subset=["month"], keep="last")

write_delta(df_gold_crypto, CRYPTO_GOLD_PATH_LOCAL, CRYPTO_GOLD_PATH_S3, mode="append", storage_options=storage_options)

print("\n--- GOLD (crypto mensual) ---")
print(df_gold_crypto)


# ===========================================================
# GOLD – Último valor FX
# ===========================================================
print("\nConstruyendo GOLD para FX (último valor)...")

if df_fx_s is not None:
    df_fx_gold = df_fx_s.tail(1)

    # Asegura que no se duplique la última tasa en el append
    df_fx_gold = df_fx_gold.drop_duplicates()

    write_delta(df_fx_gold, FX_GOLD_PATH_LOCAL, FX_GOLD_PATH_S3, mode="append", storage_options=storage_options)

    print("\n--- GOLD (fx último valor) ---")
    print(df_fx_gold)


print("\n=== Procesamiento completado ===")