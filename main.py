"""
Trabajo Práctico – Data Engineering
----------------------------------

Sistema origen: AlphaVantage API (https://www.alphavantage.co/documentation)

Endpoints utilizados:
  1. DIGITAL_CURRENCY_DAILY  
     - Extracción incremental (histórico diario)
     - Motivo: cada ejecución agrega solo un registro nuevo

  2. CURRENCY_EXCHANGE_RATE  
     - Extracción full overwrite
     - Motivo: representa una medición puntual (solo un valor a la vez)

Modelo de Data Lake:
  - BRONZE:
      Datos crudos tal como vienen de la API.
      Particionados por fecha cuando aplica.
      Estructura por sistema origen → endpoint.

  - SILVER:
      Datos limpios (validaciones, no nulos, constraints aplicados).

  - GOLD:
      Datos agregados para análisis (resúmenes, últimas mediciones).

Decisiones del diseño:
  - Separar las carpetas por sistema origen (alphavantage) y por cada entidad.
  - No incluir operaciones de mantenimiento (VACUUM/OPTIMIZE) dentro del ETL.
    → Esos procesos deben ejecutarse por separado.

"""

import os
import requests
import pandas as pd
import pyarrow as pa
import configparser
from datetime import datetime, timedelta
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from deltalake.table import TableOptimizer

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# -----------------------------------------------------------------------
# funciones auxiliares

# config
def load_config_file(filename):
    # Carga un archivo .conf desde la carpeta config/
    config_path = os.path.join(BASE_DIR, "config", filename)
    if not os.path.exists(config_path):
        print(f"No se encontró {config_path}.")
        return None
    parser = configparser.ConfigParser()
    parser.read(config_path)
    return parser

def s3_join(*parts):
    return "/".join([p.strip("/") for p in parts])

# extract
def get_data(base_url, params=None, headers=None):
    # http a la API y devuelve datos en formato JSON
    try:
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        try:
            data = response.json()
            return data
        except Exception as e:
            print("La respuesta no es JSON:", e)
            return None
    except requests.exceptions.RequestException as e:
        print(f"La petición ha fallado. Error: {e}")
        return None
    
# transform

def build_dynamic_table(data):
    # convierte las cotizaciones en un dataframe limpio
    if not data:
        return None
    
    series_key = next((k for k in data.keys() if k.startswith("Time Series")), None)

    if not series_key:
        print("No se encontró la clave de serie temporal en la respuesta.")
        print(list(data.keys()))
        return None
    
    df = pd.DataFrame.from_dict(data[series_key], orient="index")
    df.columns = [c.split(". ")[-1].split(" (")[0] for c in df.columns]

    df.index.name = "datetime"
    df.reset_index(inplace=True)
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d", errors="coerce")
    df.sort_values(by="datetime", inplace=True)

    for col in df.columns:
        if col != "datetime":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # particion por fecha
    df["date"] = df["datetime"].dt.date.astype(str)

    return df

def build_static_table(data):
    # convierte datos estaticos en dataframe
    if not data:
        return None
    
    # extrae el diccionario principal
    rate = data["Realtime Currency Exchange Rate"]
    if rate is None:
        print("No se encontró 'Realtime Currency Exchange Rate' en la respuesta.")
        return None

    # Convierte en DataFrame
    df = pd.DataFrame([rate])

    # Normaliza nombres de columnas automáticamente (sino el codigo falla porque aparecen nombres duplicados)
    new_columns = []
    seen = set()

    for c in df.columns:
        parts = c.split(" ")[1:]  # remueve el número y punto
        col_name = "_".join(parts).lower()  # une con _
        
        # Si el nombre ya existe, agrega un sufijo numérico
        base_name = col_name
        counter = 1
        while col_name in seen:
            col_name = f"{base_name}_{counter}"
            counter += 1

        seen.add(col_name)
        new_columns.append(col_name)

    df.columns = new_columns
    return df

def remove_duplicate_columns(df):
    # Elimina columnas duplicadas automáticamente
    seen = set()
    new_cols = []
    for c in df.columns:
        if c not in seen:
            new_cols.append(c)
            seen.add(c)
        else:
            new_cols.append(f"{c}_dup")
    df.columns = new_cols
    return df

# write (delta)
def save_data_as_delta(df, path, storage_options=None, mode="overwrite", partition_cols=None):  
    # guarda los datos en formato delta lake 
    write_deltalake(
        path,
        df,
        mode=mode,
        storage_options=storage_options,
        partition_by=partition_cols
    )

def upsert_data_as_delta(data, data_path, predicate, storage_options=None, partition_cols=None):
    # inserta nuevos datos o actualiza registros existentes
    if data is None:
        print("upsert: No hay datos para escribir.")
        return
    
    try:
        dt = DeltaTable(data_path, storage_options=storage_options)
        data_pa = pa.Table.from_pandas(data)
        dt.merge(
            source=data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        ) \
        .when_matched_update_all() \
        .when_not_matched_insert_all() \
        .execute()
    except TableNotFoundError:
        save_data_as_delta(data, data_path, storage_options, "overwrite", partition_cols)

# -----------------------------------------------------------------------
# funcion principal

if __name__ == "__main__":
    api_config = load_config_file("api.conf")
    if not api_config or "alphavantage" not in api_config:
        raise FileNotFoundError(
            "Falta la configuracion de la API. Crear 'config/api.conf' con las credenciales de AlphaVantage."
        )

    base_url = api_config["alphavantage"]["base_url"]
    api_key = api_config["alphavantage"]["api_key"]

    # Antes de ejecutar el script `main.py`, asegurarse de tener el archivo config/api.conf con las credenciales de alphavantage
    # y el archivo config/storage.conf en la raíz del proyecto con el siguiente formato: 

    # [alphavantage]
    # base_url = https://www.alphavantage.co/query
    # api_key = API_KEY

    # [minio]
    # AWS_ENDPOINT_URL = ENDPOINT_URL
    # AWS_ACCESS_KEY_ID = ACCESS_KEY
    # AWS_SECRET_ACCESS_KEY = SECRET_KEY
    # AWS_ALLOW_HTTP = true
    # aws_conditional_put = etag
    # AWS_S3_ALLOW_UNSAFE_RENAME = true
    # bucket_name = BUCKET

    # sino, el data lake se crea de manera local 

    # cargar configuracion de almacenamiento (minio o local)
    storage_config = load_config_file("storage.conf")
    storage_options = None

    if storage_config and "minio" in storage_config:
        print("Usando almacenamiento en MinIO\n")
        minio = storage_config["minio"]
        storage_options = {
            "AWS_ENDPOINT_URL": minio["AWS_ENDPOINT_URL"],
            "AWS_ACCESS_KEY_ID": minio["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": minio["AWS_SECRET_ACCESS_KEY"],
            "AWS_ALLOW_HTTP": minio["AWS_ALLOW_HTTP"],
            "aws_conditional_put": minio["aws_conditional_put"],
            "AWS_S3_ALLOW_UNSAFE_RENAME": minio["AWS_S3_ALLOW_UNSAFE_RENAME"]
        }
        bucket_name = minio["bucket_name"]
        bronze_root = f"s3://{bucket_name}/bronze/alphavantage"
        silver_root = f"s3://{bucket_name}/silver/alphavantage"
        gold_root   = f"s3://{bucket_name}/gold/alphavantage"
    else:
        print("Usando almacenamiento local\n")
        bronze_root = os.path.join(BASE_DIR, "data", "bronze", "alphavantage")
        silver_root = os.path.join(BASE_DIR, "data", "silver", "alphavantage")
        gold_root   = os.path.join(BASE_DIR, "data", "gold", "alphavantage")
        os.makedirs(bronze_root, exist_ok=True)
        os.makedirs(silver_root, exist_ok=True)
        os.makedirs(gold_root, exist_ok=True)

    # CAPA BRONZE - Endpoint dinámico - cotización cripto diaria - incremental: cada día se agrega un nuevo registro
    print("Capa BRONZE\n")
    print("Extracción dinámica: Precio diario de Bitcoin (BTC/USD)\n")

    params_dynamic = {
        "function": "DIGITAL_CURRENCY_DAILY",
        "symbol": "BTC",
        "market": "USD",
        "apikey": api_key
    }

    data_dynamic = get_data(base_url, params=params_dynamic)
    if data_dynamic:
        df_dynamic = build_dynamic_table(data_dynamic)
        if df_dynamic is None:
            print("Transformación dinámica devolvió None, saliendo de la etapa BRONZE dinámico.")
        else:
            print(df_dynamic.head(), "\n")

            data_path_dynamic = s3_join(bronze_root, "crypto_daily")

            upsert_data_as_delta(
                df_dynamic,
                data_path_dynamic,
                predicate="target.datetime = source.datetime",
                storage_options=storage_options,
                partition_cols=["date"]
            )

            print("Datos dinámicos guardados.\n")
            print("=" * 60)
    else:
        print("No se obtuvieron datos dinámicos de la API.")
        
    # CAPA BRONZE - Endpoint estático - cotización actual - extracción full overwrite: un solo valor que se reemplaza en cada ejecución
    print("Extracción estática: Cotización actual USD/EUR\n")

    params_static = {
        "function": "CURRENCY_EXCHANGE_RATE",
        "from_currency": "USD",
        "to_currency": "EUR",
        "apikey": api_key
    }

    data_static = get_data(base_url, params=params_static)
    if data_static:
        df_static = build_static_table(data_static)
        if df_static is None:
            print("Transformación estática devolvió None, saliendo.")
        else:
            df_static = remove_duplicate_columns(df_static)
            print(df_static.head())   

            data_path_static = s3_join(bronze_root, "exchange_rate")
            save_data_as_delta(df_static, data_path_static, storage_options, mode="overwrite")

            print("Datos estáticos guardados.\n")
            print("=" * 60) 

    else:
        print("No se obtuvieron datos estáticos de la API.")

    # CAPA SILVER
    print("Capa SILVER\n")

    # Limpieza de cripto
    silver_crypto_path = s3_join(silver_root, "crypto_daily_clean")
    try:
        dt_crypto_bronze = DeltaTable(s3_join(bronze_root, "crypto_daily"), storage_options=storage_options)
        df_crypto_bronze = dt_crypto_bronze.to_pandas()
        df_crypto_silver = df_crypto_bronze.dropna() #remuevo filas vacias
        df_crypto_silver = df_crypto_silver[df_crypto_silver["close"] > 0] #constraint

        write_deltalake(
            silver_crypto_path,
            df_crypto_silver,
            mode="overwrite",
            schema_mode="merge",
            storage_options=storage_options,
            partition_by=["date"]
        )
        print("Tabla SILVER (crypto) creada exitosamente.\n")
        print(df_crypto_silver.head(), "\n")
    except Exception as e:
        print("Error creando tabla Silver de crypto:", e)

    # Limpieza de tipo de cambio
    silver_fx_path = s3_join(silver_root, "exchange_rate_clean")
    try:
        dt_fx_bronze = DeltaTable(s3_join(bronze_root, "exchange_rate"), storage_options=storage_options)
        df_fx_bronze = dt_fx_bronze.to_pandas()
        df_fx_silver = df_fx_bronze.loc[:, ~df_fx_bronze.columns.duplicated()]

        write_deltalake(
            silver_fx_path,
            df_fx_silver,
            mode="overwrite",
            schema_mode="merge",
            storage_options=storage_options
        )
        print("Tabla SILVER (exchange rate) creada exitosamente.\n")
        print(df_fx_silver.head(), "\n")
    except Exception as e:
        print("Error creando tabla Silver de exchange rate:", e)

    # CAPA GOLD
    print("Capa Gold\n")

    gold_crypto_path = s3_join(gold_root, "crypto_monthly_summary")
    try:
        df_crypto_silver["month"] = pd.to_datetime(df_crypto_silver["date"]).dt.to_period("M").astype(str)
        df_crypto_gold = (
            df_crypto_silver.groupby("month", as_index=False)
            .agg(avg_close=("close", "mean"), max_close=("close", "max"), min_close=("close", "min"))
        )

        write_deltalake(
            gold_crypto_path,
            df_crypto_gold,
            mode="overwrite",
            schema_mode="merge",
            storage_options=storage_options
        )
        print("Tabla GOLD (resumen mensual de crypto) creada exitosamente.\n")
        print(df_crypto_gold.head(), "\n")
    except Exception as e:
        print("Error creando tabla Gold de crypto:", e)

    gold_fx_path = s3_join(gold_root, "exchange_rate_latest")
    try:
        df_fx_gold = df_fx_silver.sort_values(by="last_refreshed", ascending=False).head(1)
        write_deltalake(
            gold_fx_path,
            df_fx_gold,
            mode="overwrite",
            schema_mode="merge",
            storage_options=storage_options
        )
        print("Tabla GOLD (último tipo de cambio) creada exitosamente.\n")
        print(df_fx_gold.head(), "\n")
    except Exception as e:
        print("Error creando tabla Gold de exchange rate:", e)

    print("Pipeline completado exitosamente")