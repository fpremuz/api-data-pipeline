"""
Trabajo Práctico - Data Engineering
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

# -----------------------------------------------------------------------
# funciones auxiliares
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def load_config_file(filename):
    # Carga un archivo .conf desde la carpeta config/
    config_path = os.path.join(BASE_DIR, "config", filename)
    if not os.path.exists(config_path):
        print(f"No se encontró {config_path}.")
        return None
    parser = configparser.ConfigParser()
    parser.read(config_path)
    return parser

def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    # http a la API y devuelve datos en formato JSON
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
            if data_field:
              data = data[data_field]
        except:
            print("El formato de respuesta no es el esperado")
            return None
        return data

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        return None

def build_dynamic_table(data):
    # convierte las cotizaciones en un dataframe limpio
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
    # extrae el diccionario principal
    try:
        rate = data["Realtime Currency Exchange Rate"]
    except KeyError:
        print("No se pudo obtener tipo de cambio USD/EUR. Se continuará sin FX.")
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
        base_path = f"s3://{bucket_name}/bronze"
        bronze_api_path = f"{base_path}/alphavantage"
    else:
        print("Usando almacenamiento local\n")
        base_path = "./data/bronze"
        if storage_options is None:
            os.makedirs(base_path, exist_ok=True)
            bronze_api_path = f"{base_path}/alphavantage"

    # Endpoint dinámico - cotización cripto diaria - incremental: cada día se agrega un nuevo registro
    print("Extracción dinámica: Precio diario de Bitcoin (BTC/USD)\n")

    params_dynamic = {
        "function": "DIGITAL_CURRENCY_DAILY",
        "symbol": "BTC",
        "market": "USD",
        "apikey": api_key
    }

    data_dynamic = get_data(base_url, endpoint="", params=params_dynamic)
    if data_dynamic:
        df_dynamic = build_dynamic_table(data_dynamic)
        df_dynamic["ingestion_timestamp"] = datetime.now()
        df_dynamic = df_dynamic.dropna(subset=["datetime", "close"])
        df_dynamic = df_dynamic[df_dynamic["close"] > 0]
        print(df_dynamic.head(), "\n")

        data_path_dynamic = f"{bronze_api_path}/digital_currency_daily"

        if storage_options is None:
            os.makedirs(data_path_dynamic, exist_ok=True)

        predicate = "target.datetime = source.datetime"

        try:
            dt = DeltaTable(data_path_dynamic, storage_options=storage_options)
            prev_count = dt.to_pandas().shape[0]
            # Si la tabla ya existe, hago UPSERT (merge)
            data_pa = pa.Table.from_pandas(df_dynamic)
            dt.merge(
                source=data_pa,
                source_alias="source",
                target_alias="target",
                predicate="target.datetime = source.datetime"
            ) \
            .when_matched_update_all() \
            .when_not_matched_insert_all() \
            .execute()

            new_count = dt.to_pandas().shape[0]
            print("Tabla Delta actualizada (UPSERT completado).")
            print(f"Filas antes: {prev_count}")
            print(f"Filas después: {new_count}")
            print(f"Filas nuevas insertadas/actualizadas: {new_count - prev_count}")
        except TableNotFoundError:
            write_deltalake(
                data_path_dynamic,
                df_dynamic,
                mode="overwrite",
                storage_options=storage_options,
                partition_by=["date"],
            )
            print("Tabla Delta inicializada (estructura vacía creada).")

            try:
                dt = DeltaTable(data_path_dynamic, storage_options=storage_options)
                dt.alter.add_constraint({"positive_close": "close > 0"})

                # === Time Travel ===
                last_version = dt.version()
                print(f"Versión actual de la tabla: {last_version}")
                print("Historial de versiones recientes:")
                print(dt.history(3))  # últimas 3 versiones
            except Exception as e:
                print("Mantenimiento omitido:", e)

        print("Datos dinámicos guardados.\n")
        print("=" * 60)


    # Endpoint estático - cotización actual - extracción full overwrite: un solo valor que se reemplaza en cada ejecución
    print("Extracción estática: Cotización actual USD/EUR\n")

    params_static = {
        "function": "CURRENCY_EXCHANGE_RATE",
        "from_currency": "USD",
        "to_currency": "EUR",
        "apikey": api_key
    }

    data_static = get_data(base_url, endpoint="", params=params_static)
    if data_static:
        df_static = build_static_table(data_static)
        df_static = remove_duplicate_columns(df_static)
        df_static["ingestion_timestamp"] = datetime.now()
        print(df_static.head())   

        data_path_static = f"{bronze_api_path}/currency_exchange_rate"
        
        if storage_options is None:
            os.makedirs(data_path_static, exist_ok=True)

        save_data_as_delta(df_static, data_path_static, storage_options, mode="overwrite")

        print("Datos estáticos guardados.\n")
        print("=" * 60) 