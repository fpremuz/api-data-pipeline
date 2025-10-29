# https://www.alphavantage.co/documentation/

import requests
import pandas as pd
from datetime import datetime, timedelta
import configparser

def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parámetros:
    base_url (str): La URL base de la API.
    endpoint (str): El endpoint de la API al que se realizará la solicitud.
    params (dict): Parámetros de consulta para enviar con la solicitud.
    data_field (str): El nombre del campo en el JSON que contiene los datos.
    headers (dict): Encabezados para enviar con la solicitud.

    Retorna:
    dict: Los datos obtenidos de la API en formato JSON.
    """
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

    return df

def build_static_table(data):
    df = pd.DataFrame([data["Realtime Currency Exchange Rate"]])
    df.columns = [c.split(" ")[-1] for c in df.columns]  
    return df
    
# -----------------------------------------------------------------------
# funcion principal

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("pipeline.conf")

    base_url = config["alphavantage"]["base_url"]
    api_key = config["alphavantage"]["api_key"]

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
        print(df_dynamic.head(), "\n")
        print("=" * 60)


    # Endpoint estático - cotización actual - extracción full: un solo valor que se reemplaza en cada ejecución
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
        print(df_static.head())    