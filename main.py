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

def build_table(data, series_key):
    df = pd.DataFrame.from_dict(data[series_key], orient="index")
    df.columns = [c.split(". ")[-1].split(" (")[0] for c in df.columns]
    df.index.name = "date"
    df.reset_index(inplace=True)
    return df
    

# funcion principal

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("pipeline.conf")

    base_url = config["alphavantage"]["base_url"]
    api_key = config["alphavantage"]["api_key"]

    # Endpoint dinámico - cotización cripto diaria
    params = {
        "function": "DIGITAL_CURRENCY_DAILY",
        "symbol": "BTC",
        "market": "USD",
        "apikey": api_key
    }

    data = get_data(base_url, endpoint="", params=params)

    if data:
        series_key = next((k for k in data.keys() if k.startswith("Time Series")), None)
        if series_key:
            df = build_table(data, series_key)

            print("\nExtracción exitosa. Primeras filas:\n")
            print(df.head())
        else:
            print("No se encontró la clave esperada en la respuesta:")
            print(list(data.keys()))