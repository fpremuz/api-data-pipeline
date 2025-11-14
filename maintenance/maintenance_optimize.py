from deltalake import optimize
import os

# Directorio raíz del data lake
DATA_LAKE_ROOT = "./data"

def optimize_tables():
    """
    Ejecuta OPTIMIZE sobre todas las tablas Delta en el data lake.
    OPTIMIZE compacta archivos pequeños en menos archivos grandes (Parquet).
    Mejora performance de lectura / scans.
    """
    for root, dirs, files in os.walk(DATA_LAKE_ROOT):
        if "_delta_log" in dirs:  # indica que es una tabla Delta
            table_path = root
            print(f"OPTIMIZING: {table_path}")

            try:
                optimize(table_path)
                print(f"SUCCESS: optimized {table_path}")
            except Exception as e:
                print(f"ERROR optimizing {table_path}: {e}")


if __name__ == "__main__":
    optimize_tables()