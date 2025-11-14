from deltalake import vacuum
import os

DATA_LAKE_ROOT = "./data"

def vacuum_tables(retention_hours=168):  
    """
    Ejecuta VACUUM sobre todas las tablas Delta del data lake.
    VACUUM elimina archivos obsoletos que ya no son usados por el delta log.
    retention_hours = 168 (7 días) es el valor seguro por defecto.
    """
    for root, dirs, files in os.walk(DATA_LAKE_ROOT):
        if "_delta_log" in dirs:
            table_path = root
            print(f"VACUUM: {table_path}")

            try:
                vacuum(table_path, retention_hours=retention_hours, enforce_retention_duration=True)
                print(f"SUCCESS: vacuumed {table_path}")
            except Exception as e:
                print(f"ERROR vacuuming {table_path}: {e}")


if __name__ == "__main__":
    vacuum_tables()