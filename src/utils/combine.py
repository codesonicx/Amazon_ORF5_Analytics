import os
import glob
import pandas as pd

def combine_logs_to_csv(log_dir: str, output_csv: str) -> None:
    """
    Combina todos los archivos .log en un único CSV con separador ';'.
    """
    all_files = glob.glob(os.path.join(log_dir, "*.log"))
    if not all_files:
        print(f"No se encontraron archivos .log en {log_dir}")
        return

    print(f"Encontrados {len(all_files)} archivos log en {log_dir}")

    dfs = []
    for f in all_files:
        try:
            # Usar separador ';' (clave para que tus columnas coincidan)
            df = pd.read_csv(f, sep=";", engine="python", encoding="utf-8", on_bad_lines="skip")
            df["source_file"] = os.path.basename(f)  # opcional
            dfs.append(df)
        except Exception as e:
            print(f"❌ Error leyendo {f}: {e}")

    if not dfs:
        print("No se pudo leer ningún archivo válido.")
        return

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_csv(output_csv, index=False, encoding="utf-8")

    print(f"\n✅ CSV combinado generado en: {output_csv}")
    print(f"Total de filas: {len(combined_df)} | Columnas: {len(combined_df.columns)}")

if __name__ == "__main__":
    log_dir = input("Ruta de la carpeta con logs: ").strip()
    output_csv = "combined_logs.csv"
    combine_logs_to_csv(log_dir, output_csv)
