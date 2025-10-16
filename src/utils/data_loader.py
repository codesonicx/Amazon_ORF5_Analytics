import tkinter as tk
from tkinter import filedialog
import pandas as pd
import csv
from io import StringIO

def load_data() -> pd.DataFrame:
    """Open a file dialog, read CSV, clean nulls, and return a DataFrame."""
    # File selection
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        title="Select File",
        filetypes=[
            ("CSV files", "*.csv"),
            ("Excel files", "*.xlsx;*.xls"),
            ("All files", "*.*"),
        ]
    )

    if not file_path:
        raise ValueError("No file selected. Please select a valid file.")

    # Read and strip NULs
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read().replace("\x00", "")

    # Convert to buffer and DataFrame
    buffer = StringIO(text)
    df = pd.read_csv(
        buffer,
        sep=";",
        header=None,
        engine="python",
        quoting=csv.QUOTE_NONE,
        skipinitialspace=True,
        on_bad_lines="skip",
        dtype=str
    )

    print(f"Loading data successful, dataframe shape: {df.shape}")
    print("Parsing data...")
    return df
