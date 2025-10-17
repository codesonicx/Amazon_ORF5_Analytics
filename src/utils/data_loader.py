import csv
import tkinter as tk
from io import StringIO
from tkinter import filedialog
from typing import Optional

import pandas as pd


def load_data(file_path: Optional[str] = None) -> pd.DataFrame:
    """Load CSV/Excel data either via file picker or fixed path."""

    if not file_path:
        root = tk.Tk()
        root.withdraw()
        file_path = filedialog.askopenfilename(
            title="Select File",
            filetypes=[
                ("CSV files", "*.csv"),
                ("Excel files", "*.xlsx;*.xls"),
                ("All files", "*.*"),
            ],
        )

    if not file_path:
        raise ValueError("No file selected. Please select a valid file.")

    print(f"Loading data from {file_path}...")

    # Handle CSV
    if file_path.endswith(".csv"):
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            text = f.read().replace("\x00", "")  # strip nulls
        buffer = StringIO(text)
        df = pd.read_csv(
            buffer,
            sep=";",
            header=None,
            engine="python",
            quoting=csv.QUOTE_NONE,
            skipinitialspace=True,
            on_bad_lines="skip",
            dtype=str,
        )

    # Handle Excel
    elif file_path.endswith((".xlsx", ".xls")):
        df = pd.read_excel(file_path, dtype=str)

    else:
        raise ValueError("Unsupported file type selected!")

    print(f"Loading data successful, dataframe shape: {df.shape}")
    return df
