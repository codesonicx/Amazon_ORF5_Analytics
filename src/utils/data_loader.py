import csv
import tkinter as tk
from io import StringIO
from tkinter import filedialog
from typing import Literal, Optional

import pandas as pd


def select_file(
    file_types: Optional[list[Literal["csv", "excel"]]] = None,
    file_path: Optional[str] = None,
) -> str:
    """
    Select a file path either via file picker or use provided path.

    Args:
        file_types: List of file types to filter in dialog. Options: ["csv", "excel"].
                   If None, allows both CSV and Excel files.
        file_path: Optional pre-defined file path. If provided, skips file dialog.

    Returns:
        str: The selected or provided file path.

    Raises:
        ValueError: If no file is selected or path is invalid.
    """
    if file_path:
        return file_path

    # Build filetypes for dialog based on preferences
    if file_types is None:
        file_types = ["csv", "excel"]

    filetypes = []

    # Add preferred types first
    for ft in file_types:
        if ft == "csv":
            filetypes.append(("CSV files", "*.csv"))
        elif ft == "excel":
            filetypes.append(("Excel files", "*.xlsx;*.xls"))

    # Always add "All files" at the end
    filetypes.append(("All files", "*.*"))

    root = tk.Tk()
    root.withdraw()
    selected_path = filedialog.askopenfilename(
        title="Select File",
        filetypes=filetypes,
    )
    root.destroy()

    if not selected_path:
        raise ValueError("No file selected. Please select a valid file.")

    return selected_path


def load_data(
    file_path: Optional[str] = None,
    file_types: Optional[list[Literal["csv", "excel"]]] = None,
) -> pd.DataFrame:
    """
    Load CSV/Excel data either via file picker or fixed path.

    Args:
        file_path: Optional pre-defined file path. If None, opens file dialog.
        file_types: List of file types to filter in dialog. Options: ["csv", "excel"].
                   If None, allows both CSV and Excel files.

    Returns:
        pd.DataFrame: Loaded data as a pandas DataFrame.

    Raises:
        ValueError: If no file is selected or unsupported file type.
    """
    # Use select_file to get the path
    file_path = select_file(file_types=file_types, file_path=file_path)

    print(f"Loading data from {file_path}...")

    # Handle CSV
    if file_path.lower().endswith(".csv"):
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
    elif file_path.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(file_path, dtype=str)

    else:
        raise ValueError(
            f"Unsupported file type: {file_path}\nSupported types: .csv, .xlsx, .xls"
        )

    print(f"Loading data successful, dataframe shape: {df.shape}")
    return df
