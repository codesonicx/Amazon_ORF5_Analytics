import tkinter as tk
from tkinter import filedialog

def select_file():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(
        title="Select File",
        filetypes=[
            ("CSV files", "*.csv"),
            ("Excel files", "*.xlsx;*.xls"),
            ("All files", "*.*"),
        ]
    )

    return file_path