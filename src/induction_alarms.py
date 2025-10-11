import pandas as pd
from utils.file_picker import select_file

filepath = select_file()

# Load CSV file and drop unused columns.
df = pd.read_csv(filepath)

# Columns to remove
drop_cols = ["Group", "Type", "Priority", "Part", "Alarm Number", "Duration"]
df = df.drop(columns=drop_cols, errors="ignore")

print("\nData loaded")
print(f"Rows: {len(df)} | Columns: {list(df.columns)}")

# Group and aggregate
summary = (
    df.groupby("Text", as_index=False)["Amount"]
        .sum()
        .rename(columns={"Amount": "total_amount"})
        .sort_values("total_amount", ascending=False)
        .reset_index(drop=True)
)

print("\nCategory Summary")
print(f"Unique categories: {summary.shape[0]}")


# Copiar el DataFrame al portapapeles
summary.to_clipboard(index=False)

print("\n✅ Resultados copiados al portapapeles. Pégalos en Excel con Ctrl+V.")

