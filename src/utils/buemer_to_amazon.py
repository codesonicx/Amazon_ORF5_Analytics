import pandas as pd

from utils.file_picker import select_file

# Load the Excel file
path = select_file()
df = pd.read_excel(path)

# Sort by Beumer column
df_sorted = df.sort_values(by="Buemer", ascending=True)

# Reset index (optional, just for clean output)
df_sorted = df_sorted.reset_index(drop=True)

result = "\n".join([f"{row['Buemer']}: \"{row['Amazon']}\"," for _, row in df_sorted.iterrows()])
print(result)
