import pandas as pd
from utils.file_picker import select_file

# Placeholder – you will import this from your global config/module
from config import MAP_BEUMER_TO_AMAZON 

def clean_part(part: str) -> str:
    """Extract CHU### code from strings like '=CBS01.CHU334+S1-U1'."""
    if not isinstance(part, str):
        return part
    if ":" in part:
        part = part.split(":", 1)[1]
    if "." in part:
        part = part.split(".", 1)[1]
    if "+" in part:
        part = part.split("+", 1)[0]
    return part.strip()

def beumer_to_amazon(beumer_code: str) -> str:
    """Map Beumer chute code (CHU###) to Amazon chute code (Sxxxxx)."""
    for _, mapping in MAP_BEUMER_TO_AMAZON.items():
        if mapping["beumer"] == beumer_code:
            return mapping["amazon"]
    return beumer_code  # fallback if not found

def main():
    filepath = select_file()
    df = pd.read_csv(filepath)

    # Keep only relevant columns
    df = df[["Part", "Message", "Occurrences"]].copy()

    # Clean Beumer chute code
    df["Part"] = df["Part"].apply(clean_part)

    # Map Beumer → Amazon chute code
    df["Amazon_Part"] = df["Part"].apply(beumer_to_amazon)

    # Filter only "Chute: Jam" events
    df = df[df["Message"] == "Chute: Jam"]

    # Aggregate jams per Amazon chute
    summary = (
        df.groupby("Amazon_Part", as_index=False)["Occurrences"]
          .sum()
          .rename(columns={"Occurrences": "total_jams"})
          .sort_values("total_jams", ascending=False)
          .reset_index(drop=True)
    )

    print("\nChute Jam Summary (Amazon chutes)")
    print(f"Unique chutes: {summary.shape[0]}")
    print(summary.head(20))

    # Copy to clipboard
    summary.to_clipboard(index=False)
    print("\n✅ Results copied to clipboard. Paste into Excel with Ctrl+V.")

if __name__ == "__main__":
    main()
