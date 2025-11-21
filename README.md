# 📦 Amazon ORF5 Analytics (and other sites)

**Turn raw Log Monitor data into clear, actionable Excel reports.**

This toolkit automates the analysis of Amazon facility logs (S01, S02, S03, S04 messages). Instead of staring at Matrix-style text files, you can run this tool to instantly generate charts, detect recirculation loops, analyze defects, and calculate scanner performance.

---

## 🟢 Prerequisites

Before you start, make sure you have:
1.  **A Computer** (Windows, macOS, or Linux).
2.  **Log Files**: The `.csv` files exported from Log Monitor.
3.  **Mapping File**: An Excel file named `{SITE}_Destination_Mapping.xlsx` (e.g., `ORF5_Destination_Mapping.xlsx`) inside the `data/` folder.

> **Note:** You do **not** need to manually install Python. The tool we use (`uv`) handles everything for you.

---

## 🛠️ Installation (First Time Setup)

If you have never used a terminal before, just follow these steps exactly.

### Step 1: Download this Repository
1.  Scroll to the top of this page.
2.  Click the green **<> Code** button and select **Download ZIP**.
3.  Unzip the folder to your **Desktop** or **Documents**.

### Step 2: Open a Terminal
1.  Open the unzipped folder.
2.  **Right-click** in the empty white space of the folder.
3.  Select **"Open in Terminal"** (Windows) or "Open PowerShell window here".
    *   *Tip: If you don't see it, hold `Shift` + `Right-click`.*

### Step 3: Install `uv`
Copy and paste the command below into your terminal and press **Enter**:

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**Mac/Linux:**
```bash
curl -lsSf https://astral.sh/uv/install.sh | sh
```

### Step 4: Sync Project
Type this command and press **Enter**:
```bash
uv sync
```
*(This downloads the necessary tools to read Excel files and process data. It only needs to be done once.)*

---

## 🚀 How to Run the Scan Analyzer

The main tool is `scan.py`. Here is how to run it and what to expect.

### 1. Start the Script
In your terminal, type:
```bash
uv run src/scan.py
```

### 2. Follow the On-Screen Prompts
The script is interactive. It will ask you for the following:

1.  **Select Data File:** A window will pop up. Choose your raw Log Monitor file.
2.  **Time Window:**
    *   Enter start/end times (e.g., `14:00` to `15:00`) to focus on a specific shift.
    *   *Tip: Just type **Full** to analyze the entire file.*
3.  **Enter Site Name:**
    *   Type your site code (e.g., `ORF5`, `SAT9`, `CNO8`).
    *   *The script uses this to find the correct Mapping file in the `data/` folder.*
4.  **Cleanup (False Positives):**
    *   It will ask: *Do you want to clean up wrong sortCodes?*
    *   **Type `no`** for a standard report.
    *   **Type `yes`** if you have a list of specific Index IDs that you want to ignore (useful for removing known scanner glitches from the report).

### 3. Get Your Report
When finished, the script will say:
`Analysis results saved to: data/reports/Analysis_SO4_...xlsx`

Go to the `data/reports` folder to open your new Excel file.

---

## 🧠 What does `scan.py` actually do?

If you are curious about what is happening under the hood, here is the logic:

1.  **Filters the Noise:** It looks through thousands of log lines and throws away everything except "S04" messages (Message Code 54177).
2.  **Decodes the "Matrix":** Raw logs look like `key:[value1, value2]`. The script creates a structured table, separating "Amazon Destinations" from "Beumer" and "Jackpot" destinations.
3.  **Smart Package Detection:**
    *   It groups multiple scans of the same item into a **Single Package ID**.
    *   It detects if a package is **Normal**, **No Read** (scanner couldn't see it), or **Multi Read** (confusing barcodes).
4.  **Recirculation Detection:** It identifies packages that are stuck in a loop (Sort Code 0) and filters them out so they don't ruin your statistics.
5.  **Visual Reporting:** It generates an Excel file with:
    *   **Pie Charts:** Showing defect percentages.
    *   **Bar Charts:** Showing top sort reasons.
    *   **Stacked Charts:** Comparing Sort Reasons vs. Amazon Destinations.

---

## 📊 Understanding the Excel Output

*   **`Analysis_Results`**: The dashboard. Contains summary tables, the "Jackpot" breakdown, and all charts.
*   **`Raw_Data`**: The data after basic formatting but before deep analysis.
*   **`Scan_Defects`**: A list of items with Sort Codes 8, 9, or 10 (Scan Defects).
*   **`Window_Data`**: The fully processed data used for the charts.

---

## ❓ Troubleshooting

**"Mapping file not found"**
*   Ensure you have a file named exactly `{SITE}_Destination_Mapping.xlsx` in the `data/` folder.
*   Example: If you typed `ORF5` as the site name, you must have `data/ORF5_Destination_Mapping.xlsx`.

**"The term 'uv' is not recognized"**
*   Close your terminal window completely and open a new one. Windows needs to refresh to see the new tool.

**Script crashes immediately**
*   Check if your raw log file is empty.
*   Make sure you are running the command from the main folder (where `pyproject.toml` is located).

---

## Extra

This repository has another built in scripts that can help you to retrive more information from the logs, and in order to run those scripts you can use the following commands:

- Calculate the rate per Induction using S01 messages: 
```bash
uv run src/PPH.py
```

- Calculate the item measuremnets using S02 and S03 messages: 
open vscode using the command:
```bash
code .
```

Then open the folder `notebooks` and run the notebook `Item_Measurements.ipynb`, you must have to install all the extension required to run jupyter notebooks in vscode.