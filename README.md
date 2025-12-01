# Amazon Facility Log Analytics Toolkit

This repository provides a set of tools for analyzing Amazon Log Monitor data (S01–S04 messages). It automates the extraction, processing, and reporting of operational insights such as scanner performance, defects, recirculation loops, chute jam statistics, IAS productivity, destination breakdown, and E-Stop activity.

The toolkit converts raw log files into structured Excel reports containing metrics, summaries, and charts.

---

## Features

* Automated analysis of S01, S02, S03, and S04 Log Monitor files
* Defect classification (no-read, multi-read, sort errors)
* Recirculation and sort code 0 detection
* Excel dashboards with charts and structured data
* Jam chute statistics: frequency, downtime, and duration
* Destination-level flow and sorting breakdown
* IAS (Induction Assist System) productivity analysis
* E-Stop event extraction and reporting
* Tools for induction rate (PPH) and item measurement extraction

---

# 1. Prerequisites

Before using this repository, ensure you have:

1. A computer running Windows, macOS, or Linux
2. Log Monitor `.csv` files (S01–S04 messages)
3. A destination mapping file named:

```
{SITE}_Destination_Mapping.xlsx
```

placed inside the `data/` directory.

Example:
`data/ORF5_Destination_Mapping.xlsx`

You do **not** need to install Python manually.
The project uses **uv**, which automatically installs and manages Python environments.

---

# 2. Installation and Environment Setup

This section explains how to install Visual Studio Code (VS Code), the Python extensions, and how to initialize the environment so the tools run correctly.

---

## 2.1 Install Visual Studio Code

1. Download VS Code from:
   [https://code.visualstudio.com](https://code.visualstudio.com)

2. Install it using the default settings.

3. Open VS Code and install the following extensions:

   * **Python** (Microsoft)
   * **Pylance**
   * **Jupyter**

These are required for:

* Running Python scripts
* Using notebooks
* Autocompletion and debugging

---

## 2.2 Download This Repository

1. Go to the GitHub page
2. Click **Code → Download ZIP**
3. Extract the ZIP file to any location (Desktop, Documents, etc.)

---

## 2.3 Open a Terminal Inside the Repository

Windows:

* Open the folder
* Right-click in empty space
* Select **Open in Terminal** or **Open PowerShell window here**

macOS/Linux:

* Open the folder
* Right-click → **Open in Terminal**

---

## 2.4 Install `uv` (One-Time Setup)

### Windows (PowerShell)

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### macOS / Linux

```bash
curl -lsSf https://astral.sh/uv/install.sh | sh
```

---

## 2.5 Initialize the Environment

Inside the project directory, run:

```bash
uv sync
```

This downloads all required packages (pandas, numpy, openpyxl, matplotlib, tkinter, etc.).
You only need to run this once unless dependencies change.

---

# 3. Running the Tools

All scripts are located inside `src/`.

To run any module, use:

```bash
uv run src/<script_name>.py
```

Each tool provides an interactive interface.

---

## 3.1 `scan.py` — Main Log Analysis (S04)

Runs the primary scanning analysis pipeline.

```bash
uv run src/scan.py
```

The script will prompt you to:

1. Select a Log Monitor raw `.csv` file
2. Choose a time window or type “Full”
3. Enter your site code (ORF5, SAT9, CNO8, etc.)
4. Optionally remove false positives

Output generated:

```
data/reports/Analysis_S04_*.xlsx
```

Contents include:

* Scan defects
* Sort reason breakdown
* Recirculation detection
* Destination flow statistics
* Charts (pie, bar, and stacked visuals)

---

## 3.2 `JamChuteStats.py` — Chute Jam Statistics

Calculates:

* Jam counts
* Total downtime
* Average jam duration
* Jam frequency per chute

Run:

```bash
uv run src/JamChuteStats.py
```

Output:

```
The clipboard now contains the JamChuteStats summary table ready to be pasted into Excel.
```

---

## 3.3 `DBS.py` — Destination Breakdown Summary

Analyzes:

* Sorting flow by destination
* Misroutes and jackpot activity
* Most common destinations
* Ratio of each sort code by lane

Run:

```bash
uv run src/DBS.py
```

Output:

```
The clipboard now contains the DBS summary table ready to be pasted into Excel.
```

---

## 3.4 `IAS.py` — Induction Assist System Analysis

Computes:

* Items inducted per associate
* Induction rate per hour
* Idle periods
* Trend charts

Run:

```bash
uv run src/IAS.py
```

Output:

```
The clipboard now contains the IAS summary table ready to be pasted into Excel.
```

---

## 3.5 `Estops.py` — E-Stop Event Extraction

Provides:

* E-Stop events
* Zone-level analysis
* Duration and frequency
* Summary of operational impact

Run:

```bash
uv run src/Estops.py
```

Output:

```
The clipboard now contains the Estop summary table ready to be pasted into Excel.
```

---

# 4. Additional Tools

## 4.1 Induction Rate (PPH) Using S01 Messages

```bash
uv run src/PPH.py
```

Provides:

* Packages per hour
* Induction throughput
* Delay periods

---

## 4.2 Item Measurements (S02/S03)

1. Open the project in VS Code:

```bash
code .
```

2. Open:

```
notebooks/Item_Measurements.ipynb
```

3. Ensure the Jupyter extension is installed.

This notebook extracts:

* Length
* Width
* Height
* Volume
  from S02/S03 message data.

---

# 5. Excel Output Overview

| Sheet Name       | Purpose                          |
| ---------------- | -------------------------------- |
| Analysis_Results | Main dashboard with charts       |
| Raw_Data         | Structured cleaned raw log lines |
| Window_Data      | Processed data used for analysis |
| Scan_Defects     | Items with sort codes 8, 9, 10   |
| Jam Data         | Chute jam statistics             |
| IAS Summary      | Associate productivity           |
| PPH Summary      | Induction rates                  |

---

# 6. Troubleshooting

### “Mapping file not found”

Ensure a mapping file exists using this pattern:

```
data/{SITE}_Destination_Mapping.xlsx
```

### “‘uv’ is not recognized”

* Close the terminal window
* Open a new one
  Windows requires a refresh after installation.

### Script closes immediately

Make sure you are in the correct directory, where `pyproject.toml` is located.

### Cannot open notebook

Install the **Jupyter** extension in VS Code.