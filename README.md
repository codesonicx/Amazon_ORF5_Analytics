# Log Monitor Analysis Toolkit

This repository provides tools to generate **analysis reports** from log monitor data.
You define time windows of interest, and the scripts produce summaries including:

* **S04 checks**
* **Rate PPH (Packages per Hour) for inductions**
* **Defect analysis** (sort codes, categories, scan defects, etc.)

---

## 📂 Repository Structure

* **`src/`** → main analysis scripts.
* **`data/`** → (optional) folder for saving generated Excel reports.

---

## 🚀 Getting Started

### 1. Install [uv](https://astral.sh/uv)

If you don’t have `uv` installed, you can install it via PowerShell:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Sync dependencies

From the project root:

```bash
uv sync
```

### 3. Run analysis

Execute a script from the `src/` folder, for example:

```bash
uv run src/SO4.py
```

Replace `SO4.py` with the script you want to run.

---

## 📊 Reports

The scripts will:

* Parse **log monitor logs**.
* Filter data based on your selected **time window**.
* Generate structured Excel reports with multiple sheets, including:

  * `Analysis_Results` (summary, defect breakdown, sort code counts, charts)
  * `Raw_Data`
  * `Clean_Data`
  * `Window_Data`
  * `Scan_Defects`

---

## ✅ Example Workflow

1. Prepare your **log monitor logs**.
2. Choose a time window (e.g., 11:05 -> 11:35 '30-minute window').
3. Run the script → Excel reports are exported into the `data/` folder.
4. Review S04 checks and PPH performance in the generated analysis.

---

## ⚡ Notes

* Some scripts provide **interactive cleanup options** (e.g., handling false-positive scan defects).
* Reports include both **tables and native Excel charts** (no external PNGs).
* Designed for simplicity → minimal user input required.