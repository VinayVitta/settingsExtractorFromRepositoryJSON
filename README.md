# 🧩 Qlik QEM Repository Settings Extractor

**Author:** Vinay Vitta – Qlik Professional Services  
**Created:** May 2025  
**Language:** Python 3.10+  
**License:** Qlik PS – QDI  

---

## 📘 Overview

The **Settings Extractor from Repository JSON** is a Python-based utility designed to read **Qlik Replicate (QEM)** repository JSON and QEM export files (`.tsv`), extract task, source, target, and server metadata, and produce structured CSV and Word summaries.

It automates health checks, documentation, and platform review processes for Qlik Data Integration (QDI) environments.

---

## ⚙️ Features

✅ Extracts replication **task settings** from JSON repository exports  
✅ Reads and cleans **QEM export files** (`.tsv`)  
✅ Collects **source and target connection metadata**  
✅ Captures **server settings, schedules, and notifications**  
✅ Merges extracted data into multiple CSV outputs  
✅ Generates a professional **Word summary report**  
✅ (Optional) Writes results to **Google BigQuery** for analytics  

---

## 🧱 Project Structure

```
settingsExtractorFromRepositoryJSON/
│
├── main.py                               # Main driver script
│
├── helpers/
│   ├── utils.py                          # File read/write, TSV cleaning, CSV output
│   ├── summary.py                        # Generates Word summary reports
│   ├── bigQueryWriteData.py              # Optional: Write output to BigQuery
│
├── databases/
│   ├── sources/                          # Source system extractors
│   │   ├── src_oracle.py
│   │   ├── src_sqlserver.py
│   │   ├── src_sqlserver_mscdc.py
│   │   ├── src_hana_app_db.py
│   │   ├── src_db2zos.py
│   │   ├── src_postgres.py
│   │   ├── src_mongodb.py
│   │
│   ├── targets/                          # Target system extractors
│   │   ├── tar_snowflake.py
│   │   ├── tar_azure_adls.py
│   │   ├── tar_logStream.py
│   │   ├── tar_kafka.py
│   │
│   ├── tasks/
│   │   ├── retrieveTaskSettings.py
│   │   ├── retrieveTables.py
│   │
│   ├── serverSettings/
│       ├── retrieveServerSettings.py
│       ├── retrieveScheduledTasks.py
│       ├── retrieveNotifications.py
│
├── ui/                                   # <-- For your upcoming Web UI (see below)
│
├── requirements.txt                      # Python dependencies
└── README.md                             # This file
```
---
## 🧱 Project UI Structure

```
settingsExtractorFromRepositoryJSON/
│
├── backend/                      # FastAPI backend
│   ├── app.py                    # Main FastAPI app
│   ├── routers/
│   │   ├── extract.py            # Routes for upload & extraction
│   │   └── download.py           # Routes to serve generated files
│   ├── services/
│   │   └── runner.py             # Wraps your main extraction logic
│   └── temp_uploads/             # Temporary upload folder (auto-created)
│
├── frontend/                      # React frontend
│   ├── package.json
│   ├── src/
│   │   ├── components/
│   │   │   ├── FileUploader.jsx
│   │   │   └── OutputDownloader.jsx
│   │   ├── App.jsx
│   │   └── api.js                # API calls to FastAPI backend
│   └── public/
│
├── helpers/                       # Your existing helpers
├── databases/                     # Your existing database extractors
└── README.md

```
---

## 🚀 Usage

### 1️⃣ **Prepare Input Files**
- JSON repository export(s) from QEM  
- QEM Export `.tsv` file (Tasks export from QEM UI)

Example:
```
C:\Users\<user>\Qlik\Customers\<CustomerName>\PlatformReview\
    ├── replicate_repository_001.json
    ├── replicate_repository_002.json
    └── AemTasks_2025-06-11_15.23.08.565.tsv
```

---

### 2️⃣ **Run the Script**

```bash
python main.py
```

All outputs are written into a timestamped subfolder:
```
run_output_20251009_142233/
    ├── taskSettings_20251009_142233.csv
    ├── tables_20251009_142233.csv
    ├── qem_data_20251009_142233.csv
    ├── task_settings_qem_merge_20251009_142233.csv
    ├── exportRepositoryCSV_20251009_142233.csv
    ├── serverSettings_20251009_142233.csv
    ├── serverNotifications_20251009_142233.csv
    ├── serverScheduleSettings_20251009_142233.csv
    └── task_summary_20251009_142233.docx
```

---

### 3️⃣ **Optional – Write to BigQuery**

If you want to store merged data in BigQuery, configure credentials in:
```python
helpers/bigQueryWriteData.py
```

---

## 📦 Dependencies

Install all requirements:
```bash
pip install -r requirements.txt
```

Example `requirements.txt`:
```text
pandas
openpyxl
python-docx
google-cloud-bigquery
google-auth
```

---

## 🧩 Adding a Web UI

You can extend this tool with a **browser-based UI** to allow file upload and download of outputs.

### Option 1 – **Streamlit (Python-native UI)**
Easiest and fastest. Add under `/ui/streamlit_app.py`.

Run with:
```bash
streamlit run ui/streamlit_app.py
```

You’ll get:
- A file uploader for JSON and TSV files  
- A “Run Extraction” button  
- Download links for all generated outputs  

---

### Option 2 – **React + FastAPI (Full-stack architecture)**

If you want a more **industry-standard enterprise setup**, use this:

```
settingsExtractorFromRepositoryJSON/
│
├── backend/               # FastAPI layer wrapping your existing Python logic
│   ├── app.py
│   ├── routers/
│   │   ├── extract.py     # Routes to run extraction
│   │   ├── download.py
│   │
│   ├── services/
│   │   ├── runner.py      # Imports and calls process_repository() from main
│
└── frontend/              # React (TypeScript) app
    ├── src/
    │   ├── components/
    │   ├── pages/
    │   └── api/
    └── package.json
```

- `FastAPI` exposes endpoints like:
  - `POST /upload` → to upload JSON + TSV
  - `POST /run` → triggers extraction
  - `GET /download/{file}` → returns results
- `React` provides the UI with progress and download links.

---

## 📘 Example Output Summary

| Output File | Description |
|--------------|--------------|
| `taskSettings_*.csv` | Extracted task configurations |
| `tables_*.csv` | Table-level mappings |
| `qem_data_*.csv` | Cleaned QEM Export |
| `task_settings_qem_merge_*.csv` | Task settings merged with QEM export |
| `exportRepositoryCSV_*.csv` | Final repository export (all merged) |
| `task_summary_*.docx` | Summary Word report |
| `serverSettings_*.csv` | Server settings |
| `serverNotifications_*.csv` | Notification setup |
| `serverScheduleSettings_*.csv` | Schedule setup |

---

## 🧰 Next Steps
- [ ] Add a `/ui` Streamlit interface for easy use  
- [ ] Convert into a FastAPI backend for automation  
- [ ] Dockerize for repeatable deployments  

---

## 📄 License
This project is property of **Qlik Professional Services (QDI)** and intended for internal and customer delivery use only.
