# 🏛️ Sovereign-Talent-Engine (Host Authority)
**Location:** iMac 12.1 Host (i5-2400S)
**Architecture:** Infinite Gravity Ingestion // Self-Healing Substrate

## 🌌 I. Initialization & Substrate Architecture
Foundational scripts to architect the relational vault.
* **`host_init_db.py`**: Constructs the `people.db` SQLite schema. Defines `Entities` (Immutable Anchors), `Affiliations` (Elastic Bonds), and `Metadata_Logs` (Provenance Chain).
* **`host_init_coa.py`**: Seeds the **Chart of Archetypes (COA)**. Injects the 20-point Digital Twin weights (11 Dynamic Archetypes, 55 Immutable Domains) for Information Gain scoring.
* **`manage_sovereign_tables.py`**: Data utility for the COA. Handles bi-directional CSV synchronization for manual tuning of the intelligence model.

## 🛰️ II. Ingestion Layer (Infinite Gravity)
A delimiter-agnostic mining layer designed for unstructured or "hostile" data sources.
* **`host_extract_spreadsheets.py`**: The "Universal Miner." Scans multi-tab workbooks using **Identity Mass** to bypass junk headers and auto-map column signals.
* **`host_extract_linkedin.py`**: The "Graph Ingestor." Specialized for LinkedIn `Connections.csv`. Features **"Delimiter Ghost"** detection to force-split columns and bypass export disclaimers.
* **`host_extract_emails.py`**: The "Log Miner." Performs **Recursive Deep Scans** on Maildirs and raw directories to extract sender/recipient signals and communication context.

## 🧠 III. The Intelligence Core
* **`engine_core.py`**: The centralized **System Brain**. Executed automatically post-ingest to perform real-time **Identity Resolution**. 
    * **Hard Merge:** Unifies fragments sharing identical Email anchors.
    * **Provenance:** Maintains the source-tagging for every merged signal to ensure total data accountability.

---
**Protocol:**
1. **Initialize:** `python3 host_init_db.py` followed by `python3 host_init_coa.py`.
2. **Ingest:** Execute any `host_extract_*.py` targeting the `Data_Archive/`.
3. **Verify:** Use SQLite to monitor the maturation of the `Discovery` queue.
