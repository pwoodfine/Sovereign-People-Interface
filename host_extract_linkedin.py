import sqlite3
import pandas as pd
import uuid
import os
import json
import re
import warnings
from engine_core import self_heal_network

warnings.filterwarnings('ignore')

DB_NAME = "people.db"
MEMORY_FILE = "signal_memory.json"

PATTERNS = {
    'email': re.compile(r'[^@]+@[^@]+\.[^@]+'),
    'url': re.compile(r'(linkedin\.com/in/|http|www\.)')
}

SIGNAL_MAP = {
    'first_name': ['first name', 'firstname', 'given name'],
    'last_name': ['last name', 'lastname', 'surname'],
    'name': ['name', 'full name', 'contact'],
    'email': ['email', 'email address', 'mail'],
    'company': ['company', 'organization', 'employer'],
    'position': ['position', 'job title', 'title'],
    'connected_on': ['connected on', 'connection date'],
    'url': ['url', 'profile', 'linkedin id']
}

def load_memory():
    if os.path.exists(MEMORY_FILE):
        with open(MEMORY_FILE, 'r') as f:
            try: return json.load(f)
            except: return {}
    return {}

def save_memory(memory):
    with open(MEMORY_FILE, 'w') as f:
        json.dump(memory, f, indent=4)

def calculate_identity_mass(row):
    """
    Scans row for identity signals. If row is unsplit (single string), 
    it scans the entire string for multiple keywords to ensure a high score.
    """
    score = 0
    cells = [str(c).lower().strip() for c in row.dropna()]
    
    # If the row is a single cell, it might be an unsplit CSV line
    if len(cells) == 1:
        text = cells[0]
        for category, keywords in SIGNAL_MAP.items():
            if any(k in text for k in keywords):
                score += 3 if category in ['email', 'first_name', 'last_name'] else 1
    else:
        # Standard multi-column check
        for cell in cells:
            for category, keywords in SIGNAL_MAP.items():
                if any(k in cell for k in keywords):
                    score += 3 if category in ['email', 'first_name', 'last_name'] else 1
                    break # Break inner loop only to move to next cell
    return score

def process_file(file_path, cursor, memory):
    filename = os.path.basename(file_path)
    
    # 1. High-Sensitivity Load (Uses 'python' engine to auto-detect delimiters)
    try:
        df_raw = pd.read_csv(file_path, header=None, sep=None, engine='python', dtype=str, on_bad_lines='skip')
    except:
        return 0
    
    # 2. Gravity Scan
    best_idx = 0
    max_mass = 0
    for idx, row in df_raw.head(100).iterrows():
        mass = calculate_identity_mass(row)
        if mass > max_mass:
            max_mass = mass
            best_idx = idx
            
    if max_mass < 5:
        print(f"  [!] Gravity Failure: Low signal density in {filename} (Mass: {max_mass}).")
        return 0

    # 3. Handle Delimiter Failure: If best row is still a single column, force split it
    header_row = df_raw.iloc[best_idx].dropna().tolist()
    if len(header_row) == 1 and ',' in header_row[0]:
        print(f"  -> Delimiter Ghost detected. Force-splitting data...")
        df_raw = pd.read_csv(file_path, header=None, skiprows=best_idx, dtype=str).reset_index(drop=True)
        best_idx = 0

    print(f"  -> Gravity Well Locked at Row {best_idx} (Mass: {max_mass})")
    
    # 4. Alignment
    df = df_raw.iloc[best_idx + 1:].copy()
    df.columns = [str(c).strip() for c in df_raw.iloc[best_idx]]
    
    # 5. Column Mapping
    col_map = {}
    norm_cols = {str(c).lower().strip(): c for c in df.columns}
    for category, keywords in SIGNAL_MAP.items():
        for k in keywords:
            match = next((orig for clean, orig in norm_cols.items() if k in clean), None)
            if match:
                col_map[category] = match
                break
    
    # 6. Extraction
    count = 0
    for _, row in df.iterrows():
        email = str(row.get(col_map.get('email'), "")).strip()
        f_name = str(row.get(col_map.get('first_name'), "")).strip()
        l_name = str(row.get(col_map.get('last_name'), "")).strip()
        
        display_name = f"{f_name} {l_name}".strip()
        if display_name.lower() in ['nan', 'nan nan', '']: display_name = None
        if email.lower() in ['nan', '']: email = None
        
        if not (display_name or email): continue
        
        anchor = email if email else display_name
        sov_id = f"RAW-{uuid.uuid5(uuid.NAMESPACE_DNS, str(anchor).lower()).hex[:8].upper()}"

        try:
            cursor.execute('''
                INSERT INTO entities (sovereign_id, display_name, entity_type, status)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(sovereign_id) DO UPDATE SET status = status
            ''', (sov_id, display_name if display_name else anchor, 'Individual', 'Discovery'))
            
            meta = [f"Source: {filename}", f"Anchor: {anchor}"]
            if 'company' in col_map: meta.append(f"Company: {row[col_map['company']]}")
            if 'position' in col_map: meta.append(f"Position: {row[col_map['position']]}")
            
            cursor.execute('''
                INSERT INTO metadata_logs (action, module, details)
                VALUES (?, ?, ?)
            ''', ('SOCIAL_INGEST', 'host_extract_linkedin', f"{sov_id} :: {' | '.join(meta)}"))
            count += 1
        except: continue
        
    print(f"  [+] Ingested {count} social records from {filename}.")
    return count

def run_ingest(path):
    print(f"--- 2030 Infinite Social Ingest: {path} ---")
    memory = load_memory()
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    if os.path.isfile(path):
        process_file(path, cursor, memory)
    else:
        for f in os.listdir(path):
            if f.lower().endswith('.csv'):
                process_file(os.path.join(path, f), cursor, memory)

    save_memory(memory)
    conn.commit()
    conn.close()
    self_heal_network()

if __name__ == "__main__":
    run_ingest(input("Enter path to LinkedIn CSV: ").strip())
