import sqlite3
import pandas as pd
import uuid
import os
import json
import re
import warnings
from engine_core import self_heal_network  # INTEGRATION

warnings.filterwarnings('ignore')

DB_NAME = "people.db"
MEMORY_FILE = "signal_memory.json"

# --- 2030 PATTERN DNA ---
PATTERNS = {
    'email': re.compile(r'[^@]+@[^@]+\.[^@]+'),
    'phone': re.compile(r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'),
    'url': re.compile(r'(linkedin\.com/in/|http|www\.)')
}

SIGNAL_MAP = {
    'name': ['name', 'full name', 'fullname', 'contact', 'person', 'recipient', 'sender', 'to', 'from'],
    'first_name': ['first name', 'firstname', 'fname'],
    'last_name': ['last name', 'lastname', 'lname'],
    'email': ['email', 'e-mail', 'mail', 'email address', 'email #1', 'email #2'],
    'phone': ['phone', 'mobile', 'cell', 'telephone', 'number'],
    'url': ['linkedin', 'url', 'website', 'profile'],
    'unsubscribed': ['unsubscribed', 'unsubscribe', 'opt-out', 'status']
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

def get_row_score(row_values):
    score = 0
    row_str = [str(x).lower().strip().replace('_', ' ') for x in row_values]
    for val in row_str:
        for category, keywords in SIGNAL_MAP.items():
            if any(k == val for k in keywords):
                score += 3 if category in ['name', 'email'] else 1
    return score

def process_matrix(df, filename, sheet_name, cursor, memory):
    # 1. Gravity Scan (Find Header)
    best_score = -1
    header_idx = 0
    for idx, row in df.head(1000).iterrows():
        score = get_row_score(row.fillna('').astype(str).tolist())
        if score > best_score:
            best_score = score
            header_idx = idx

    df.columns = df.iloc[header_idx]
    data_df = df.iloc[header_idx + 1:].reset_index(drop=True)

    # 2. Build Map
    col_map = {}
    normalized_cols = {str(c).lower().strip().replace('_', ' '): c for c in df.columns}
    
    for field, keywords in SIGNAL_MAP.items():
        for k in keywords:
            if k in normalized_cols:
                col_map[field] = normalized_cols[k]
                break

    # 3. Self-Heal via Content
    for col in data_df.columns:
        col_clean = str(col).lower().strip()
        if col_clean in memory:
            col_map[memory[col_clean]] = col
            continue
            
        sample = data_df[col].dropna().astype(str).head(50)
        if sample.empty: continue
        
        if sample.apply(lambda x: bool(PATTERNS['email'].search(x))).mean() > 0.4:
            col_map['email'] = col
            memory[col_clean] = 'email'
        elif sample.apply(lambda x: bool(PATTERNS['phone'].search(x))).mean() > 0.4:
            col_map['phone'] = col
            memory[col_clean] = 'phone'

    if not col_map: return 0

    # 4. Extract
    count = 0
    is_unsub_tab = 'unsubscribe' in sheet_name.lower()
    
    for _, row in data_df.iterrows():
        email = str(row[col_map['email']]).strip() if 'email' in col_map else None
        phone = str(row[col_map['phone']]).strip() if 'phone' in col_map else None
        
        name = None
        if 'name' in col_map:
            name = str(row[col_map['name']]).strip()
        elif 'first_name' in col_map:
             f = str(row[col_map['first_name']]).strip()
             l = str(row[col_map['last_name']]).strip() if 'last_name' in col_map else ""
             name = f"{f} {l}".strip()

        # Clean "nan"
        if name and name.lower() in ['nan', 'none', '']: name = None
        if email and email.lower() in ['nan', 'none', '']: email = None
        if phone and phone.lower() in ['nan', 'none', '']: phone = None

        if not (name or email or phone): continue

        anchor = email if email else (name if name else phone)
        sov_id = f"RAW-{uuid.uuid5(uuid.NAMESPACE_DNS, str(anchor).lower()).hex[:8].upper()}"
        
        status = 'Unsubscribed' if is_unsub_tab else 'Discovery'
        if 'unsubscribed' in col_map:
            if str(row[col_map['unsubscribed']]).lower() in ['yes', 'true', '1', 'unsubscribed']:
                status = 'Unsubscribed'

        try:
            cursor.execute('''
                INSERT INTO entities (sovereign_id, display_name, entity_type, status)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(sovereign_id) DO UPDATE SET status = excluded.status
            ''', (sov_id, name if name else anchor, 'Individual', status))
            
            meta = [f"Source: {filename} > {sheet_name}", f"Anchor: {anchor}"]
            if email: meta.append(f"Email: {email}")
            if phone: meta.append(f"Phone: {phone}")
            
            cursor.execute('''
                INSERT INTO metadata_logs (action, module, details)
                VALUES (?, ?, ?)
            ''', ('SPREADSHEET_INGEST', 'host_extract_spreadsheets', f"{sov_id} :: {' | '.join(meta)}"))
            count += 1
        except: continue
        
    print(f"  [+] Tab '{sheet_name}': Mined {count} signals.")
    return count

def run_ingest(path):
    print(f"--- 2030 Infinite Spreadsheet Ingest: {path} ---")
    memory = load_memory()
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    for filename in os.listdir(path):
        if filename.startswith('.') or not filename.lower().endswith(('.csv', '.xlsx', '.xls')): continue
        print(f"Scanning: {filename}")
        try:
            file_path = os.path.join(path, filename)
            if filename.endswith('.csv'):
                df = pd.read_csv(file_path, header=None, dtype=str, on_bad_lines='skip')
                process_matrix(df, filename, "Default", cursor, memory)
            else:
                xls = pd.ExcelFile(file_path)
                for sheet in xls.sheet_names:
                    df = pd.read_excel(file_path, sheet_name=sheet, header=None, dtype=str)
                    process_matrix(df, filename, sheet, cursor, memory)
        except Exception as e:
            print(f"  [!] Error {filename}: {e}")

    save_memory(memory)
    conn.commit()
    conn.close()
    
    # CALL THE BRAIN
    self_heal_network()

if __name__ == "__main__":
    run_ingest(input("Enter path to spreadsheets: ").strip())
