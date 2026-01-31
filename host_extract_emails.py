import sqlite3
import pandas as pd
import uuid
import os
import json
import re
import warnings
import email
import email.header
from engine_core import self_heal_network

# Silence technical noise
warnings.filterwarnings('ignore')

DB_NAME = "people.db"
MEMORY_FILE = "signal_memory.json"

PATTERNS = {'email': re.compile(r'[^@]+@[^@]+\.[^@]+')}

# The Proximity Anchors for Gravity Detection
SIGNAL_MAP = {
    'email': ['email', 'address', 'sender', 'recipient', 'from', 'to'],
    'name': ['name', 'display', 'contact', 'person'],
    'subject': ['subject', 'topic', 'title', 're:'],
    'date': ['date', 'sent', 'received', 'timestamp']
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
    """Calculates 'Identity Mass' to locate the header row in CSV logs."""
    score = 0
    cells = [str(c).lower().strip() for c in row.dropna()]
    
    # Check for unsplit 'Delimiter Ghost' strings
    if len(cells) == 1 and ',' in cells[0]:
        text = cells[0]
        for category, keywords in SIGNAL_MAP.items():
            if any(k in text for k in keywords):
                score += 3 if category in ['email', 'name'] else 1
    else:
        for cell in cells:
            for category, keywords in SIGNAL_MAP.items():
                if any(k in cell for k in keywords):
                    score += 3 if category in ['email', 'name'] else 1
                    break 
    return score

def ingest_signal(email_addr, name, context, source, cursor):
    """Universal Sovereign Anchor logic."""
    if not email_addr or str(email_addr).lower() in ['nan', 'none', '']: return False
    email_clean = email_addr.lower().strip()
    if not PATTERNS['email'].match(email_clean): return False

    sov_id = f"RAW-{uuid.uuid5(uuid.NAMESPACE_DNS, email_clean).hex[:8].upper()}"
    display = name if name and str(name).lower() not in ['nan', 'none', ''] else email_clean

    try:
        cursor.execute('''
            INSERT INTO entities (sovereign_id, display_name, entity_type, status)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(sovereign_id) DO UPDATE SET status = status
        ''', (sov_id, display, 'Individual', 'Discovery'))
        
        meta = [f"Source: {source}", f"Anchor: Email"]
        if context: 
            clean_subject = str(context)[:50].replace('\n', ' ')
            meta.append(f"Context: {clean_subject}")
        
        cursor.execute('''
            INSERT INTO metadata_logs (action, module, details)
            VALUES (?, ?, ?)
        ''', ('EMAIL_INGEST', 'host_extract_emails', f"{sov_id} :: {' | '.join(meta)}"))
        return True
    except: return False

def process_email_csv(file_path, filename, cursor, memory):
    """Dynamic Gravity Ingest for CSV-based email lists."""
    try:
        df_raw = pd.read_csv(file_path, header=None, sep=None, engine='python', dtype=str, on_bad_lines='skip')
    except: return 0

    best_idx, max_mass = 0, 0
    for idx, row in df_raw.head(100).iterrows():
        mass = calculate_identity_mass(row)
        if mass > max_mass:
            max_mass, best_idx = mass, idx
            
    if max_mass < 4: return 0

    print(f"  -> Gravity Well Locked at Row {best_idx} (Mass: {max_mass})")
    
    df = df_raw.iloc[best_idx + 1:].copy()
    df.columns = [str(c).strip() for c in df_raw.iloc[best_idx]]
    
    # Self-Healing Map
    col_map = {}
    norm_cols = {str(c).lower().strip(): c for c in df.columns}
    for category, keywords in SIGNAL_MAP.items():
        for k in keywords:
            match = next((orig for clean, orig in norm_cols.items() if k in clean), None)
            if match:
                col_map[category] = match
                break
    
    count = 0
    for _, row in df.iterrows():
        email_val = str(row.get(col_map.get('email'), "")).strip()
        name_val = str(row.get(col_map.get('name'), "")).strip()
        if ingest_signal(email_val, name_val, row.get(col_map.get('subject'), ''), filename, cursor):
            count += 1
    return count

def process_raw_directory(path, cursor):
    """Deep Scan for Maildir or loose email files."""
    print(f"  -> Initiating Deep Scan of {path}...")
    total_count = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            if f.startswith('.'): continue
            file_path = os.path.join(root, f)
            try:
                with open(file_path, 'r', errors='ignore') as fh:
                    content = fh.read()
                    if "From:" in content or "Subject:" in content:
                        msg = email.message_from_string(content)
                        sender = str(msg.get('From', ''))
                        s_name, s_email = None, None
                        if '<' in sender:
                            s_name = sender.split('<')[0].strip().strip('"')
                            s_email = sender.split('<')[1].split('>')[0].strip()
                        elif '@' in sender: s_email = sender.strip()

                        if s_email and ingest_signal(s_email, s_name, msg.get('Subject', ''), f, cursor):
                            total_count += 1
            except: continue
    return total_count

def run_ingest(path):
    print(f"--- 2030 Infinite Email Ingest: {path} ---")
    memory = load_memory()
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    if os.path.isdir(path):
        # 1. Check for CSVs in directory
        csv_count = 0
        for f in os.listdir(path):
            if f.lower().endswith('.csv'):
                print(f"Scanning CSV: {f}")
                csv_count += process_email_csv(os.path.join(path, f), f, cursor, memory)
        
        # 2. If no CSVs found, perform Deep Scan (Maildir/Files)
        if csv_count == 0:
            process_raw_directory(path, cursor)
    
    save_memory(memory)
    conn.commit()
    conn.close()
    self_heal_network()

if __name__ == "__main__":
    run_ingest(input("Enter path to Email Folder: ").strip())
