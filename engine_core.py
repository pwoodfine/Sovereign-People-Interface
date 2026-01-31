import sqlite3
import os

DB_NAME = "people.db"

def self_heal_network():
    """
    The Central Resolution Engine.
    Called automatically after every ingestion to merge fragments.
    """
    if not os.path.exists(DB_NAME): return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    print("\n--- 🧠 Self-Healing: Resolving Identity Fragments ---")
    
    # --- PHASE 1: Email-Based Hard Merge ---
    # Finds entities that share the exact same email address
    cursor.execute('''
        SELECT substr(details, instr(details, 'Email: ') + 7, 
               instr(substr(details, instr(details, 'Email: ') + 7), ' |') - 1) as email_val,
               GROUP_CONCAT(DISTINCT substr(details, 1, instr(details, ' ::') - 1)) as ids
        FROM metadata_logs
        WHERE details LIKE '%Email: %'
        GROUP BY email_val
        HAVING COUNT(DISTINCT substr(details, 1, instr(details, ' ::') - 1)) > 1
    ''')
    
    merges = cursor.fetchall()
    count = 0
    for email, id_list in merges:
        ids = id_list.split(',')
        primary_id = ids[0] # The first ID becomes the 'Sovereign'
        
        for dup_id in ids[1:]:
            # Re-point the history/logs to the Primary ID
            cursor.execute("UPDATE metadata_logs SET details = REPLACE(details, ?, ?) WHERE details LIKE ?", 
                           (dup_id, primary_id, f"{dup_id}%"))
            # Remove the Duplicate Entity
            cursor.execute("DELETE FROM entities WHERE sovereign_id = ?", (dup_id,))
            count += 1
            
    if count > 0:
        print(f"  [Healing] Merged {count} fragmented identities via Email match.")
    else:
        print("  [Healing] Network is healthy. No email conflicts found.")

    conn.commit()
    conn.close()
