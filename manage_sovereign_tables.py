import sqlite3
import csv
import os

DB_NAME = "people.db"
MAPPING = {
    "archetypes": "Dynamic Pulse - Archetypes.csv",
    "chart_of_accounts": "Immutable Geography - Chart of Accounts.csv"
}

def export_logic():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    for table, file_name in MAPPING.items():
        try:
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            headers = [d[0] for d in cursor.description]
            with open(file_name, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)
            print(f"Exported: {file_name}")
        except sqlite3.Error as e:
            print(f"Failed to export {table}: {e}")
    conn.close()

def import_logic():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    for table, file_name in MAPPING.items():
        if not os.path.exists(file_name):
            print(f"Skipping {table}: {file_name} not found.")
            continue
        
        try:
            with open(file_name, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                cursor.execute(f"DELETE FROM {table}")
                
                cols = reader.fieldnames
                placeholders = ", ".join(["?"] * len(cols))
                sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
                
                for row in reader:
                    cursor.execute(sql, list(row.values()))
            print(f"Imported: {table} from {file_name}")
        except (sqlite3.Error, csv.Error) as e:
            print(f"Failed to import {table}: {e}")
            
    conn.commit()
    conn.close()

if __name__ == "__main__":
    choice = input("Enter 'E' to Export to CSV or 'I' to Import from CSV: ").strip().upper()
    if choice == 'E':
        export_logic()
    elif choice == 'I':
        import_logic()
