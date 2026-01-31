import sqlite3

def architect_substrate():
    db_name = "people.db"
    
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # 1. ENTITIES: The primary records for talent and organizations
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sovereign_id TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                entity_type TEXT DEFAULT 'Individual',
                status TEXT DEFAULT 'Discovery'
            )
        ''')

        # 2. AFFILIATIONS: The 'Elastic Bonds' connecting entities to the logic
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS affiliations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id TEXT NOT NULL,
                target_node TEXT NOT NULL,
                bond_type TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                FOREIGN KEY (entity_id) REFERENCES entities (sovereign_id)
            )
        ''')

        # 3. METADATA LOGS: Tracking provenance and system changes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                action TEXT NOT NULL,
                module TEXT NOT NULL,
                details TEXT
            )
        ''')

        # 4. STRUCTURAL PLACEHOLDERS: Tables for Archetypes and Chart of Accounts
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS archetypes (
                id INTEGER PRIMARY KEY, 
                name TEXT, 
                signature TEXT, 
                healing_trigger TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chart_of_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                profile TEXT, 
                domain TEXT, 
                sub_domain TEXT
            )
        ''')

        conn.commit()
    except sqlite3.Error as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    architect_substrate()
