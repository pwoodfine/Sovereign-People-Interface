import sqlite3

def initialize_coa_logic():
    db_name = "people.db"
    
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Clear existing logic to ensure a clean 'Locked' state
        cursor.execute("DELETE FROM archetypes")
        cursor.execute("DELETE FROM chart_of_accounts")

        # DATA: 11 Archetypes (The Dynamic Pulse)
        archetypes = [
            (1, 'The Executive', 'Strategic Direction', 'Stagnation'), 
            (2, 'The Guardian', 'Risk & Compliance', 'Breach'),
            (3, 'The Fiduciary', 'Resource Integrity', 'Leakage'), 
            (4, 'The Architect', 'System Design', 'Complexity'),
            (5, 'The Engineer', 'Technical Execution', 'Friction'), 
            (6, 'The Artisan', 'Creative Precision', 'Homogeneity'),
            (7, 'The Constructor', 'Physical Realization', 'Structural Gap'), 
            (8, 'The Catalyst', 'Growth & Momentum', 'Inertia'),
            (9, 'The Envoy', 'External Synergy', 'Friction'), 
            (10, 'The Steward', 'Asset Preservation', 'Degradation'),
            (11, 'The Sage', 'Knowledge & Vision', 'Ignorance')
        ]
        cursor.executemany("INSERT INTO archetypes VALUES (?,?,?,?)", archetypes)

        # DATA: 55 Sub-Domains (The Immutable Geography)
        coa = [
            ('Compliance', 'Counsel', 'Legal Representation'), ('Compliance', 'Accounting', 'Fiscal Oversight'),
            ('Compliance', 'Auditor', 'Independent Review'), ('Compliance', 'Bank', 'Institutional Custodian'),
            ('Real Estate', 'Leasing', 'Office'), ('Real Estate', 'Leasing', 'Industrial'), ('Real Estate', 'Leasing', 'Retail'),
            ('Real Estate', 'Tenants', 'Office'), ('Real Estate', 'Tenants', 'Industrial'), ('Real Estate', 'Tenants', 'Retail'),
            ('Real Estate', 'Municipalities', 'Local Authority'),
            ('Construction', 'Collaborators', 'Control Architect'), ('Construction', 'Collaborators', 'Building Design'),
            ('Construction', 'Collaborators', 'Space Planners'), ('Construction', 'Collaborators', 'Façade Consultants'),
            ('Construction', 'Collaborators', 'Landscape Architects'), ('Construction', 'Collaborators', 'Traffic Consultants'),
            ('Construction', 'Collaborators', 'Digital Systems'), ('Construction', 'Collaborators', 'Local Architect'),
            ('Construction', 'Collaborators', 'Structural Engineers'), ('Construction', 'Collaborators', 'Wayfinding'),
            ('IT Support', 'Contributors', 'Software Architect'), ('IT Support', 'Contributors', 'DevOps'),
            ('IT Support', 'Contributors', 'Networking'), ('IT Support', 'Contributors', 'Quality Assurance'),
            ('IT Support', 'Contributors', 'Database'), ('IT Support', 'Contributors', 'Software Integration'),
            ('IT Support', 'Contributors', 'Security'), ('IT Support', 'Contributors', 'Front-End (UI/UX)'),
            ('IT Support', 'Contributors', 'Data Science'), ('IT Support', 'Contributors', 'BIM'),
            ('IT Support', 'Contributors', 'Mobile'), ('IT Support', 'Contributors', 'IoT'),
            ('IT Support', 'Contributors', 'Back-End'), ('IT Support', 'Contributors', 'GIS'),
            ('Investor Relations', 'Finance', 'Portfolio Managers'), ('Investor Relations', 'Finance', 'Investment Bankers'),
            ('Investor Relations', 'Contributors', 'Academics'), ('Investor Relations', 'Contributors', 'Photography'),
            ('Investor Relations', 'Contributors', 'Copy Editing'), ('Investor Relations', 'Contributors', 'Merchandise'),
            ('Investor Relations', 'Contributors', 'Print Vendors'), ('Investor Relations', 'Contributors', 'Creative Design'),
            ('Investor Relations', 'Contributors', 'Visualizers'), ('Investor Relations', 'Media', 'Communication'),
            ('Investor Relations', 'Media', 'Publicity'), ('Investor Relations', 'Politicians', 'Strategic Relations'),
            ('Personnel', 'Governance', 'Supervisory Board'), ('Personnel', 'Governance', 'Management Board'),
            ('Personnel', 'Committees', 'Partnership Oversight'), ('Personnel', 'Operations', 'General Staff'),
            ('Personnel', 'Operations', 'Secretary Pool'),
            ('Local Administration', 'Professional Services', 'Local Accountant'),
            ('Local Administration', 'Professional Services', 'Local Counsel'),
            ('Local Administration', 'Financial Services', 'Local Bank')
        ]
        cursor.executemany("INSERT INTO chart_of_accounts (profile, domain, sub_domain) VALUES (?,?,?)", coa)

        conn.commit()
    except sqlite3.Error as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    initialize_coa_logic()
