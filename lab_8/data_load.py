import duckdb

def main():
    con = duckdb.connect('cpi.db')
    
    # 1. Initialize Tables from PCPI24M1.csv
    print("Initializing Database with PCPI24M1.csv...")
    
    # Append Table
    con.execute("DROP TABLE IF EXISTS cpi_append")
    con.execute("CREATE TABLE cpi_append AS SELECT * FROM read_csv_auto('PCPI24M1.csv')")
    
    # Truncate Table
    con.execute("DROP TABLE IF EXISTS cpi_trunc")
    con.execute("CREATE TABLE cpi_trunc AS SELECT * FROM read_csv_auto('PCPI24M1.csv')")
    
    # Incremental Table
    # Note: We must specify DATE as PRIMARY KEY for ON CONFLICT to work
    con.execute("DROP TABLE IF EXISTS cpi_inc")
    con.execute("CREATE TABLE cpi_inc (DATE VARCHAR PRIMARY KEY, CPI DOUBLE)")
    con.execute("INSERT INTO cpi_inc SELECT * FROM read_csv_auto('PCPI24M1.csv')")
    
    print("Initial Data Load Complete. Displaying row counts:")
    print("  cpi_append:", con.execute("SELECT COUNT(*) FROM cpi_append").fetchone()[0])
    print("  cpi_trunc:", con.execute("SELECT COUNT(*) FROM cpi_trunc").fetchone()[0])
    print("  cpi_inc:", con.execute("SELECT COUNT(*) FROM cpi_inc").fetchone()[0])
    
    # 2. Update with PCPI25M2.csv
    print("\nUpdating Database with PCPI25M2.csv...")
    
    # Append Method: Just Insert all new records
    con.execute("INSERT INTO cpi_append SELECT * FROM read_csv_auto('PCPI25M2.csv')")
    
    # Truncate Method: Delete existing records and Insert all new records
    con.execute("DELETE FROM cpi_trunc")
    con.execute("INSERT INTO cpi_trunc SELECT * FROM read_csv_auto('PCPI25M2.csv')")
    
    # Incremental Method: Upsert using ON CONFLICT logic
    con.execute("""
        INSERT INTO cpi_inc 
        SELECT * FROM read_csv_auto('PCPI25M2.csv')
        ON CONFLICT (DATE) DO UPDATE SET CPI = EXCLUDED.CPI
    """)
    
    print("Data Update Complete. Displaying final row counts:")
    print("  cpi_append:", con.execute("SELECT COUNT(*) FROM cpi_append").fetchone()[0])
    print("  cpi_trunc:", con.execute("SELECT COUNT(*) FROM cpi_trunc").fetchone()[0])
    print("  cpi_inc:", con.execute("SELECT COUNT(*) FROM cpi_inc").fetchone()[0])

    print("\nSample queries to show differences:")
    append_dupes = con.execute("SELECT DATE, COUNT(*) FROM cpi_append GROUP BY DATE HAVING COUNT(*) > 1").fetchall()
    print("  Duplicate entries in cpi_append:", len(append_dupes))
    
    print("\nDatabase processing complete.")

if __name__ == "__main__":
    main()
