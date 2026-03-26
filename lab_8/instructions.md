# Lab 8 

## Usage Instructions
1. Make sure that `PCPI24M1.csv` and `PCPI25M2.csv` are  in the same directory as the script.

2. Run  data loading script:
   ```bash
   python3 data_load.py
   ```
3. A DukDB database file named `cpi.db` will be created automatically.

## Manual Testing Instructions
To verify the functionality of the three different table loading, try:

1. Opening DuckDB interactive prompt:
   ```bash
   duckdb cpi.db
   ```
2. Query the data in the three tables (`cpi_append`, `cpi_trunc`, `cpi_inc`) to observe how they've handled the incoming updates:
   ```sql
   SELECT COUNT(*) FROM cpi_append;
   SELECT COUNT(*) FROM cpi_trunc;
   SELECT COUNT(*) FROM cpi_inc;


## Expected Results

The CPI database from the January 2024 has 924 rows while the newer February 2025 vintage has 937 rows). Resulting operations alter each table significantly:

* **Row Counts**: 
  * `cpi_append` grows to 1,861 rows (924 + 937)
  * Both `cpi_trunc` and `cpi_inc` correctly max out at 937 rows (representing the accurate, most-recent dataset).

* **Explanation**:
  * **Append** blindly copies all new data below the old  data. 
  * **Truncate** perfectly guarantees accuracy because it  wipes out the old table and re-installs the newest incoming CSV cleanly. 
  * **Incremental** merges the tables securely by locating matching 'DATE' values and overriding older numbers with the brand-new values from 2025, leaving unchanged history completely intact. 
