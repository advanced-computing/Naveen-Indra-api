# NYC Jobs API Documentation

## Connection
Endpoint: ```[http://127.0.0.1:5000](http://127.0.0.1:5000)```

## Endpoints

### 1. Root Info
- Method: GET
- Path: ```/```
- Parameters: None
- Returns: API metadata and endpoint directory.

### 2. Job Listing
- Method: GET
- Path: ```/jobs```
- Query Parameters: 
    - ```format```: json (default) | csv
    - ```limit```: integer (default: 50)
    - ```offset```: integer (pagination)
    - ```<column_name>```: filter by dataset columns (e.g., Agency, Work Location)
- Example:
http://127.0.0.1:5000/jobs?format=json&Agency=POLICE%20DEPARTMENT&limit=5

### 3. Job Detail
- Method: GET
- Path: ```/jobs/<job_id>```
- Query Parameters: 
    - ```format```: json | csv
- Returns: Single record for the specified Job ID. Returns 404 if not found.
- Example:
http://127.0.0.1:5000/jobs/727294?format=json 