import json
import os
import pandas as pd
from flask import Flask, jsonify, request

app = Flask(__name__)

# data initialization
_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "employees.csv")

def load_data():
    if not os.path.exists(_csv_path):
        return pd.DataFrame()
    df = pd.read_csv(_csv_path).fillna("")
    if "Job ID" in df.columns:
        df["Job ID"] = df["Job ID"].astype(str)
    return df

jobs_df = load_data()

def apply_filters(df, query_params):
    reserved = ["limit", "offset", "format"]
    for col, val in query_params.items():
        if col in reserved:
            continue
        if col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                try:
                    num_val = float(val) if "." in val else int(val)
                    df = df[df[col] == num_val]
                except ValueError:
                    df = df[df[col].astype(str) == str(val)]
            else:
                df = df[df[col].astype(str) == str(val)]
    return df

def get_formatted_output(df, fmt):
    if fmt == "csv":
        return df.to_csv(index=False), 200, {"Content-Type": "text/csv"}
    
    return df.to_json(orient="records")

### Routes

@app.route("/")
def index():
    return jsonify({
        "message": "NYC Jobs API - Lab 4",
        "documentation": "https://github.com/advanced-computing/Naveen-Indra-api",
        "endpoints": {
            "GET /jobs": "List jobs. Supports filtering by any column, limit, offset, and format (json|csv)",
            "GET /jobs/<id>": "Retrieve a single job by Job ID. Supports format (json|csv)"
        }
    })

@app.get("/jobs")
def list_jobs():
    
    # 1. parse params
    limit = request.args.get("limit", default=50, type=int)
    offset = request.args.get("offset", default=0, type=int)
    fmt = request.args.get("format", default="json").lower()

    # 2. filter
    filtered_df = apply_filters(jobs_df, request.args)
    total_matches = len(filtered_df)

    # 3. paginate
    page_df = filtered_df.iloc[offset : offset + limit]

    # 4. respond with json
    if fmt == "csv":
        return get_formatted_output(page_df, "csv")

    return jsonify({
        "metadata": {
            "total": total_matches,
            "limit": limit,
            "offset": offset,
            "count": len(page_df)
        },
        "jobs": json.loads(get_formatted_output(page_df, "json"))
    })

@app.get("/jobs/<job_id>")
def get_job_by_id(job_id):
    fmt = request.args.get("format", default="json").lower()
    
    # search for exactly one record by ID
    match = jobs_df[jobs_df["Job ID"] == str(job_id)]
    
    if match.empty:
        return jsonify({"error": f"Job ID '{job_id}' not found"}), 404

    if fmt == "csv":
        return get_formatted_output(match, "csv")

    # return as json
    record_json = json.loads(get_formatted_output(match, "json"))
    return jsonify(record_json[0])

if __name__ == "__main__":
    app.run(debug=True, port=5000)

