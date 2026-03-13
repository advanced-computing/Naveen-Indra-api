import json
import os
import duckdb
import pandas as pd
from flask import Flask, jsonify, request

app = Flask(__name__)

# data initialization
DB_PATH = "file.db"

def get_con():
    return duckdb.connect(DB_PATH)

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
        "message": "NYC Jobs API - Lab 7",
        "documentation": "https://github.com/advanced-computing/Naveen-Indra-api",
        "endpoints": {
            "GET /jobs": "List jobs. Supports filtering by any column, limit, offset, and format (json|csv)",
            "GET /jobs/<id>": "Retrieve a single job by Job ID. Supports format (json|csv)",
            "POST /users": "Add a new user {username, age, country}",
            "GET /users/summary": "Number of users, average age, top-3 countries",
        }
    })

@app.get("/jobs")
def list_jobs():
    
    # 1. parse params
    limit = request.args.get("limit", default=50, type=int)
    offset = request.args.get("offset", default=0, type=int)
    fmt = request.args.get("format", default="json").lower()

    # 2. load from DuckDB and filter
    con = get_con()
    jobs_df = con.execute("SELECT * FROM employees").fetchdf()
    con.close()

    filtered_df = apply_filters(jobs_df, request.args)
    total_matches = len(filtered_df)

    # 3. paginate
    page_df = filtered_df.iloc[offset : offset + limit]

    # 4. respond
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

    con = get_con()
    jobs_df = con.execute("SELECT * FROM employees").fetchdf()
    con.close()

    if "Job ID" in jobs_df.columns:
        jobs_df["Job ID"] = jobs_df["Job ID"].astype(str)

    # search for exactly one record by ID
    match = jobs_df[jobs_df["Job ID"] == str(job_id)]
    
    if match.empty:
        return jsonify({"error": f"Job ID '{job_id}' not found"}), 404

    if fmt == "csv":
        return get_formatted_output(match, "csv")

    # return as json
    record_json = json.loads(get_formatted_output(match, "json"))
    return jsonify(record_json[0])

### Users endpoints

@app.post("/users")
def add_user():
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"error": "Request body must be JSON"}), 400

    username = body.get("username", "").strip()
    age      = body.get("age")
    country  = body.get("country", "").strip()

    if not username:
        return jsonify({"error": "username is required"}), 400
    if age is None or not isinstance(age, int) or age < 0:
        return jsonify({"error": "age must be a non-negative integer"}), 400
    if not country:
        return jsonify({"error": "country is required"}), 400

    con = get_con()
    con.execute("INSERT INTO users (username, age, country) VALUES (?, ?, ?)", [username, age, country])
    con.close()

    return jsonify({"message": "User added", "user": {"username": username, "age": age, "country": country}}), 201

@app.get("/users/summary")
def users_summary():
    con = get_con()
    total_users   = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    avg_age       = con.execute("SELECT ROUND(AVG(age), 2) FROM users").fetchone()[0]
    top_countries = con.execute("""
        SELECT country, COUNT(*) AS user_count
        FROM   users
        GROUP  BY country
        ORDER  BY user_count DESC
        LIMIT  3
    """).fetchdf().to_dict(orient="records")
    con.close()

    return jsonify({
        "total_users":     total_users,
        "average_age":     float(avg_age) if avg_age is not None else None,
        "top_3_countries": top_countries,
    })

if __name__ == "__main__":
    app.run(debug=True, port=5000)