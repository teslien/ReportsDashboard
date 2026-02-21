from flask import Flask, render_template, request, jsonify, make_response, has_request_context, redirect, url_for
from werkzeug.local import LocalProxy
import requests
import base64
import sqlite3
import os
from datetime import datetime, timedelta, timezone
import re
from urllib.parse import unquote

app = Flask(__name__)

# =========================
# 🔴 CONFIG (DYNAMIC)
# =========================
JIRA_DOMAIN = "https://lumberfi.atlassian.net"

# Defaults
DEFAULT_JIRA_EMAIL = ""
DEFAULT_JIRA_API_TOKEN = ""
DEFAULT_PROJECT_KEY = ""

def _decode_value(value):
    if not value:
        return ""
    try:
        value = unquote(value).strip()
        # Remove surrounding quotes if present (some browsers/servers add them)
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        return value.strip()
    except Exception:
        return str(value).strip()

def _get_jira_config():
    """Fetch Jira config from DB. Returns (email, token, project_key)."""
    try:
        conn = sqlite3.connect("tracker.db")
        cursor = conn.cursor()
        cursor.execute("SELECT email, api_token, project_key FROM jira_config WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        if row:
            return row[0] or "", row[1] or "", row[2] or ""
    except Exception:
        pass
    return "", "", ""

def _get_project_key():
    _, _, db_project = _get_jira_config()
    project = db_project or DEFAULT_PROJECT_KEY
    if has_request_context():
        req_project = request.headers.get("X-Project-Key") or request.args.get("project_key") or request.cookies.get("project_key")
        if req_project:
            project = _decode_value(req_project)
    print(f"DEBUG: _get_project_key decoded: {repr(project)}")
    return project.upper() if project else ""

def _get_jira_headers():
    db_email, db_token, _ = _get_jira_config()
    email = db_email or DEFAULT_JIRA_EMAIL
    token = db_token or DEFAULT_JIRA_API_TOKEN
    
    if has_request_context():
        req_email = request.headers.get("X-Jira-Email") or request.args.get("jira_email") or request.cookies.get("jira_email")
        req_token = request.headers.get("X-Jira-Token") or request.args.get("jira_token") or request.cookies.get("jira_token")
        if req_email: email = _decode_value(req_email)
        if req_token: token = _decode_value(req_token)
        
    if not email or not token:
        # Return empty auth if credentials are missing
        return {"Content-Type": "application/json"}
        
    auth_str = f"{email}:{token}"
    auth_b64 = base64.b64encode(auth_str.encode()).decode()
    
    return {
        "Authorization": f"Basic {auth_b64}",
        "Content-Type": "application/json"
    }

# Proxies to allow dynamic access per request while keeping existing code working
PROJECT_KEY = LocalProxy(_get_project_key)
HEADERS = LocalProxy(_get_jira_headers)

# Legacy auth (deprecated but kept for compatibility if used directly)
AUTH = base64.b64encode(f"{DEFAULT_JIRA_EMAIL}:{DEFAULT_JIRA_API_TOKEN}".encode()).decode()





# =========================
# DATABASE SETUP
# =========================
DB_PATH = "tracker.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Trackers Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trackers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Tickets Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracker_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracker_id INTEGER,
            issue_key TEXT NOT NULL,
            comment TEXT DEFAULT '',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (tracker_id) REFERENCES trackers(id) ON DELETE CASCADE
        )
    ''')
    # Todos Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS todos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            priority TEXT DEFAULT 'Low',
            due_date DATE NOT NULL,
            status TEXT DEFAULT 'Pending',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Teams Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Team Members Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER,
            account_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            avatar_url TEXT,
            FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE
        )
    ''')
    # Sprints Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sprints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER,
            name TEXT NOT NULL,
            state TEXT DEFAULT 'active',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE
        )
    ''')
    # Sprint Weeks Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sprint_weeks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sprint_id INTEGER,
            week_number INTEGER NOT NULL,
            goal TEXT,
            FOREIGN KEY (sprint_id) REFERENCES sprints(id) ON DELETE CASCADE
        )
    ''')
    # Sprint Tickets Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sprint_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sprint_id INTEGER,
            week_id INTEGER,
            issue_key TEXT NOT NULL,
            comment TEXT DEFAULT '',
            pr_raised INTEGER DEFAULT 0,
            demo_done INTEGER DEFAULT 0,
            pr_merged INTEGER DEFAULT 0,
            FOREIGN KEY (sprint_id) REFERENCES sprints(id) ON DELETE CASCADE,
            FOREIGN KEY (week_id) REFERENCES sprint_weeks(id) ON DELETE CASCADE
        )
    ''')

    # Scrum Notes Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scrum_notes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        DATE    NOT NULL,
            team_id     INTEGER NOT NULL,
            member_id   TEXT    NOT NULL,
            member_name TEXT    NOT NULL,
            ticket_key  TEXT    NOT NULL,
            comment     TEXT    DEFAULT \'\',
            deadline    DATE,
            status      TEXT    DEFAULT \'Pending\',
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE
        )
    ''')

    # Jira Config Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jira_config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            email TEXT,
            api_token TEXT,
            project_key TEXT
        )
    ''')
    
    # Migration: Add new columns if they don't exist
    cursor.execute("PRAGMA table_info(sprint_tickets)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'comment' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN comment TEXT DEFAULT ''")
    if 'pr_raised' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN pr_raised INTEGER DEFAULT 0")
    if 'demo_done' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN demo_done INTEGER DEFAULT 0")

    conn.commit()
    conn.close()

init_db()

# =========================
# PAGE ROUTE
# =========================
@app.route("/settings")
def settings():
    return render_template("settings.html", project=PROJECT_KEY)

@app.route("/api/settings/jira", methods=["GET", "POST"])
def jira_settings_api():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if request.method == "POST":
        data = request.json
        email = data.get("email", "").strip()
        token = data.get("token", "").strip()
        project = data.get("project_key", "").strip().upper()
        
        if not email or not token or not project:
            return jsonify({"error": "All fields are required"}), 400
            
        cursor.execute("""
            INSERT INTO jira_config (id, email, api_token, project_key)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                email=excluded.email,
                api_token=excluded.api_token,
                project_key=excluded.project_key
        """, (email, token, project))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
        
    else:  # GET
        cursor.execute("SELECT email, api_token, project_key FROM jira_config WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        if row:
            return jsonify({
                "email": row[0],
                "token": row[1],
                "project_key": row[2]
            })
        return jsonify({"email": "", "token": "", "project_key": ""})

@app.route("/")
def index():
    email, token, _ = _get_jira_config()
    if not email or not token:
        return redirect(url_for('settings'))
    return render_template("index.html", project=PROJECT_KEY)

# =========================
# FETCH ASSIGNEES
# =========================
@app.route("/api/assignees", methods=["GET", "POST"])
def assignees():
    # If Select2 sends a search query
    data = request.get_json(silent=True) or {}
    query = request.args.get('q') or data.get('q')

    if not PROJECT_KEY:
        return jsonify({"error": "Missing project key. Save it in Settings first."}), 400
    if "Authorization" not in HEADERS:
        return jsonify({"error": "Missing Jira credentials. Save them in Settings first."}), 401
    
    # Use assignable search to get users who can actually be assigned to the project
    params = {
        "project": PROJECT_KEY,
        "maxResults": 100
    }
    if query:
        params["query"] = query

    try:
        # Explicitly cast LocalProxy objects to ensure they are passed correctly
        headers_dict = dict(HEADERS)
        project_key_str = str(PROJECT_KEY)
        
        print(f"DEBUG: calling Jira assignable/search with project={project_key_str}")
        
        jira_res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/user/assignable/search",
            headers=headers_dict,
            params={
                "project": project_key_str,
                "maxResults": 100,
                "query": query
            } if query else {
                "project": project_key_str,
                "maxResults": 100
            }
        )
        
        if jira_res.status_code != 200:
            print(f"DEBUG: Jira Error {jira_res.status_code}: {jira_res.text}")
            return jsonify({"error": f"Jira API error: {jira_res.text}"}), jira_res.status_code
            
        users = jira_res.json()
        if not isinstance(users, list):
            # Sometimes Jira returns an error object instead of a list if something is wrong
            return jsonify(users), jira_res.status_code

        # Format for Select2
        results = [{"id": u.get("accountId"), "name": u.get("displayName")} for u in users if u.get("accountType") == "atlassian"]
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =========================
# FETCH CUSTOMERS
# =========================
@app.route("/api/customers", methods=["GET"])
def customers():
    query = request.args.get('q') or request.args.get('term')
    unique_customers = set()
    
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)
    
    # If a query is provided, search specifically for matching customers
    if query:
        jql = f'"Customer" ~ "{query}*"'
        # We only need to scan a few issues to find the variations
        scan_range = [0, 100]
    else:
        jql = 'Customer is not EMPTY'
        # Broaden scan to find more unique clients - huge scan (1500 issues)
        scan_range = range(0, 1500, 100)
    
    for start_at in scan_range:
        jira_res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params={
                "jql": jql,
                "maxResults": 100,
                "startAt": start_at,
                "fields": "customfield_10077"
            }
        )
        
        if jira_res.status_code != 200:
            break
            
        res = jira_res.json()
        issues = res.get("issues", [])
        if not issues:
            break
            
        for i in issues:
            cust_val = i["fields"].get("customfield_10077")
            if isinstance(cust_val, list):
                for c in cust_val:
                    if isinstance(c, dict):
                        val = c.get("value")
                        if val: unique_customers.add(val)
                    elif c:
                        unique_customers.add(c)
            elif isinstance(cust_val, dict):
                val = cust_val.get("value")
                if val: unique_customers.add(val)
            elif cust_val:
                unique_customers.add(cust_val)
                
    # Filter results by query locally as well for double safety
    results = []
    for c in sorted(list(unique_customers)):
        if not query or query.lower() in c.lower():
            results.append({"id": c, "name": c})
            
    return jsonify(results)

# =========================
# JQL SEARCH PROXY
# =========================
@app.route("/api/search", methods=["POST"])
def search():
    # Attempt to handle both JSON and Form data to avoid 415
    data = request.get_json(silent=True) or request.form.to_dict()
    
    # Cast headers
    headers_dict = dict(HEADERS)
    
    jira_res = requests.post(
        f"{JIRA_DOMAIN}/rest/api/3/search/jql",
        headers=headers_dict,
        json=data
    )
    return jsonify(jira_res.json())

# =========================
# SCOREBOARD ROUTE
# =========================
@app.route("/scoreboard")
def scoreboard():
    return render_template("scoreboard.html", project=PROJECT_KEY)

# =========================
# FETCH SCOREBOARD DATA
# =========================
@app.route("/api/scoreboard_data", methods=["POST"])
def scoreboard_data():
    print("DEBUG: Entered scoreboard_data")
    data = request.json
    range_type = data.get("range", "day")
    
    # Explicitly cast LocalProxy objects
    project_key_str = str(PROJECT_KEY)
    headers_dict = dict(HEADERS)
    
    # Use quotes for safety
    jql = f'project = "{project_key_str}" AND statusCategory = "Done"'
    
    if range_type == "day":
        jql += " AND resolutiondate >= startOfDay()"
    elif range_type == "yesterday":
        jql += " AND resolutiondate >= startOfDay(-1d) AND resolutiondate < startOfDay()"
    elif range_type == "week":
        jql += " AND resolutiondate >= startOfWeek()"
    elif range_type == "month":
        jql += " AND resolutiondate >= startOfMonth()"
    elif range_type == "year":
        jql += " AND resolutiondate >= startOfYear()"
        
    # If custom date is passed
    if "startDate" in data and data["startDate"]:
        jql += f" AND resolutiondate >= '{data['startDate']}'"
    if "endDate" in data and data["endDate"]:
        jql += f" AND resolutiondate <= '{data['endDate']}'"

    # Fetch ALL completed issues for the period
    all_issues = []
    start_at = 0
    max_results = 100
    
    print(f"DEBUG: Executing JQL: {jql}")
    
    # payload setup
    payload = {
        "jql": jql,
        "maxResults": int(max_results),
        "startAt": int(start_at),
        "fields": ["assignee", "status", "resolutiondate"]
    }
    
    while True:
        # Using GET /rest/api/3/search/jql as required by the error message.
        # GET requests pass parameters in the URL query string.
        params = {
            "jql": jql,
            "maxResults": max_results,
            "startAt": start_at,
            "fields": "assignee,status,resolutiondate" # Comma separated for GET
        }
        
        res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params=params
        ).json()
        
        # print(f"DEBUG: Jira Response Keys: {res.keys()}")
        if "errorMessages" in res:
            print(f"DEBUG: Error from Jira: {res['errorMessages']}")
        
        issues = res.get("issues", [])
        print(f"DEBUG: Found {len(issues)} issues (Total: {res.get('total', 0)})")
        all_issues.extend(issues)
        
        if start_at + len(issues) >= res.get("total", 0):
            break
        start_at += len(issues)

    # Aggregate by assignee
    stats = {}
    for i in all_issues:
        assignee = i["fields"].get("assignee")
        if assignee:
            aid = assignee["accountId"]
            name = assignee["displayName"]
            if aid not in stats:
                stats[aid] = {"id": aid, "name": name, "count": 0, "avatar": assignee["avatarUrls"]["48x48"]}
            stats[aid]["count"] += 1
            
    return jsonify(list(stats.values()))

# =========================
# VELOCITY DATA (TRENDS)
# =========================
@app.route("/api/velocity_data", methods=["POST"])
def velocity_data():
    data = request.json
    range_type = data.get("range", "day")
    
    project_key_str = str(PROJECT_KEY)
    headers_dict = dict(HEADERS)
    
    jql = f'project = "{project_key_str}" AND statusCategory = "Done"'
    
    if range_type == "day":
        jql += " AND resolutiondate >= startOfDay()"
    elif range_type == "yesterday":
        jql += " AND resolutiondate >= startOfDay(-1d) AND resolutiondate < startOfDay()"
    elif range_type == "week":
        jql += " AND resolutiondate >= startOfWeek()"
    elif range_type == "month":
        jql += " AND resolutiondate >= startOfMonth()"
    elif range_type == "year":
        jql += " AND resolutiondate >= startOfYear()"

    # Fetch simple fields
    fields = "resolutiondate"
    
    # We need ALL issues to count properly
    all_issues = []
    start_at = 0
    max_results = 100
    
    while True:
        res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params={
                "jql": jql,
                "maxResults": max_results,
                "startAt": start_at,
                "fields": fields
            }
        ).json()
        
        issues = res.get("issues", [])
        all_issues.extend(issues)
        
        if start_at + len(issues) >= res.get("total", 0):
            break
        start_at += len(issues)

    # Aggregate by Date
    # For Year -> by Month
    # For others -> by Day
    from datetime import datetime
    
    timeline = {}
    
    for i in all_issues:
        date_str = i["fields"]["resolutiondate"]
        # Parse ISO date 2023-10-25T12:00:00.000+0000
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        
        if range_type == "year":
            key = dt.strftime("%Y-%m") # Group by month
        else:
            key = dt.strftime("%Y-%m-%d") # Group by day

        timeline[key] = timeline.get(key, 0) + 1
        
    # Sort by date
    sorted_timeline = [{"date": k, "count": v} for k, v in sorted(timeline.items())]
    
    return jsonify(sorted_timeline)

# =========================
# USER DETAILS ROUTE
# =========================
@app.route("/user/<account_id>")
def user_details(account_id):
    return render_template("user_tickets.html", project=PROJECT_KEY, account_id=account_id)

# =========================
# FETCH USER TICKETS
# =========================
@app.route("/api/user_tickets", methods=["POST"])
def user_tickets():
    data = request.json
    account_id = data.get("accountId")
    range_type = data.get("range", "month")
    specific_date = data.get("specificDate")
    
    project_key_str = str(PROJECT_KEY)
    headers_dict = dict(HEADERS)
    
    if not account_id:
        return jsonify({"error": "Missing accountId"}), 400

    # Strict JQL construction
    jql = f'project = "{project_key_str}" AND statusCategory = "Done" AND assignee = "{account_id}"'
    
    if range_type == "date" and specific_date:
        from datetime import datetime, timedelta
        date_dt = datetime.strptime(specific_date, "%Y-%m-%d")
        next_day = (date_dt + timedelta(days=1)).strftime("%Y-%m-%d")
        jql += f' AND resolutiondate >= "{specific_date}" AND resolutiondate < "{next_day}"'
    elif range_type == "day":
        jql += " AND resolutiondate >= startOfDay()"
    elif range_type == "yesterday":
        jql += " AND resolutiondate >= startOfDay(-1d) AND resolutiondate < startOfDay()"
    elif range_type == "week":
        jql += " AND resolutiondate >= startOfWeek()"
    elif range_type == "month":
        jql += " AND resolutiondate >= startOfMonth()"
    elif range_type == "year":
        jql += " AND resolutiondate >= startOfYear()"
    
    # We want details
    fields = "summary,status,resolutiondate,priority,issuetype,assignee"
    
    # Fetch issues (using GET /jql as established)
    res = requests.get(
        f"{JIRA_DOMAIN}/rest/api/3/search/jql",
        headers=headers_dict,
        params={
            "jql": jql,
            "maxResults": 100, # Limit to last 100 for now
            "fields": fields
        }
    ).json()
    
    return jsonify(res.get("issues", []))

# =========================
# ASSIGNEEE FULL STATS (NEW)
# =========================
@app.route("/api/suggest_tickets", methods=["POST"])
def suggest_tickets():
    query = request.json.get("query", "")
    if not query:
        return jsonify({"results": []})
    
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)
    project_key_str = str(PROJECT_KEY)

    # Search for tickets matching key or summary
    jql = f'project = "{project_key_str}" AND (key ~ "{query}*" OR summary ~ "{query}*")'
    
    try:
        res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params={
                "jql": jql,
                "maxResults": 10,
                "fields": "summary,key"
            }
        ).json()
        
        issues = res.get("issues", [])
        results = [
            {"id": i["key"], "text": f'{i["key"]}: {i["fields"]["summary"]}'}
            for i in issues
        ]
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/assignee_full_profile", methods=["POST"])
def assignee_full_profile():
    data = request.json
    account_id = data.get("accountId")
    range_type = data.get("range", "month")
    specific_date = data.get("specificDate") # YYYY-MM-DD
    
    if not account_id:
        return jsonify({"error": "Missing accountId"}), 400

    # Explicitly cast LocalProxy objects to ensure they are passed correctly
    headers_dict = dict(HEADERS)
    project_key_str = str(PROJECT_KEY)

    # 1. FETCH DONE ISSUES (In Range)
    jql_done = f'project = "{project_key_str}" AND statusCategory = "Done" AND assignee = "{account_id}"'
    
    if range_type == "date" and specific_date:
        # Fetch for that specific day
        from datetime import datetime, timedelta
        date_dt = datetime.strptime(specific_date, "%Y-%m-%d")
        next_day = (date_dt + timedelta(days=1)).strftime("%Y-%m-%d")
        jql_done += f' AND resolutiondate >= "{specific_date}" AND resolutiondate < "{next_day}"'
    elif range_type == "day":
        jql_done += " AND resolutiondate >= startOfDay()"
    elif range_type == "yesterday":
        jql_done += " AND resolutiondate >= startOfDay(-1d) AND resolutiondate < startOfDay()"
    elif range_type == "week":
        jql_done += " AND resolutiondate >= startOfWeek()"
    elif range_type == "month":
        jql_done += " AND resolutiondate >= startOfMonth()"
    elif range_type == "year":
        jql_done += " AND resolutiondate >= startOfYear()"
        
    res_done = requests.get(
        f"{JIRA_DOMAIN}/rest/api/3/search/jql",
        headers=headers_dict,
        params={
            "jql": jql_done,
            "maxResults": 100,
            "fields": "priority,issuetype"
        }
    ).json()
    done_issues = res_done.get("issues", [])

    # 2. FETCH OPEN ISSUES (Current Load - No date range)
    jql_open = f'project = "{project_key_str}" AND statusCategory != "Done" AND assignee = "{account_id}"'
    
    res_open = requests.get(
        f"{JIRA_DOMAIN}/rest/api/3/search/jql",
        headers=headers_dict,
        params={
            "jql": jql_open,
            "maxResults": 100,
            "fields": "priority,issuetype,status"
        }
    ).json()
    open_issues = res_open.get("issues", [])

    return jsonify({
        "done": done_issues,
        "open": open_issues
    })


# =========================
# CUSTOM REPORTS
# =========================
@app.route("/reports")
def reports_page():
    return render_template("custom_reports.html", project=PROJECT_KEY)

@app.route("/tracker")
def tracker():
    return render_template("tracker.html", project=PROJECT_KEY)

@app.route("/report_view")
def report_view():
    return render_template("report_view.html", project=PROJECT_KEY)

@app.route("/api/execute_jql", methods=["POST"])
def execute_jql():
    data = request.json
    jql = data.get("jql")
    
    if not jql:
        return jsonify({"error": "No JQL provided"}), 400

    print(f"DEBUG: Executing Custom JQL: {jql}")
    
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)

    # Fetch widely used fields. customfield_10016 is commonly Story Points.
    fields = "summary,status,assignee,priority,issuetype,created,updated,resolutiondate,project,reporter,resolution,duedate,customfield_10016"
    
    all_issues = []
    start_at = 0
    max_results = 100
    
    while True:
        res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params={
                "jql": jql,
                "maxResults": max_results,
                "startAt": start_at,
                "fields": fields
            }
        ).json()
        
        if "errorMessages" in res:
             return jsonify({"error": res["errorMessages"]}), 400

        issues = res.get("issues", [])
        all_issues.extend(issues)
        
        if start_at + len(issues) >= res.get("total", 0):
            break
        start_at += len(issues)
    
    print(f"DEBUG: Found {len(all_issues)} issues for tracker/reports")
    return jsonify(all_issues)

# =========================
# MULTI-TRACKER API
# =========================

@app.route("/api/trackers", methods=["GET", "POST"])
def manage_trackers():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if request.method == "POST":
        title = request.json.get("title")
        if not title:
            return jsonify({"error": "Title is required"}), 400
        cursor.execute("INSERT INTO trackers (title) VALUES (?)", (title,))
        conn.commit()
        tracker_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": tracker_id, "title": title})
    
    else:
        cursor.execute("SELECT id, title, created_at FROM trackers ORDER BY created_at DESC")
        trackers = [{"id": r[0], "title": r[1], "created_at": r[2]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(trackers)

@app.route("/api/trackers/<int:tracker_id>", methods=["DELETE"])
def delete_tracker(tracker_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM trackers WHERE id = ?", (tracker_id,))
    cursor.execute("DELETE FROM tracker_tickets WHERE tracker_id = ?", (tracker_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/api/trackers/<int:tracker_id>/tickets", methods=["GET", "POST"])
def tracker_tickets(tracker_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if request.method == "POST":
        issue_key = request.json.get("issueKey")
        if not issue_key:
            return jsonify({"error": "Issue key is required"}), 400
        cursor.execute("INSERT INTO tracker_tickets (tracker_id, issue_key) VALUES (?, ?)", (tracker_id, issue_key))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    
    else:
        cursor.execute("SELECT issue_key, comment FROM tracker_tickets WHERE tracker_id = ?", (tracker_id,))
        tickets = [{"issue_key": r[0], "comment": r[1]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(tickets)

@app.route("/api/trackers/<int:tracker_id>/tickets/<string:issue_key>", methods=["DELETE"])
def delete_tracker_ticket(tracker_id, issue_key):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM tracker_tickets WHERE tracker_id = ? AND issue_key = ?", (tracker_id, issue_key))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/api/trackers/<int:tracker_id>/tickets/comment", methods=["PUT"])
def update_ticket_comment(tracker_id):
    data = request.json
    issue_key = data.get("issueKey")
    comment = data.get("comment")
    
    if not issue_key:
        return jsonify({"error": "Issue key is required"}), 400
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE tracker_tickets SET comment = ? WHERE tracker_id = ? AND issue_key = ?", (comment, tracker_id, issue_key))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

# =========================
# TODO API
# =========================

@app.route("/todo")
def todo_page():
    return render_template("todo.html", project=PROJECT_KEY)

@app.route("/api/todos", methods=["GET", "POST"])
def manage_todos():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if request.method == "POST":
        data = request.json
        title = data.get("title")
        description = data.get("description", "")
        priority = data.get("priority", "Low")
        due_date = data.get("due_date") # YYYY-MM-DD
        
        if not title or not due_date:
            return jsonify({"error": "Title and due date are required"}), 400
            
        cursor.execute('''
            INSERT INTO todos (title, description, priority, due_date)
            VALUES (?, ?, ?, ?)
        ''', (title, description, priority, due_date))
        conn.commit()
        todo_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": todo_id, "title": title, "status": "Pending"})
    
    else:
        date_filter = request.args.get("date")
        if date_filter:
            cursor.execute("SELECT id, title, description, priority, due_date, status FROM todos WHERE due_date = ? ORDER BY created_at DESC", (date_filter,))
        else:
            cursor.execute("SELECT id, title, description, priority, due_date, status FROM todos ORDER BY due_date ASC, created_at DESC")
            
        todos = []
        for r in cursor.fetchall():
            todos.append({
                "id": r[0],
                "title": r[1],
                "description": r[2],
                "priority": r[3],
                "due_date": r[4],
                "status": r[5]
            })
        conn.close()
        return jsonify(todos)

@app.route("/api/todos/<int:todo_id>", methods=["PUT", "DELETE"])
def update_delete_todo(todo_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if request.method == "DELETE":
        cursor.execute("DELETE FROM todos WHERE id = ?", (todo_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    
    else:
        data = request.json
        status = data.get("status")
        title = data.get("title")
        description = data.get("description")
        priority = data.get("priority")
        due_date = data.get("due_date")
        
        update_fields = []
        params = []
        if status:
            update_fields.append("status = ?")
            params.append(status)
        if title:
            update_fields.append("title = ?")
            params.append(title)
        if description is not None:
            update_fields.append("description = ?")
            params.append(description)
        if priority:
            update_fields.append("priority = ?")
            params.append(priority)
        if due_date:
            update_fields.append("due_date = ?")
            params.append(due_date)
            
        if update_fields:
            params.append(todo_id)
            cursor.execute(f"UPDATE todos SET {', '.join(update_fields)} WHERE id = ?", params)
            conn.commit()
            
        conn.close()
        return jsonify({"success": True})

# =========================
# EXPLORER API
# =========================

@app.route("/explorer")
def explorer_page():
    return render_template("explorer.html", project=PROJECT_KEY)

@app.route("/api/explorer", methods=["POST"])
def explorer_data():
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)
    project_key_str = str(PROJECT_KEY)

    data = request.json
    assignee = data.get("assignee")
    priority = data.get("priority")
    timeline = data.get("timeline", "all")
    start_date = data.get("startDate")
    end_date = data.get("endDate")
    is_production = data.get("production", False)
    customer = data.get("customer")
    
    # If raw JQL is provided, use it instead of building one
    raw_jql = data.get("jql")
    if raw_jql:
        jql = raw_jql
    else:
        jql = f'project = "{project_key_str}"'
        
        if assignee:
            jql += f' AND assignee = "{assignee}"'
        
        if customer:
            jql += f' AND "Customer" = "{customer}"'
        
        if priority and priority != "All":
            jql += f' AND priority = "{priority}"'
            
        # Timeline Logic
        if timeline == "today":
            jql += " AND created >= startOfDay()"
        elif timeline == "week":
            jql += " AND created >= startOfWeek()"
        elif timeline == "2weeks":
            jql += " AND created >= '-2w'"
        elif timeline == "month":
            jql += " AND created >= startOfMonth()"
        elif timeline == "year":
            jql += " AND created >= startOfYear()"
        elif timeline == "range":
            if start_date:
                jql += f" AND created >= '{start_date} 00:00'"
            if end_date:
                jql += f" AND created <= '{end_date} 23:59'"

        query_type = data.get("queryType", "normal")

        if query_type == "stale":
            # Logic: Created more than 1 week ago AND no update since 5 days
            jql += ' AND created <= "-1w" AND updated <= "-5d"'
        elif query_type == "critical":
            # Logic: Priority is Highest AND status is NOT in any closed/resolved category
            jql += ' AND priority = "Highest" AND status NOT IN ("Deployed", "Done", "Not A Bug", "Ready for Staging", "Resolved", "Staged", "Unable to Reproduce", "Ready for QA")'

        if is_production:
            jql += ' AND "platform[checkboxes]" = PRODUCTION'

        # Filter out common "Closed" or "Resolved" statuses for normal view if no specific status is requested
        if query_type == "normal":
            jql += ' AND status NOT IN ("Deployed", "Done", "Not A Bug", "Ready for Staging", "Resolved", "Staged", "Unable to Reproduce", "Ready for QA")'

        jql += " ORDER BY created DESC, assignee ASC, updated DESC"
    
    print(f"DEBUG Explorer JQL: {jql}")
    
    # Fetch issues
    start_at = 0
    max_results = 50
    
    params = {
        "jql": jql,
        "maxResults": max_results,
        "startAt": start_at,
        "fields": "summary,status,assignee,priority,created,updated,issuetype,customfield_10077"
    }
    
    try:
        res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params=params
        ).json()
        
        if "errorMessages" in res:
            return jsonify({"error": res["errorMessages"]}), 400
            
        issues = res.get("issues", [])
        if issues:
            print(f"DEBUG: First issue fields: {list(issues[0]['fields'].keys())}")
            print(f"DEBUG: Created: {issues[0]['fields'].get('created')}")
        formatted = []
        for i in issues:
            f = i["fields"]
            formatted.append({
                "key": i["key"],
                "summary": f.get("summary"),
                "status": f.get("status", {}).get("name"),
                "assignee": f.get("assignee", {}).get("displayName") if f.get("assignee") else "Unassigned",
                "priority": f.get("priority", {}).get("name"),
                "customer": ", ".join([c.get("value") for c in f.get("customfield_10077")]) if isinstance(f.get("customfield_10077"), list) else (f.get("customfield_10077", {}).get("value") if isinstance(f.get("customfield_10077"), dict) else f.get("customfield_10077")),
                "created": f.get("created"),
                "updated": f.get("updated"),
                "type": f.get("issuetype", {}).get("name")
            })
            
        return jsonify({
            "issues": formatted,
            "total": res.get("total", 0)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/query_builder")
def query_builder_page():
    return render_template("query_builder.html", project=PROJECT_KEY)

@app.route("/api/query_builder", methods=["POST"])
def query_builder_data():
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)
    project_key_str = str(PROJECT_KEY)

    data = request.json
    assignees = data.get("assignees", [])
    priorities = data.get("priorities", [])
    statuses = data.get("statuses", [])
    start_date = data.get("startDate")
    end_date = data.get("endDate")
    platform_mode = data.get("platform", "all")
    
    jql = f'project = "{project_key_str}"'
    
    if assignees:
        assignee_list = ", ".join([f'"{a}"' for a in assignees])
        jql += f' AND assignee IN ({assignee_list})'
    
    if platform_mode == "production":
        jql += ' AND "platform[checkboxes]" = PRODUCTION'
    elif platform_mode == "non-production":
        jql += ' AND ("platform[checkboxes]" != PRODUCTION OR "platform[checkboxes]" is EMPTY)'
    
    if priorities:
        priority_list = ", ".join([f'"{p}"' for p in priorities])
        jql += f' AND priority IN ({priority_list})'
        
    if statuses:
        status_list = ", ".join([f'"{s}"' for s in statuses])
        jql += f' AND status IN ({status_list})'
        
    if start_date:
        jql += f" AND updated >= '{start_date} 00:00'"
    if end_date:
        jql += f" AND updated <= '{end_date} 23:59'"
        
    jql += " ORDER BY updated DESC"
    
    print(f"DEBUG Query Builder JQL: {jql}")
    
    # Fetch issues using existing explorer_data logic but with the new JQL
    # Since I can't easily call explorer_data from here without refactoring,
    # I'll implement a common fetcher or just duplicate for now as is typical in this script.
    
    fields = "summary,status,assignee,priority,created,updated,issuetype,customfield_10077"
    params = {
        "jql": jql,
        "maxResults": 100,
        "fields": fields
    }
    
    try:
        res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params=params
        ).json()
        
        if "errorMessages" in res:
            return jsonify({"error": res["errorMessages"]}), 400
            
        issues = res.get("issues", [])
        formatted = []
        for i in issues:
            f = i["fields"]
            formatted.append({
                "key": i["key"],
                "summary": f.get("summary"),
                "status": f.get("status", {}).get("name"),
                "assignee": f.get("assignee", {}).get("displayName") if f.get("assignee") else "Unassigned",
                "priority": f.get("priority", {}).get("name"),
                "customer": ", ".join([c.get("value") for c in f.get("customfield_10077")]) if isinstance(f.get("customfield_10077"), list) else (f.get("customfield_10077", {}).get("value") if isinstance(f.get("customfield_10077"), dict) else f.get("customfield_10077")),
                "created": f.get("created"),
                "updated": f.get("updated"),
                "type": f.get("issuetype", {}).get("name")
            })
            
        return jsonify({
            "issues": formatted,
            "total": res.get("total", 0)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/bulk_update")
def bulk_update_page():
    return render_template("bulk_update.html", project=PROJECT_KEY)

@app.route("/api/bulk_update", methods=["POST"])
def bulk_update():
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)

    data = request.json
    issue_keys = data.get("issueKeys", [])
    field = data.get("field")
    value = data.get("value", "")
    if isinstance(value, str):
        value = value.strip()

    if not issue_keys or not field:
        return jsonify({"error": "Issue keys and field are required"}), 400

    results = []
    
    # Prepare Jira Payload
    jira_field = field
    if field == "sprint":
        jira_field = "customfield_10020"
    elif field == "labels":
        jira_field = "labels"
    elif field == "priority":
        jira_field = "priority"
    elif field == "assignee":
        jira_field = "assignee"

    for key in issue_keys:
        key = key.strip()
        if not key: continue
        
        url = f"{JIRA_DOMAIN}/rest/api/3/issue/{key}"
        
        update_data = {}
        if field == "labels":
            # Jira labels are an array. We usually append or replace. 
            # For simplicity in "bulk update", we append if it's a single label or replace if desired.
            # User said "add a value to update it". 
            update_data = {"update": {"labels": [{"add": value}]}}
        elif field == "assignee":
            update_data = {"fields": {"assignee": {"accountId": value if value != "unassigned" else None}}}
        elif field == "priority":
            update_data = {"fields": {"priority": {"name": value}}}
        elif field == "sprint":
            # Sprint is a custom field, strictly expects an integer ID or None to clear
            try:
                val_str = str(value).strip().lower()
                if not val_str or val_str == "none" or val_str == "null":
                    update_data = {"fields": {"customfield_10020": None}}
                else:
                    sprint_id = int(str(value).strip())
                    update_data = {"fields": {"customfield_10020": sprint_id}}
            except ValueError:
                results.append({"key": key, "status": "error", "message": f"Invalid Sprint ID: '{value}'. A numeric ID or 'None' is required."})
                continue
        else:
            # Generic field update
            update_data = {"fields": {jira_field: value}}

        try:
            jira_res = requests.put(url, headers=headers_dict, json=update_data)
            if jira_res.status_code == 204:
                results.append({"key": key, "status": "success"})
            else:
                results.append({"key": key, "status": "error", "message": jira_res.text})
        except Exception as e:
            results.append({"key": key, "status": "error", "message": str(e)})

    return jsonify(results)

@app.route("/api/jira_metadata", methods=["GET"])
def jira_metadata():
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)
    
    # Fetch priorities and statuses for the project to populate dropdowns
    try:
        pri_res = requests.get(f"{JIRA_DOMAIN}/rest/api/3/priority", headers=headers_dict)
        priorities = [{"id": p["id"], "name": p["name"]} for p in pri_res.json()]
        
        # We could also fetch sprints if we had the board ID, but that's complex.
        # For now, just priorities and maybe projects?
        
        return jsonify({
            "priorities": priorities
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/sprints", methods=["GET"])
def get_sprints():
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)
    project_key_str = str(PROJECT_KEY)

    # 1. Get all boards associated with project TIM
    boards_url = f"{JIRA_DOMAIN}/rest/agile/1.0/board"
    try:
        boards_res = requests.get(boards_url, headers=headers_dict, params={"projectKeyOrId": project_key_str})
        if boards_res.status_code != 200:
            return jsonify({"error": f"Failed to fetch boards: {boards_res.text}"}), 500
        
        boards = boards_res.json().get("values", [])
        all_sprints_map = {} # Use map to deduplicate by sprint ID
        
        for b in boards:
            board_id = b["id"]
            start_at = 0
            max_results = 50
            
            while True:
                sprint_url = f"{JIRA_DOMAIN}/rest/agile/1.0/board/{board_id}/sprint"
                res = requests.get(
                    sprint_url, 
                    headers=headers_dict, 
                    params={"startAt": start_at, "maxResults": max_results}
                )
                
                if res.status_code != 200:
                    break
                    
                data = res.json()
                values = data.get("values", [])
                if not values:
                    break
                    
                for s in values:
                    # Keep newest state/info if duplicate (though shouldn't happen much)
                    all_sprints_map[s["id"]] = {
                        "id": s["id"],
                        "name": s["name"],
                        "state": s["state"]
                    }
                
                if data.get("isLast", True):
                    break
                start_at += len(values)
        
        final_sprints = list(all_sprints_map.values())
        # Sort by ID descending (newest first)
        final_sprints.sort(key=lambda x: x["id"], reverse=True)
        return jsonify(final_sprints)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =========================
# TEAM MANAGEMENT API
# =========================

@app.route("/teams")
def teams_page():
    return render_template("teams.html", project=PROJECT_KEY)

@app.route("/api/teams", methods=["GET", "POST"])
def manage_teams():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if request.method == "POST":
        name = request.json.get("name")
        if not name:
            return jsonify({"error": "Team name is required"}), 400
        cursor.execute("INSERT INTO teams (name) VALUES (?)", (name,))
        conn.commit()
        team_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": team_id, "name": name})
    else:
        cursor.execute("SELECT id, name, created_at FROM teams ORDER BY name ASC")
        teams = [{"id": r[0], "name": r[1], "created_at": r[2]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(teams)

@app.route("/api/teams/<int:team_id>", methods=["DELETE"])
def delete_team(team_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM teams WHERE id = ?", (team_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/api/teams/<int:team_id>/members", methods=["GET", "POST"])
def team_members(team_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if request.method == "POST":
        data = request.json
        account_id = data.get("accountId")
        display_name = data.get("displayName")
        avatar_url = data.get("avatarUrl")
        if not account_id or not display_name:
            return jsonify({"error": "Account ID and Display Name are required"}), 400
        cursor.execute("INSERT INTO team_members (team_id, account_id, display_name, avatar_url) VALUES (?, ?, ?, ?)",
                       (team_id, account_id, display_name, avatar_url))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    else:
        cursor.execute("SELECT id, account_id, display_name, avatar_url FROM team_members WHERE team_id = ?", (team_id,))
        members = [
            {"id": r[0], "accountId": r[1], "account_id": r[1], "displayName": r[2], "avatarUrl": r[3]}
            for r in cursor.fetchall()
        ]
        conn.close()
        return jsonify(members)

@app.route("/api/teams/<int:team_id>/members/<int:member_id>", methods=["DELETE"])
def delete_team_member(team_id, member_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM team_members WHERE id = ? AND team_id = ?", (member_id, team_id))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

# =========================
# SPRINT PLANNING API
# =========================

@app.route("/planning/<int:team_id>")
def planning_page(team_id):
    return render_template("planning.html", project=PROJECT_KEY, team_id=team_id)

@app.route("/api/teams/<int:team_id>/sprints", methods=["GET", "POST"])
def team_sprints(team_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if request.method == "POST":
        name = request.json.get("name")
        if not name:
            return jsonify({"error": "Sprint name is required"}), 400
        cursor.execute("INSERT INTO sprints (team_id, name) VALUES (?, ?)", (team_id, name))
        conn.commit()
        sprint_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": sprint_id, "name": name})
    else:
        cursor.execute("SELECT id, name, state, created_at FROM sprints WHERE team_id = ? ORDER BY created_at DESC", (team_id,))
        sprints = [{"id": r[0], "name": r[1], "state": r[2], "created_at": r[3]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(sprints)

@app.route("/api/sprints/<int:sprint_id>/weeks", methods=["GET", "POST"])
def sprint_weeks(sprint_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if request.method == "POST":
        data = request.json
        week_number = data.get("weekNumber")
        goal = data.get("goal", "")
        cursor.execute("INSERT INTO sprint_weeks (sprint_id, week_number, goal) VALUES (?, ?, ?)",
                       (sprint_id, week_number, goal))
        conn.commit()
        week_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": week_id, "weekNumber": week_number})
    else:
        cursor.execute("SELECT id, week_number, goal FROM sprint_weeks WHERE sprint_id = ? ORDER BY week_number ASC", (sprint_id,))
        weeks = [{"id": r[0], "weekNumber": r[1], "goal": r[2]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(weeks)

@app.route("/api/sprint_weeks/<int:week_id>", methods=["DELETE"])
def delete_sprint_week(week_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sprint_weeks WHERE id = ?", (week_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/api/sprints/<int:sprint_id>/tickets", methods=["GET", "POST"])
def sprint_tickets_api(sprint_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if request.method == "POST":
        data = request.json
        week_id = data.get("weekId")
        issue_key = data.get("issueKey")
        if not issue_key:
            return jsonify({"error": "Issue key is required"}), 400
        cursor.execute("INSERT INTO sprint_tickets (sprint_id, week_id, issue_key) VALUES (?, ?, ?)",
                       (sprint_id, week_id, issue_key))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    else:
        cursor.execute("SELECT id, week_id, issue_key, comment, pr_raised, demo_done, pr_merged FROM sprint_tickets WHERE sprint_id = ?", (sprint_id,))
        tickets = [{"id": r[0], "weekId": r[1], "issueKey": r[2], "comment": r[3], "prRaised": bool(r[4]), "demoDone": bool(r[5]), "prMerged": bool(r[6])} for r in cursor.fetchall()]
        conn.close()
        return jsonify(tickets)

@app.route("/api/sprint_tickets/<int:ticket_id>", methods=["PUT", "DELETE"])
def manage_sprint_ticket(ticket_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if request.method == "DELETE":
        cursor.execute("DELETE FROM sprint_tickets WHERE id = ?", (ticket_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    elif request.method == "PUT":
        data = request.json
        comment = data.get("comment")
        pr_raised = 1 if data.get("prRaised") else 0
        demo_done = 1 if data.get("demoDone") else 0
        pr_merged = 1 if data.get("prMerged") else 0
        
        cursor.execute("""
            UPDATE sprint_tickets 
            SET comment = ?, pr_raised = ?, demo_done = ?, pr_merged = ? 
            WHERE id = ?
        """, (comment, pr_raised, demo_done, pr_merged, ticket_id))
        
        conn.commit()
        conn.close()
        return jsonify({"success": True})

@app.route("/status_tracker")
def status_tracker():
    return render_template("status_tracker.html", project=PROJECT_KEY)

@app.route("/api/status_history", methods=["GET"])
def status_history():
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)
    project_key_str = str(PROJECT_KEY)

    keys_input = request.args.get("issue_keys", "")
    target_date = request.args.get("target_date", "") # Expected format YYYY-MM-DD
    
    # Process keys
    keys = re.split(r'[\s,]+', keys_input)
    keys = [k.strip().upper() for k in keys if k.strip()]
    
    # User's timezone offset (IST: +5:30)
    user_tz = timezone(timedelta(hours=5, minutes=30))

    # JQL to fetch these issues
    if keys:
        keys_str = ", ".join([f'"{k}"' for k in keys])
        jql = f"key IN ({keys_str})"
    elif target_date:
        # Fetch status changes for the target date and adjacent days to account for TZ shifts
        # JQL search is inclusive. Searching around the date helps capture all relevant issues.
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        prev_day = (target_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        next_day = (target_dt + timedelta(days=1)).strftime("%Y-%m-%d")
        jql = f'project = "{project_key_str}" AND status changed DURING ("{prev_day}", "{next_day}")'
    else:
        return jsonify([])
    
    url = f"{JIRA_DOMAIN}/rest/api/3/search/jql"
    print(f"DEBUG: HITTING STATUS_HISTORY. JQL: {jql}")
    
    params = {
        "jql": jql,
        "expand": "changelog",
        "fields": "summary,assignee,status,avatarUrls",
        "maxResults": 100
    }
    
    try:
        res = requests.get(url, headers=headers_dict, params=params)
        if res.status_code != 200:
            return jsonify({"error": f"Jira error: {res.text}"}), res.status_code
        
        issues = res.json().get("issues", [])
        updates = []
        
        for issue in issues:
            key = issue["key"]
            summary = issue["fields"]["summary"]
            changelog = issue.get("changelog", {}).get("histories", [])
            
            for history in changelog:
                # Jira timestamp example: 2026-02-15T16:05:05.123+0530
                # We need to handle the format which might have +HHMM instead of +HH:MM
                ts_str = history["created"]
                # Clean up timezone offset if it doesn't have a colon (common in Jira REST API)
                if "+" in ts_str and ts_str[-3] != ":":
                    ts_str = ts_str[:-2] + ":" + ts_str[-2:]
                elif "-" in ts_str and ts_str[-3] != ":":
                    ts_str = ts_str[:-2] + ":" + ts_str[-2:]
                
                try:
                    # fromisoformat handles +HH:MM in Python 3.7+
                    created_dt_utc = datetime.fromisoformat(ts_str)
                    # Convert to user's timezone (IST)
                    created_dt_local = created_dt_utc.astimezone(user_tz)
                    created_date_local = created_dt_local.strftime("%Y-%m-%d")
                except Exception as e:
                    print(f"DEBUG: Parsing error for {ts_str}: {e}")
                    created_date_local = history["created"][:10]
                
                # If target_date is provided, only show updates for that date in local time
                if target_date and created_date_local != target_date:
                    continue
                
                for item in history["items"]:
                    if item["field"] == "status":
                        updates.append({
                            "key": key,
                            "summary": summary,
                            "author": {
                                "name": history["author"]["displayName"],
                                "avatar": history["author"]["avatarUrls"]["24x24"]
                            },
                            "created": history["created"],
                            "from": item["fromString"],
                            "to": item["toString"]
                        })
        
        # Sort by creation time descending
        updates.sort(key=lambda x: x["created"], reverse=True)
        return jsonify(updates)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/ticket_history/<key>", methods=["GET"])
def ticket_history(key):
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)

    url = f"{JIRA_DOMAIN}/rest/api/3/issue/{key}?expand=changelog"
    try:
        res = requests.get(url, headers=headers_dict)
        if res.status_code != 200:
            return jsonify({"error": f"Jira error: {res.text}"}), res.status_code
        
        data = res.json()
        changelog = data.get("changelog", {}).get("histories", [])
        
        formatted_history = []
        for history in changelog:
            for item in history["items"]:
                formatted_history.append({
                    "author": {
                        "name": history["author"]["displayName"],
                        "avatar": history["author"]["avatarUrls"]["24x24"]
                    },
                    "created": history["created"],
                    "field": item["field"],
                    "from": item["fromString"],
                    "to": item["toString"]
                })
        
        # Sort by creation time descending
        formatted_history.sort(key=lambda x: x["created"], reverse=True)
        return jsonify({
            "key": key,
            "summary": data["fields"]["summary"],
            "history": formatted_history
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =========================
# SCRUM NOTES
# =========================

@app.route("/scrum_notes")
def scrum_notes_page():
    resp = make_response(render_template("scrum_notes.html", project=PROJECT_KEY))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

@app.route("/api/jira/ticket/<key>", methods=["GET"])
def fetch_jira_ticket(key):
    """Fetch a single Jira ticket's details (summary, status, priority, assignee)."""
    # Explicitly cast LocalProxy objects
    headers_dict = dict(HEADERS)
    print(f"DEBUG fetch_jira_ticket: Headers are {dict(headers_dict)}")

    try:
        res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/issue/{key.upper()}",
            headers=headers_dict,
            params={"fields": "summary,status,priority,assignee,issuetype"}
        )
        print(f"DEBUG fetch_jira_ticket: Jira returned {res.status_code} {res.text}")
        if res.status_code == 404:
            return jsonify({"error": f"Ticket {key} not found"}), 404
        if res.status_code != 200:
            return jsonify({"error": res.text}), res.status_code
        data = res.json()
        f = data["fields"]
        return jsonify({
            "key": data["key"],
            "summary": f.get("summary"),
            "status": f.get("status", {}).get("name"),
            "priority": f.get("priority", {}).get("name"),
            "assignee": f.get("assignee", {}).get("displayName") if f.get("assignee") else None,
            "type": f.get("issuetype", {}).get("name")
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/scrum_notes", methods=["GET", "POST"])
def scrum_notes():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if request.method == "POST":
        data = request.json
        date = data.get("date")
        team_id = data.get("team_id")
        member_id = data.get("member_id")
        member_name = data.get("member_name")
        ticket_key = (data.get("ticket_key") or "").strip().upper()
        comment = data.get("comment", "")
        deadline = data.get("deadline") or None

        if isinstance(member_id, str):
            member_id = member_id.strip()

        invalid_member_ids = {None, "", "undefined", "null"}
        
        # If member_id is explicitly invalid, try to resolve it
        if (member_id in invalid_member_ids or member_id is None) and member_name and team_id:
            cursor.execute(
                """
                SELECT account_id
                FROM team_members
                WHERE team_id = ?
                  AND lower(trim(display_name)) = lower(trim(?))
                LIMIT 1
                """,
                (team_id, member_name),
            )
            row = cursor.fetchone()
            if row:
                member_id = row[0]
            else:
                # If resolution fails, ensure we don't save "undefined"
                conn.close()
                return jsonify({"error": f"Could not resolve member ID for '{member_name}'. Please refresh the page."}), 400

        if not all([date, team_id, member_id, member_name, ticket_key]) or member_id in invalid_member_ids:
            conn.close()
            return jsonify({"error": "Missing required fields (member_id)"}), 400

        cursor.execute("""
            INSERT INTO scrum_notes (date, team_id, member_id, member_name, ticket_key, comment, deadline)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (date, team_id, member_id, member_name, ticket_key, comment, deadline))
        conn.commit()
        new_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": new_id, "success": True})

    else:  # GET
        date = request.args.get("date")
        team_id = request.args.get("team_id")
        if not date or not team_id:
            conn.close()
            return jsonify({"error": "date and team_id are required"}), 400
        cursor.execute("""
            SELECT id, date, team_id, member_id, member_name, ticket_key, comment, deadline, status
            FROM scrum_notes
            WHERE date = ? AND team_id = ?
            ORDER BY member_name, created_at
        """, (date, team_id))
        rows = [{
            "id": r[0], "date": r[1], "team_id": r[2],
            "member_id": r[3], "member_name": r[4],
            "ticket_key": r[5], "comment": r[6],
            "deadline": r[7], "status": r[8]
        } for r in cursor.fetchall()]
        conn.close()
        return jsonify(rows)

@app.route("/api/scrum_notes/<int:note_id>", methods=["PUT", "DELETE"])
def scrum_note_item(note_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if request.method == "DELETE":
        cursor.execute("DELETE FROM scrum_notes WHERE id = ?", (note_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})

    else:  # PUT
        data = request.json
        fields, params = [], []
        if "comment" in data:
            fields.append("comment = ?")
            params.append(data["comment"])
        if "deadline" in data:
            fields.append("deadline = ?")
            params.append(data["deadline"] or None)
        if "status" in data:
            fields.append("status = ?")
            params.append(data["status"])
        if not fields:
            conn.close()
            return jsonify({"error": "No fields to update"}), 400
        params.append(note_id)
        cursor.execute(f"UPDATE scrum_notes SET {', '.join(fields)} WHERE id = ?", params)
        conn.commit()
        conn.close()
        return jsonify({"success": True})

@app.route("/api/scrum_notes/report", methods=["GET"])
def scrum_notes_report():
    start = request.args.get("start")
    end = request.args.get("end")
    team_id = request.args.get("team_id")
    member_id = request.args.get("member_id")

    if not start or not end:
        return jsonify({"error": "start and end dates are required"}), 400

    query = """
        SELECT id, date, team_id, member_id, member_name, ticket_key, comment, deadline, status
        FROM scrum_notes
        WHERE date BETWEEN ? AND ?
    """
    params = [start, end]

    if team_id:
        query += " AND team_id = ?"
        params.append(team_id)
    if member_id:
        query += " AND member_id = ?"
        params.append(member_id)

    query += " ORDER BY date DESC, member_name, ticket_key"

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = [{
        "id": r[0], "date": r[1], "team_id": r[2],
        "member_id": r[3], "member_name": r[4],
        "ticket_key": r[5], "comment": r[6],
        "deadline": r[7], "status": r[8]
    } for r in cursor.fetchall()]
    conn.close()
    return jsonify(rows)

if __name__ == "__main__":
    app.run(debug=True)
