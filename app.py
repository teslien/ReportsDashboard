from flask import Flask, render_template, request, jsonify, make_response, has_request_context, redirect, url_for, send_file
from werkzeug.local import LocalProxy
import io
import re
import json
import sqlite3
import os
import requests
import base64
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from xhtml2pdf import pisa
from pypdf import PdfWriter

app = Flask(__name__)

# =========================
# 🔴 CONFIG (DYNAMIC)
# =========================

# Defaults
DEFAULT_JIRA_EMAIL = ""
DEFAULT_JIRA_API_TOKEN = ""
DEFAULT_PROJECT_KEY = ""
DEFAULT_JIRA_DOMAIN = "https://lumberfi.atlassian.net"

def _decode_value(value):
    if not value or value.lower() in ("null", "undefined"):
        return ""
    try:
        # Some values might be double quoted or URL encoded
        value = unquote(value).strip()
        # Remove surrounding quotes if present (some browsers/servers add them)
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        return value.strip()
    except Exception:
        return value.strip()

def _get_jira_config():
    """Fetch Jira config from DB. Returns (email, token, project_key, domain)."""
    try:
        conn = sqlite3.connect("tracker.db")
        cursor = conn.cursor()
        cursor.execute("SELECT email, api_token, project_key, jira_domain FROM jira_config WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        if row:
            return row[0] or "", row[1] or "", row[2] or "", row[3] or DEFAULT_JIRA_DOMAIN
    except Exception:
        pass
    return "", "", "", DEFAULT_JIRA_DOMAIN

def _get_project_key():
    _, _, db_project, _ = _get_jira_config()
    project = db_project or DEFAULT_PROJECT_KEY
    if has_request_context():
        req_project = request.headers.get("X-Project-Key") or request.args.get("project_key") or request.cookies.get("project_key")
        if req_project:
            project = _decode_value(req_project)
    print(f"DEBUG: _get_project_key decoded: {repr(project)}")
    return project.upper() if project else ""

def _get_jira_domain():
    _, _, _, db_domain = _get_jira_config()
    domain = db_domain or DEFAULT_JIRA_DOMAIN
    if has_request_context():
        req_domain = request.headers.get("X-Jira-Domain") or request.args.get("jira_domain") or request.cookies.get("jira_domain")
        if req_domain:
            domain = _decode_value(req_domain)
    
    if domain and not domain.startswith("http"):
        domain = "https://" + domain
    return domain.rstrip("/")

def _get_jira_headers():
    db_email, db_token, _, _ = _get_jira_config()
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
JIRA_DOMAIN = LocalProxy(_get_jira_domain)

@app.context_processor
def inject_config():
    return dict(
        JIRA_DOMAIN=str(JIRA_DOMAIN),
        project=str(PROJECT_KEY)
    )

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
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS todo_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            color TEXT DEFAULT 'blue'
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
            deploy_status TEXT DEFAULT 'N/A',
            qa_assignee TEXT DEFAULT '',
            qa_status TEXT DEFAULT 'Pending',
            bugs_found TEXT DEFAULT '',
            completed INTEGER DEFAULT 0,
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
            tags        TEXT    DEFAULT \'\',
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
            project_key TEXT,
            jira_domain TEXT
        )
    ''')
    
    # Migration: Add new columns if they don't exist
    cursor.execute("PRAGMA table_info(jira_config)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'jira_domain' not in columns:
        cursor.execute("ALTER TABLE jira_config ADD COLUMN jira_domain TEXT")
    
    cursor.execute("PRAGMA table_info(sprint_tickets)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'comment' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN comment TEXT DEFAULT ''")
    if 'pr_raised' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN pr_raised INTEGER DEFAULT 0")
    if 'demo_done' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN demo_done INTEGER DEFAULT 0")
    if 'pr_merged' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN pr_merged INTEGER DEFAULT 0")
    if 'deploy_status' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN deploy_status TEXT DEFAULT 'N/A'")
    if 'qa_assignee' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN qa_assignee TEXT DEFAULT ''")
    if 'qa_status' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN qa_status TEXT DEFAULT 'Pending'")
    if 'bugs_found' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN bugs_found TEXT DEFAULT ''")
    if 'completed' not in columns:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN completed INTEGER DEFAULT 0")

    cursor.execute("PRAGMA table_info(todos)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'tags' not in columns:
        cursor.execute("ALTER TABLE todos ADD COLUMN tags TEXT DEFAULT '[]'")

    # Migration: Add tags column to scrum_notes if it doesn't exist
    cursor.execute("PRAGMA table_info(scrum_notes)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'tags' not in columns:
        cursor.execute("ALTER TABLE scrum_notes ADD COLUMN tags TEXT DEFAULT ''")

    # Custom Reports Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS custom_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            jql TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

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
        domain = data.get("jira_domain", "").strip()
        
        if not email or not token or not project:
            return jsonify({"error": "All fields are required"}), 400
            
        cursor.execute("""
            INSERT INTO jira_config (id, email, api_token, project_key, jira_domain)
            VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                email=excluded.email,
                api_token=excluded.api_token,
                project_key=excluded.project_key,
                jira_domain=excluded.jira_domain
        """, (email, token, project, domain))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
        
    else:  # GET
        cursor.execute("SELECT email, api_token, project_key, jira_domain FROM jira_config WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        if row:
            return jsonify({
                "email": row[0],
                "token": row[1],
                "project_key": row[2],
                "jira_domain": row[3] or DEFAULT_JIRA_DOMAIN
            })
        return jsonify({"email": "", "token": "", "project_key": "", "jira_domain": DEFAULT_JIRA_DOMAIN})

@app.route("/api/settings/todo_tags", methods=["GET", "POST"])
def todo_tags_settings_api():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if request.method == "POST":
        data = request.json or {}
        name = (data.get("name") or "").strip()
        color = (data.get("color") or "blue").strip()
        
        if not name:
            conn.close()
            return jsonify({"error": "Tag name is required"}), 400
        
        cursor.execute("INSERT INTO todo_tags (name, color) VALUES (?, ?)", (name, color))
        conn.commit()
        tag_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": tag_id, "name": name, "color": color})
        
    else:
        cursor.execute("SELECT id, name, color FROM todo_tags ORDER BY id ASC")
        tags = [{"id": r[0], "name": r[1], "color": r[2]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(tags)

@app.route("/api/settings/todo_tags/<int:tag_id>", methods=["DELETE"])
def delete_todo_tag(tag_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM todo_tags WHERE id = ?", (tag_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/")
def index():
    email, token, _, _ = _get_jira_config()
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

    project_key_str = str(PROJECT_KEY)
    if not project_key_str:
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

@app.route("/api/jira/search", methods=["GET"])
def jira_search():
    query = request.args.get('q', '')
    if len(query) < 2:
        return jsonify([])
        
    project_key_str = str(PROJECT_KEY)
    headers_dict = dict(HEADERS)
    
    # Search for issues by key or summary in the current project
    jql = f'project = "{project_key_str}" AND (key ~ "{query}*" OR summary ~ "{query}*")'
    
    try:
        res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search",
            headers=headers_dict,
            params={
                "jql": jql,
                "maxResults": 10,
                "fields": "summary,issuetype"
            }
        )
        data = res.json()
        results = []
        for issue in data.get('issues', []):
            results.append({
                "id": issue['key'],
                "text": f"{issue['key']}: {issue['fields']['summary']}",
                "key": issue['key']
            })
        return jsonify(results)
    except Exception as e:
        print(f"DEBUG: Search Error: {str(e)}")
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
# SPRINT DASHBOARD API
# =========================
@app.route("/api/dashboard/sprint_stats", methods=["POST"])
def sprint_stats():
    data = request.json
    team_id = data.get("team_id")
    sprint_id = data.get("sprint_id") # Optional Jira Sprint ID
    production_only = data.get("production_only", False)
    
    if not team_id:
        return jsonify({"error": "Team is required"}), 400
        
    # 1. Get Team Members
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT account_id, display_name, avatar_url FROM team_members WHERE team_id = ?", (team_id,))
    members = [{"id": r[0], "name": r[1], "avatar": r[2]} for r in cursor.fetchall()]
    conn.close()
    
    if not members:
        return jsonify({"error": "No members found for this team"}), 400
        
    member_ids = [m["id"] for m in members]
    member_ids_str = ", ".join([f'"{mid}"' for mid in member_ids])
    
    headers_dict = dict(HEADERS)
    project_key_str = str(PROJECT_KEY)

    # 2. Build JQL
    # We must have project key
    jql = f'project = "{project_key_str}"'
    
    # Optional Sprint filter
    if sprint_id:
        jql += f' AND sprint = {sprint_id}'
    
    # Required Assignee filter (for this dashboard, we only track the team members)
    jql += f' AND assignee IN ({member_ids_str})'
    
    # Optional Production filter
    if production_only:
        jql += ' AND "platform[checkboxes]" = PRODUCTION'
    
    try:
        res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params={
                "jql": jql, 
                "maxResults": 1000, 
                "fields": "summary,status,priority,assignee,issuetype"
            }
        ).json()
        
        if "errorMessages" in res:
            return jsonify({"error": res["errorMessages"]}), 400
            
        all_issues = res.get("issues", [])
        
        # 3. Calculate Stats
        done_issues = [i for i in all_issues if i["fields"]["status"]["statusCategory"]["key"] == "done"]
        active_issues = [i for i in all_issues if i["fields"]["status"]["statusCategory"]["key"] != "done"]
        
        total_solved = len(done_issues)
        bugs_solved = len([i for i in done_issues if i["fields"]["issuetype"]["name"].lower() == "bug"])
        
        # User Performance
        performance = {}
        for m in members:
            performance[m["id"]] = {"name": m["name"], "avatar": m["avatar"], "solved": 0, "active": 0}
            
        for i in all_issues:
            assignee = i["fields"].get("assignee")
            if assignee:
                aid = assignee["accountId"]
                if aid in performance:
                    if i["fields"]["status"]["statusCategory"]["key"] == "done":
                        performance[aid]["solved"] += 1
                    else:
                        performance[aid]["active"] += 1
                        
        # Today's Work: Active tickets with their current status
        today_work = []
        for i in active_issues:
            today_work.append({
                "key": i["key"],
                "summary": i["fields"]["summary"],
                "assignee": i["fields"].get("assignee", {}).get("displayName", "Unassigned"),
                "status": i["fields"]["status"]["name"],
                "type_icon": i["fields"]["issuetype"].get("iconUrl")
            })
            
        # Group status distribution for chart
        status_dist = {}
        for i in all_issues:
            s = i["fields"]["status"]["name"]
            status_dist[s] = status_dist.get(s, 0) + 1
            
        # Group type distribution
        type_dist = {}
        for i in all_issues:
            t = i["fields"]["issuetype"]["name"]
            type_dist[t] = type_dist.get(t, 0) + 1
            
        # Group priority distribution
        pri_dist = {}
        for i in all_issues:
            p = i["fields"]["priority"]["name"]
            pri_dist[p] = pri_dist.get(p, 0) + 1
            
        # NEW: Fetch local tracking data for the table
        tracking_data = {}
        if sprint_id:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT issue_key, pr_raised, pr_merged, deploy_status, qa_assignee, qa_status, bugs_found, completed
                FROM sprint_tickets 
                WHERE sprint_id = ?
            """, (sprint_id,))
            rows = cursor.fetchall()
            for r in rows:
                tracking_data[r["issue_key"]] = {
                    "pr_raised": bool(r["pr_raised"]),
                    "pr_merged": bool(r["pr_merged"]),
                    "deploy_status": r["deploy_status"],
                    "qa_assignee": r["qa_assignee"],
                    "qa_status": r["qa_status"],
                    "bugs_found": r["bugs_found"],
                    "completed": bool(r["completed"])
                }
            conn.close()

        # Merge local data into all issues for the tracking table
        tracking_issues = []
        for i in all_issues:
            key = i["key"]
            local = tracking_data.get(key, {
                "pr_raised": False,
                "pr_merged": False,
                "deploy_status": "N/A",
                "qa_assignee": "",
                "qa_status": "Pending",
                "bugs_found": "",
                "completed": False
            })
            tracking_issues.append({
                "key": key,
                "summary": i["fields"]["summary"],
                "status": i["fields"]["status"]["name"],
                "assignee": i["fields"].get("assignee", {}).get("displayName", "Unassigned"),
                "type_icon": i["fields"]["issuetype"].get("iconUrl"),
                "local": local
            })

        return jsonify({
            "total_solved": total_solved,
            "bugs_solved": bugs_solved,
            "active_load": len(active_issues),
            "performance": list(performance.values()),
            "today_work": today_work,
            "tracking_issues": tracking_issues,
            "charts": {
                "status": status_dist,
                "type": type_dist,
                "priority": pri_dist
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/sprint_tickets/update_field", methods=["POST"])
def update_sprint_ticket_field():
    data = request.json
    sprint_id = data.get("sprint_id")
    issue_key = data.get("issue_key")
    field = data.get("field")
    value = data.get("value")
    
    if not sprint_id or not issue_key or not field:
        return jsonify({"error": "Missing required fields"}), 400
        
    # Security: whitelist fields
    allowed_fields = ['pr_raised', 'pr_merged', 'deploy_status', 'qa_assignee', 'qa_status', 'bugs_found', 'completed', 'comment', 'demo_done']
    if field not in allowed_fields:
        return jsonify({"error": "Invalid field"}), 400
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if record exists
    cursor.execute("SELECT id FROM sprint_tickets WHERE sprint_id = ? AND issue_key = ?", (sprint_id, issue_key))
    row = cursor.fetchone()
    
    if row:
        cursor.execute(f"UPDATE sprint_tickets SET {field} = ? WHERE id = ?", (value, row[0]))
    else:
        cursor.execute(f"INSERT INTO sprint_tickets (sprint_id, issue_key, {field}) VALUES (?, ?, ?)", (sprint_id, issue_key, value))
        
    conn.commit()
    conn.close()
    return jsonify({"success": True})

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

# =========================
# CUSTOM REPORTS API
# =========================

@app.route("/api/reports", methods=["GET", "POST"])
def manage_reports():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if request.method == "POST":
        data = request.json
        name = data.get("name")
        jql = data.get("jql")
        
        if not name or not jql:
            return jsonify({"error": "Name and JQL are required"}), 400
            
        cursor.execute("INSERT INTO custom_reports (name, jql) VALUES (?, ?)", (name, jql))
        conn.commit()
        report_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": report_id, "name": name, "jql": jql})
    
    else:  # GET
        cursor.execute("SELECT id, name, jql, created_at FROM custom_reports ORDER BY created_at DESC")
        reports = [{"id": r[0], "name": r[1], "jql": r[2], "created": r[3]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(reports)

@app.route("/api/reports/<int:report_id>", methods=["GET", "PUT", "DELETE"])
def report_detail(report_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if request.method == "GET":
        cursor.execute("SELECT id, name, jql, created_at FROM custom_reports WHERE id = ?", (report_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return jsonify({"id": row[0], "name": row[1], "jql": row[2], "created": row[3]})
        return jsonify({"error": "Report not found"}), 404
        
    elif request.method == "PUT":
        data = request.json
        name = data.get("name")
        jql = data.get("jql")
        
        if not name or not jql:
            return jsonify({"error": "Name and JQL are required"}), 400
            
        cursor.execute("UPDATE custom_reports SET name = ?, jql = ? WHERE id = ?", (name, jql, report_id))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
        
    elif request.method == "DELETE":
        cursor.execute("DELETE FROM custom_reports WHERE id = ?", (report_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})

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

    fields = "summary,status,assignee,priority,issuetype,created,updated,resolutiondate,project,reporter,resolution,duedate,customfield_10016,customfield_10077"
    
    all_issues = []
    start_at = 0
    max_results = 100
    
    while True:
        try:
            jira_response = requests.get(
                f"{str(JIRA_DOMAIN)}/rest/api/3/search/jql",
                headers=headers_dict,
                params={
                    "jql": jql,
                    "maxResults": max_results,
                    "startAt": start_at,
                    "fields": fields
                }
            )
            
            if jira_response.status_code != 200:
                print(f"DEBUG: Jira Error {jira_response.status_code}: {jira_response.text}")
                return jsonify({"error": f"Jira API Error {jira_response.status_code}", "details": jira_response.text}), jira_response.status_code

            res = jira_response.json()
        except Exception as e:
            print(f"DEBUG: Exception during Jira Request: {e}")
            return jsonify({"error": "Failed to connect to Jira or parse response", "details": str(e)}), 500
        
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
        tags = data.get("tags", "[]")
        if isinstance(tags, list):
            tags = json.dumps(tags)
        
        if not title or not due_date:
            return jsonify({"error": "Title and due date are required"}), 400
            
        cursor.execute('''
            INSERT INTO todos (title, description, priority, due_date, tags)
            VALUES (?, ?, ?, ?, ?)
        ''', (title, description, priority, due_date, tags))
        conn.commit()
        todo_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": todo_id, "title": title, "status": "Pending"})
    
    else:
        date_filter = request.args.get("date")
        if date_filter:
            cursor.execute("SELECT id, title, description, priority, due_date, status, tags FROM todos WHERE due_date = ? ORDER BY created_at DESC", (date_filter,))
        else:
            cursor.execute("SELECT id, title, description, priority, due_date, status, tags FROM todos ORDER BY due_date ASC, created_at DESC")
            
        todos = []
        for r in cursor.fetchall():
            todos.append({
                "id": r[0],
                "title": r[1],
                "description": r[2],
                "priority": r[3],
                "due_date": r[4],
                "status": r[5],
                "tags": r[6] or "[]"
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
        tags = data.get("tags")
        if isinstance(tags, list):
            tags = json.dumps(tags)
        
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
        if tags is not None:
            update_fields.append("tags = ?")
            params.append(tags)
            
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
    
    # Use JQL to find issues for this project to extract sprint data
    jql = f'project = "{project_key_str}"'
    
    url = f"{JIRA_DOMAIN}/rest/api/3/search/jql"
    params = {
        "jql": jql,
        "maxResults": 100, # Assuming 100 issues specify most open sprints
        "fields": "customfield_10020" # The sprint field
    }
    
    try:
        query = request.args.get("q", "").lower()
        res = requests.get(url, headers=headers_dict, params=params)
        if res.status_code != 200:
            return jsonify({"error": f"Jira error: {res.text}"}), res.status_code
        
        data = res.json()
        issues = data.get("issues", [])
        
        all_sprints_map = {}
        for issue in issues:
            sprints = issue["fields"].get("customfield_10020")
            if sprints and isinstance(sprints, list):
                for s in sprints:
                    s_id = s.get("id")
                    s_name = s.get("name", "")
                    s_state = s.get("state", "unknown")
                    
                    # Only include active or future sprints (openSprints normally does this, but being safe)
                    if s_state in ["active", "future"]:
                        if query and query not in s_name.lower():
                            continue
                        all_sprints_map[s_id] = {
                            "id": s_id,
                            "name": s_name,
                            "state": s_state
                        }
        
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

@app.route("/api/sprint_weeks/<int:week_id>", methods=["PUT", "DELETE"])
def manage_sprint_week(week_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if request.method == "DELETE":
        cursor.execute("DELETE FROM sprint_weeks WHERE id = ?", (week_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    elif request.method == "PUT":
        data = request.json
        goal = data.get("goal")
        cursor.execute("UPDATE sprint_weeks SET goal = ? WHERE id = ?", (goal, week_id))
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
        week_id = data.get("weekId") # New field for moving tickets
        
        # Build dynamic query
        fields = ["comment = ?", "pr_raised = ?", "demo_done = ?", "pr_merged = ?"]
        params = [comment, pr_raised, demo_done, pr_merged]
        
        if week_id is not None:
            fields.append("week_id = ?")
            params.append(week_id)
            
        params.append(ticket_id)
        
        cursor.execute(f"""
            UPDATE sprint_tickets 
            SET {', '.join(fields)}
            WHERE id = ?
        """, params)
        
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
            params={"fields": "summary,status,priority,assignee,issuetype,description,reporter,created,updated,customfield_10016,customfield_10077,customfield_10020,labels,components,comment"}
        )
        print(f"DEBUG fetch_jira_ticket: Jira returned {res.status_code} {res.text}")
        if res.status_code == 404:
            return jsonify({"error": f"Ticket {key} not found"}), 404
        if res.status_code != 200:
            return jsonify({"error": res.text}), res.status_code
        data = res.json()
        f = data["fields"]
        
        # Extract Customer
        customer = "N/A"
        cust_val = f.get("customfield_10077")
        if cust_val:
            if isinstance(cust_val, list) and len(cust_val) > 0:
                v = cust_val[0]
                customer = v.get("value") or v if isinstance(v, dict) else str(v)
            elif isinstance(cust_val, dict):
                customer = cust_val.get("value") or str(cust_val)
            else:
                customer = str(cust_val)

        # Extract Sprint
        sprint = "N/A"
        sprint_val = f.get("customfield_10020")
        if sprint_val and isinstance(sprint_val, list) and len(sprint_val) > 0:
            sp = sprint_val[-1]
            sprint = sp.get("name") or str(sp)

        # Extract Story Points
        story_points = f.get("customfield_10016")

        # Extract Description (Jira v3 uses Doc format)
        description = f.get("description")

        return jsonify({
            "key": data["key"],
            "summary": f.get("summary"),
            "status": f.get("status", {}).get("name"),
            "priority": f.get("priority", {}).get("name"),
            "assignee": f.get("assignee", {}).get("displayName") if f.get("assignee") else "Unassigned",
            "assignee_avatar": f.get("assignee", {}).get("avatarUrls", {}).get("48x48") if f.get("assignee") else None,
            "type": f.get("issuetype", {}).get("name"),
            "type_icon": f.get("issuetype", {}).get("iconUrl"),
            "reporter": f.get("reporter", {}).get("displayName") if f.get("reporter") else "Unknown",
            "created": f.get("created"),
            "updated": f.get("updated"),
            "customer": customer,
            "sprint": sprint,
            "story_points": story_points,
            "description": description,
            "labels": f.get("labels", []),
            "components": [c.get("name") for c in f.get("components", [])],
            "comments": [
                {
                    "author": c.get("author", {}).get("displayName"),
                    "body": c.get("body"),
                    "created": c.get("created")
                } for c in f.get("comment", {}).get("comments", [])
            ] if f.get("comment") else []
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
            INSERT INTO scrum_notes (date, team_id, member_id, member_name, ticket_key, comment, deadline, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (date, team_id, member_id, member_name, ticket_key, comment, deadline, data.get("tags", "")))
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
            SELECT id, date, team_id, member_id, member_name, ticket_key, comment, deadline, status, tags
            FROM scrum_notes
            WHERE date = ? AND team_id = ?
            ORDER BY member_name, created_at
        """, (date, team_id))
        rows = [{
            "id": r[0], "date": r[1], "team_id": r[2],
            "member_id": r[3], "member_name": r[4],
            "ticket_key": r[5], "comment": r[6],
            "deadline": r[7], "status": r[8], "tags": r[9]
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
        if "tags" in data:
            fields.append("tags = ?")
            params.append(data["tags"])
        if not fields:
            conn.close()
            return jsonify({"error": "No fields to update"}), 400
        params.append(note_id)
        cursor.execute(f"UPDATE scrum_notes SET {', '.join(fields)} WHERE id = ?", params)
        conn.commit()
        conn.close()
        return jsonify({"success": True})

@app.route("/api/scrum_notes/summary", methods=["GET"])
def scrum_notes_summary():
    """Fetch unique ticket keys worked on by a team/project in a date range."""
    start_date = request.args.get("start")
    end_date = request.args.get("end")
    team_id = request.args.get("team_id")
    
    if not start_date or not end_date:
        return jsonify({"error": "start and end dates are required"}), 400
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Logic: Get unique tickets from scrum_notes for the range, 
    # Aggregate member names, and pick the latest non-empty comment/tags
    # Note: Using subqueries to get the latest comment/tags for that specific key
    query = """
        SELECT ticket_key, 
               GROUP_CONCAT(DISTINCT member_name) as members,
               GROUP_CONCAT(DISTINCT member_id) as member_ids,
               (SELECT comment FROM scrum_notes sn2 WHERE sn2.ticket_key = sn1.ticket_key AND sn2.comment != '' ORDER BY date DESC, created_at DESC LIMIT 1) as latest_comment,
               (SELECT tags FROM scrum_notes sn2 WHERE sn2.ticket_key = sn1.ticket_key AND sn2.tags != '' ORDER BY date DESC, created_at DESC LIMIT 1) as latest_tags
        FROM scrum_notes sn1
        WHERE date >= ? AND date <= ?
    """
    params = [start_date, end_date]
    
    if team_id:
        query += " AND team_id = ?"
        params.append(team_id)
        
    query += " GROUP BY ticket_key"
    
    try:
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        
        results = []
        for r in rows:
            results.append({
                "ticket_key": r[0],
                "members": (r[1].split(",") if r[1] else []),
                "member_ids": (r[2].split(",") if r[2] else []),
                "comment": r[3] or "",
                "tags": r[4] or ""
            })
            
        conn.close()
        return jsonify(results)
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

@app.route("/api/scrum_notes/ticket/<key>", methods=["PUT"])
def scrum_note_by_ticket(key):
    """Update tags or comment for all notes of a specific ticket."""
    data = request.json
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    fields, params = [], []
    if "comment" in data:
        fields.append("comment = ?")
        params.append(data["comment"])
    if "tags" in data:
        fields.append("tags = ?")
        params.append(data["tags"])
        
    if not fields:
        conn.close()
        return jsonify({"error": "No fields to update"}), 400
        
    params.append(key)
    try:
        # Update the LATEST note for this ticket to preserve history but update the tracker view
        # The summary API picks the latest comment/tags, so we update the latest entry.
        cursor.execute(f"""
            UPDATE scrum_notes 
            SET {', '.join(fields)} 
            WHERE id = (SELECT id FROM scrum_notes WHERE ticket_key = ? ORDER BY date DESC, created_at DESC LIMIT 1)
        """, params)
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

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


@app.route("/work_report")
def work_report():
    return render_template("work_report.html", project=PROJECT_KEY)


@app.route("/api/reports/generate", methods=["POST"])
def generate_report_api():
    data = request.json
    team_name = data.get("team_name")
    sprint_name = data.get("sprint_name") or "N/A"
    from_date = data.get("from_date")
    to_date = data.get("to_date")
    team_members = data.get("team_members", "")
    ticket_text = data.get("tickets", "")
    report_format = data.get("format", "word")
    selected_cols = data.get("columns", ["key", "type", "summary", "status", "priority"])
    exclude_open = data.get("exclude_open", False)

    # Column Mapping for name and base widths
    col_config = {
        "key": {"name": "Key", "width": 0.8},
        "type": {"name": "Type", "width": 0.8},
        "summary": {"name": "Summary", "width": 2.0}, # Base/Min width
        "status": {"name": "Status", "width": 1.1},
        "priority": {"name": "Priority", "width": 0.8},
        "assignee": {"name": "Assignee", "width": 1.1},
        "customer": {"name": "Customer", "width": 1.2},
        "sprint": {"name": "Sprint", "width": 1.5}
    }

    # Filter config to only selected ones
    active_cols = [c for c in ["key", "type", "summary", "status", "priority", "assignee", "customer", "sprint"] if c in selected_cols]

    # Status priority for ordering
    top_statuses = [
        "DEPLOYED", "DONE", "NOT A BUG", "READY FOR STAGING", 
        "RESOLVED", "STAGED", "UNABLE TO REPRODUCE", 
        "READY FOR QA", "READY FOR INTERNAL DEMO"
    ]

    def get_status_rank(status_name):
        if not status_name: return 1
        name = status_name.upper()
        if name in top_statuses:
            return 0 # Higher priority (on top)
        return 1 # Lower priority (bottom)

    # Parse tickets
    raw_keys = re.findall(r'[A-Z]+-\d+', ticket_text.upper())
    ticket_keys = list(dict.fromkeys(raw_keys)) # Remove duplicates, preserve order
    if not ticket_keys:
        return jsonify({"error": "No valid ticket keys found"}), 400

    # Fetch Ticket Details from Jira
    bugs = []
    others = []
    
    bug_count = 0
    task_count = 0
    story_count = 0
    epic_count = 0
    other_count = 0

    headers_dict = dict(HEADERS)
    
    # Use JQL Search for efficiency and to filter by Team
    keys_str = ", ".join([f'"{k}"' for k in ticket_keys])
    # Matching user example: "team[team]" = UUID (no quotes)
    jql = f'key in ({keys_str}) AND "team[team]" = 4da67a24-33ef-42a2-b940-840dd6e450bc'
    
    try:
        # Jira Cloud v3 migration: use /search/jql endpoint
        params = {
            "jql": jql,
            "maxResults": len(ticket_keys),
            "fields": "issuetype,summary,priority,status,assignee,customfield_10077,customfield_10020"
        }
        search_res = requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params=params
        )
        
        if search_res.status_code == 200:
            found_issues = search_res.json().get("issues", [])
            for issue in found_issues:
                key = issue.get("key")
                fields = issue.get("fields", {})
                itype = fields.get("issuetype", {}).get("name", "Task")
                summary = fields.get("summary", "No Summary")
                priority = fields.get("priority", {}).get("name", "Medium")
                status = fields.get("status", {}).get("name", "Unknown")
                assignee_field = fields.get("assignee")
                assignee = assignee_field.get("displayName", "Unassigned") if assignee_field else "Unassigned"

                # Extract Customer
                customer = "N/A"
                cust_val = fields.get("customfield_10077")
                if cust_val:
                    if isinstance(cust_val, list) and len(cust_val) > 0:
                        v = cust_val[0]
                        customer = v.get("value") or v if isinstance(v, dict) else str(v)
                    elif isinstance(cust_val, dict):
                        customer = cust_val.get("value") or str(cust_val)
                    else:
                        customer = str(cust_val)

                # Extract Sprint
                sprint = "N/A"
                sprint_val = fields.get("customfield_10020")
                if sprint_val and isinstance(sprint_val, list) and len(sprint_val) > 0:
                    # Sprints are often returned as list of objects
                    sp = sprint_val[-1] # Take the latest one
                    sprint = sp.get("name") or str(sp)

                if exclude_open and status.lower() == "open":
                    continue

                ticket_obj = {
                    "key": key,
                    "type": itype,
                    "summary": summary,
                    "priority": priority,
                    "status": status,
                    "assignee": assignee,
                    "customer": customer,
                    "sprint": sprint,
                    "rank": get_status_rank(status) 
                }

                if itype.lower() == "bug":
                    bugs.append(ticket_obj)
                    bug_count += 1
                else:
                    others.append(ticket_obj)
                    if itype.lower() == "task": task_count += 1
                    elif itype.lower() == "story": story_count += 1
                    elif itype.lower() == "epic": epic_count += 1
                    else: other_count += 1
        else:
            print(f"Jira Search Error: {search_res.status_code} - {search_res.text}")
    except Exception as e:
        print(f"Error fetching issues via JQL: {e}")

    # Sort: Status rank (0 first), then by Key
    bugs.sort(key=lambda x: (x['rank'], x['key']))
    others.sort(key=lambda x: (x['rank'], x['key']))

    # Generate Chart
    plt.figure(figsize=(6, 4))
    labels = []
    sizes = []
    colors = []
    if bug_count: labels.append('Bugs'); sizes.append(bug_count); colors.append('#ef4444')
    if task_count: labels.append('Tasks'); sizes.append(task_count); colors.append('#3b82f6')
    if story_count: labels.append('Stories'); sizes.append(story_count); colors.append('#10b981')
    if epic_count: labels.append('Epics'); sizes.append(epic_count); colors.append('#8b5cf6')
    if other_count: labels.append('Others'); sizes.append(other_count); colors.append('#6b7280')

    chart_b64 = ""
    chart_stream = io.BytesIO()
    if sizes:
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=colors)
        plt.axis('equal')
        plt.title("Issue Distribution")
        plt.savefig(chart_stream, format='png', bbox_inches='tight')
        chart_b64 = base64.b64encode(chart_stream.getvalue()).decode()
    else:
        # Create a small blank white pixel so the PDF img tag doesn't break
        chart_b64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII="
    
    plt.close()

    if report_format == "word":
        doc = Document()
        
        # Header
        title = doc.add_heading(f"Executive Work Report: {team_name}", 0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run(f"Sprint: {sprint_name} | Period: {from_date} to {to_date}")
        run.italic = True
        
        doc.add_heading("Team Members", level=1)
        doc.add_paragraph(team_members)

        if sizes:
            doc.add_heading("Issue Distribution", level=1)
            doc.add_picture(chart_stream, width=Inches(4.5))
            last_p = doc.paragraphs[-1]
            last_p.alignment = WD_ALIGN_PARAGRAPH.CENTER

        # Function to add a table to doc
        def add_styled_table(data_list, title_text, cols):
            if not data_list:
                return
            doc.add_heading(title_text, level=1)
            table = doc.add_table(rows=1, cols=len(cols))
            table.style = 'Table Grid'
            
            # Header
            hdr_cells = table.rows[0].cells
            total_fixed_width = 0
            summary_idx = -1
            
            for i, col_id in enumerate(cols):
                hdr_cells[i].text = col_config[col_id]["name"]
                if col_id != "summary":
                    total_fixed_width += col_config[col_id]["width"]
                else:
                    summary_idx = i
            
            # Total width for standard Portrait is ~6.5 inches
            available_width = 6.4
            summary_width = max(2.0, available_width - total_fixed_width)
            
            for i, col_id in enumerate(cols):
                if col_id == "summary":
                    table.columns[i].width = Inches(summary_width)
                else:
                    table.columns[i].width = Inches(col_config[col_id]["width"])

            # Data
            for t in data_list:
                row_cells = table.add_row().cells
                for i, col_id in enumerate(cols):
                    row_cells[i].text = str(t.get(col_id, ""))

        # Tasks First
        add_styled_table(others, "Tasks, Stories & Epics", active_cols)
        # Bugs Second
        add_styled_table(bugs, "Bugs Summary", active_cols)

        output = io.BytesIO()
        doc.save(output)
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"Work_Report_{team_name.replace(' ', '_')}.docx")

    else: # PDF
        def g_html(data_list, cols):
            if not data_list: return f"<tr><td colspan='{len(cols)}' style='text-align:center'>No items.</td></tr>"
            html_rows = ""
            for t in data_list:
                html_rows += "<tr>"
                for c in cols:
                    val = t.get(c, "")
                    extra = " class='summary-col'" if c == "summary" else ""
                    html_rows += f"<td{extra}>{val}</td>"
                html_rows += "</tr>"
            return html_rows

        others_html = g_html(others, active_cols)
        bugs_html = g_html(bugs, active_cols)
        
        table_headers = "".join([f"<th>{col_config[c]['name']}</th>" for c in active_cols])
        
        # Calculate summary width purely for CSS
        fixed_count = len(active_cols) - (1 if "summary" in active_cols else 0)
        s_width = "40%" if fixed_count <= 4 else "30%"

        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Helvetica, Arial, sans-serif; color: #333; }}
                .header {{ text-align: center; border-bottom: 2px solid #3b82f6; padding-bottom: 10px; }}
                h1 {{ color: #1e3a8a; }}
                .meta {{ color: #666; font-style: italic; }}
                .section-title {{ background: #f3f4f6; padding: 5px 10px; border-left: 4px solid #3b82f6; margin-top: 20px; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 10px; table-layout: fixed; }}
                th, td {{ border: 1px solid #ddd; padding: 6px; text-align: left; font-size: 8pt; word-wrap: break-word; }}
                th {{ background-color: #f9fafb; font-weight: bold; }}
                .summary-col {{ width: {s_width}; }}
                td {{ width: auto; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Work Report: {team_name}</h1>
                <p class="meta">Sprint: {sprint_name} | {from_date} to {to_date}</p>
            </div>
            
            <div class="section-title">Team Members</div>
            <p>{team_members.replace('\\n', '<br>')}</p>
            
            <div class="section-title">Issue Distribution</div>
            <div style="text-align: center;">
                <img src="data:image/png;base64,{chart_b64}" width="400" />
            </div>
            
            <div class="section-title">Tasks, Stories & Epics</div>
            <table>
                <tr>{table_headers}</tr>
                {others_html}
            </table>
            
            <div class="section-title">Bugs Summary</div>
            <table>
                <tr>{table_headers}</tr>
                {bugs_html}
            </table>
        </body>
        </html>
        """
        
        output = io.BytesIO()
        pisa.CreatePDF(io.BytesIO(html_content.encode("utf-8")), dest=output)
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"Work_Report_{team_name.replace(' ', '_')}.pdf")

@app.route("/merge_pdf")
def merge_pdf_page():
    return render_template("merge_pdf.html")

@app.route("/api/pdf/merge", methods=["POST"])
def pdf_merge_api():
    if 'files' not in request.files:
        return jsonify({"error": "No files provided"}), 400
    
    files = request.files.getlist('files')
    output_name = request.form.get('output_name', 'merged_document')
    
    if len(files) < 2:
        return jsonify({"error": "At least 2 PDF files are required to merge"}), 400
    
    try:
        merger = PdfWriter()
        for pdf in files:
            merger.append(pdf)
        
        output = io.BytesIO()
        merger.write(output)
        merger.close()
        output.seek(0)
        
        return send_file(
            output, 
            as_attachment=True, 
            download_name=f"{output_name}.pdf",
            mimetype='application/pdf'
        )
    except Exception as e:
        return jsonify({"error": f"Merge failed: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(debug=True)
