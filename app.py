from flask import Flask, render_template, request, jsonify, make_response, has_request_context, redirect, url_for, send_file, session, abort
from werkzeug.local import LocalProxy
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from authlib.integrations.flask_client import OAuth
from functools import wraps
from dotenv import load_dotenv
import io
import re
import json
import os
import requests
import base64
import secrets
import mysql.connector
from mysql.connector import Error
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

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev_secret_key_change_in_production")

import time

# =========================
# SIMPLE IN-MEMORY CACHE
# =========================
JIRA_CACHE = {}
CACHE_TTL = 300 # 5 minutes default

def get_jira_cached(url, headers, params, ttl=CACHE_TTL, force_refresh=False):
    """
    Simple caching wrapper for Jira GET requests.
    Keys are based on URL + sorted params.
    """
    # Create a unique key for this request
    # Convert params to a sorted tuple of items to be hashable
    param_key = tuple(sorted(params.items())) if params else ()
    cache_key = (url, param_key)
    
    now = time.time()
    
    if not force_refresh and cache_key in JIRA_CACHE:
        timestamp, data = JIRA_CACHE[cache_key]
        if now - timestamp < ttl:
            print(f"DEBUG: Serving from cache: {url}")
            return data
            
    # Fetch fresh data
    try:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            JIRA_CACHE[cache_key] = (now, data)
            return data
        else:
            # If error, try to return stale cache if available, otherwise return error
            if cache_key in JIRA_CACHE:
                print(f"DEBUG: Fetch failed ({resp.status_code}), serving stale cache.")
                return JIRA_CACHE[cache_key][1]
            return resp.json() # Return the error response
    except Exception as e:
        if cache_key in JIRA_CACHE:
            print(f"DEBUG: Exception {e}, serving stale cache.")
            return JIRA_CACHE[cache_key][1]
        raise e

# =========================
# 🔐 AUTH & SECURITY SETUP
# =========================
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login_page"

oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get('GOOGLE_CLIENT_ID'),
    client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

# User Class
class User(UserMixin):
    def __init__(self, id, email, name, role_id, role_name, permissions, api_token):
        self.id = id
        self.email = email
        self.name = name
        self.role_id = role_id
        self.role_name = role_name
        self.permissions = permissions
        self.api_token = api_token
        
    def has_permission(self, perm):
        if self.role_name == 'Admin': return True
        return self.permissions.get(perm, False)

    def can_view_page(self, page_key):
        if self.role_name == 'Admin': return True
        # If 'allowed_pages' is not present, default to True (backward compatibility)
        allowed = self.permissions.get('allowed_pages')
        if allowed is None: return True 
        return page_key in allowed

    def to_dict(self):
        return {
            "id": self.id,
            "email": self.email,
            "name": self.name,
            "role_id": self.role_id,
            "role_name": self.role_name,
            "permissions": self.permissions
        }

@login_manager.user_loader
def load_user(user_id):
    conn, cursor = get_db_connection()
    if not conn: return None
    cursor.execute("""
        SELECT u.id, u.email, u.name, u.role_id, r.name, r.permissions, u.api_token
        FROM users u
        LEFT JOIN roles r ON u.role_id = r.id
        WHERE u.id = %s
    """, (user_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        perms = json.loads(row[5]) if row[5] else {}
        return User(id=row[0], email=row[1], name=row[2], role_id=row[3], role_name=row[4], permissions=perms, api_token=row[6])
    return None

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role_name != 'Admin':
            return abort(403)
        return f(*args, **kwargs)
    return decorated_function

def permission_required(permission):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return abort(403)
            # Admin has all permissions
            if current_user.role_name == 'Admin':
                return f(*args, **kwargs)
            # Check specific permission
            if current_user.has_permission(permission):
                return f(*args, **kwargs)
            return abort(403)
        return decorated_function
    return decorator

def page_permission_required(page_key):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('login_page'))
            if not current_user.can_view_page(page_key):
                return render_template("403.html"), 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator

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

def add_audit_log(page, item_key, field_name, new_value, old_value=None):
    """Adds a record to the audit_logs table."""
    conn, cursor = get_db_connection()
    if not conn:
        return False
    try:
        cursor.execute("""
            INSERT INTO audit_logs (user_id, user_name, page, item_key, field_name, old_value, new_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (current_user.id, current_user.name, page, item_key, field_name, str(old_value) if old_value is not None else None, str(new_value)))
        conn.commit()
        return True
    except Exception as e:
        print(f"Audit Log Error: {e}")
        return False
    finally:
        conn.close()

def _get_jira_config():
    """Fetch Jira config from DB. Returns (email, token, project_key, domain)."""
    try:
        conn, cursor = get_db_connection()
        if not conn: return "", "", "", DEFAULT_JIRA_DOMAIN
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
    conn, cursor = get_db_connection(dictionary=True)
    public_trackers = []
    header_title = "Jira Analytics"
    if conn:
        try:
            cursor.execute("SELECT id, name FROM trackers_v2 WHERE is_public = TRUE")
            public_trackers = cursor.fetchall()
            
            cursor.execute("SELECT config_value FROM app_config WHERE config_key = 'header_title'")
            row = cursor.fetchone()
            if row:
                header_title = row['config_value']
        except:
            pass
        finally:
            conn.close()
            
    return dict(
        JIRA_DOMAIN=str(JIRA_DOMAIN),
        project=str(PROJECT_KEY),
        public_trackers=public_trackers,
        header_title=header_title
    )

# Legacy auth (deprecated but kept for compatibility if used directly)
AUTH = base64.b64encode(f"{DEFAULT_JIRA_EMAIL}:{DEFAULT_JIRA_API_TOKEN}".encode()).decode()





# =========================
# DATABASE SETUP
# =========================
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'rohit',
    'password': 'Rohit',
    'database': 'rb_win'
}

def get_db_connection(dictionary=False):
    """Establish a connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if dictionary:
            # Equivalent to sqlite3.Row
            cursor = conn.cursor(dictionary=True)
        else:
            cursor = conn.cursor()
        return conn, cursor
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None, None

def init_db():
    # First, try to connect without a database to create it if it doesn't exist
    try:
        conn = mysql.connector.connect(
            host=MYSQL_CONFIG['host'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password']
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_CONFIG['database']}")
        conn.close()
    except Error as e:
        print(f"Error creating database: {e}")

    conn, cursor = get_db_connection()
    if not conn:
        return

    # Roles Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS roles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            permissions TEXT
        )
    ''')

    # Users Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255),
            google_id VARCHAR(255) UNIQUE,
            password_hash VARCHAR(255),
            role_id INT,
            api_token VARCHAR(255) UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (role_id) REFERENCES roles(id)
        )
    ''')

    # Seed Roles if empty
    cursor.execute("SELECT count(*) FROM roles")
    if cursor.fetchone()[0] == 0:
        cursor.execute("INSERT INTO roles (name, permissions) VALUES ('Admin', '{\"all\": true}')")
        cursor.execute("INSERT INTO roles (name, permissions) VALUES ('Editor', '{\"view\": true, \"edit\": true}')")
        cursor.execute("INSERT INTO roles (name, permissions) VALUES ('Viewer', '{\"view\": true, \"allowed_pages\": [\"dashboard\"]}')")
    else:
        # Ensure Viewer role is restricted to dashboard if it was previously seeded without allowed_pages
        cursor.execute("SELECT permissions FROM roles WHERE name = 'Viewer'")
        row = cursor.fetchone()
        if row:
            perms = json.loads(row[0]) if row[0] else {}
            if 'allowed_pages' not in perms:
                perms['allowed_pages'] = ['dashboard']
                cursor.execute("UPDATE roles SET permissions = %s WHERE name = 'Viewer'", (json.dumps(perms),))

    # Trackers Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trackers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Tickets Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracker_tickets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            tracker_id INT,
            issue_key VARCHAR(255) NOT NULL,
            comment TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (tracker_id) REFERENCES trackers(id) ON DELETE CASCADE
        )
    ''')
    # Todos Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS todos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT,
            title TEXT NOT NULL,
            description TEXT,
            priority VARCHAR(50) DEFAULT 'Low',
            due_date DATE NOT NULL,
            status VARCHAR(50) DEFAULT 'Pending',
            tags TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS todo_tags (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            color VARCHAR(50) DEFAULT 'blue'
        )
    ''')
    # Teams Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS teams (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Team Members Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_members (
            id INT AUTO_INCREMENT PRIMARY KEY,
            team_id INT,
            account_id VARCHAR(255) NOT NULL,
            display_name VARCHAR(255) NOT NULL,
            avatar_url TEXT,
            FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE
        )
    ''')
    # Sprints Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sprints (
            id INT AUTO_INCREMENT PRIMARY KEY,
            team_id INT,
            name VARCHAR(255) NOT NULL,
            state VARCHAR(50) DEFAULT 'active',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE
        )
    ''')
    # Sprint Weeks Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sprint_weeks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sprint_id INT,
            week_number INT NOT NULL,
            goal TEXT,
            FOREIGN KEY (sprint_id) REFERENCES sprints(id) ON DELETE CASCADE
        )
    ''')
    # Sprint Tickets Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sprint_tickets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sprint_id INT,
            week_id INT,
            issue_key VARCHAR(255) NOT NULL,
            comment TEXT,
            pr_raised INT DEFAULT 0,
            demo_done INT DEFAULT 0,
            pr_merged INT DEFAULT 0,
            deploy_status VARCHAR(50) DEFAULT 'N/A',
            qa_assignee VARCHAR(255) DEFAULT '',
            qa_status VARCHAR(50) DEFAULT 'Pending',
            bugs_found TEXT,
            requirements_clear VARCHAR(50) DEFAULT 'No',
            completed INT DEFAULT 0,
            is_flagged INT DEFAULT 0
        )
    ''')

    # Ensure is_flagged exists if table already exists
    try:
        cursor.execute("ALTER TABLE sprint_tickets ADD COLUMN is_flagged INT DEFAULT 0")
    except:
        pass

    # Drop legacy foreign keys that cause issues with Jira Sprint IDs
    try:
        cursor.execute("ALTER TABLE sprint_tickets DROP FOREIGN KEY sprint_tickets_ibfk_1")
    except:
        pass
    try:
        cursor.execute("ALTER TABLE sprint_tickets DROP FOREIGN KEY sprint_tickets_ibfk_2")
    except:
        pass

    # Scrum Notes Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scrum_notes (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            `date`      DATE    NOT NULL,
            team_id     INT NOT NULL,
            member_id   VARCHAR(255)    NOT NULL,
            member_name VARCHAR(255)    NOT NULL,
            ticket_key  VARCHAR(255)    NOT NULL,
            comment     TEXT,
            deadline    DATE,
            status      VARCHAR(50)    DEFAULT 'Pending',
            tags        TEXT,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE
        )
    ''')

    # Jira Config Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jira_config (
            id INT PRIMARY KEY,
            email VARCHAR(255),
            api_token TEXT,
            project_key VARCHAR(255),
            jira_domain VARCHAR(255)
        )
    ''')
    
    # Custom Reports Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS custom_reports (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            jql TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # --- DYNAMIC TRACKERS (V2) ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trackers_v2 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            jql TEXT NOT NULL,
            created_by INT,
            is_public BOOLEAN DEFAULT FALSE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracker_columns (
            id INT AUTO_INCREMENT PRIMARY KEY,
            tracker_id INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            column_type ENUM('text', 'checkbox', 'select', 'user', 'rca') NOT NULL,
            options TEXT,
            order_index INT DEFAULT 0,
            FOREIGN KEY (tracker_id) REFERENCES trackers_v2(id) ON DELETE CASCADE
        )
    ''')

    # Ensure rca is in the enum if table already exists
    try:
        cursor.execute("ALTER TABLE tracker_columns MODIFY COLUMN column_type ENUM('text', 'checkbox', 'select', 'user', 'rca') NOT NULL")
    except:
        pass

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracker_data_v2 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            tracker_id INT NOT NULL,
            issue_key VARCHAR(50) NOT NULL,
            column_id INT NOT NULL,
            value TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_cell (tracker_id, issue_key, column_id),
            FOREIGN KEY (tracker_id) REFERENCES trackers_v2(id) ON DELETE CASCADE,
            FOREIGN KEY (column_id) REFERENCES tracker_columns(id) ON DELETE CASCADE
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracker_rca (
            id INT AUTO_INCREMENT PRIMARY KEY,
            tracker_id INT NOT NULL,
            issue_key VARCHAR(50) NOT NULL,
            issue_details TEXT,
            rca_text TEXT,
            fix_text TEXT,
            prevention_text TEXT,
            token VARCHAR(100) UNIQUE,
            submitted_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_rca (tracker_id, issue_key),
            FOREIGN KEY (tracker_id) REFERENCES trackers_v2(id) ON DELETE CASCADE
        )
    ''')

    
    # App Config Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS app_config (
            config_key VARCHAR(255) PRIMARY KEY,
            config_value TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    ''')

    # Audit Logs Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT,
            user_name VARCHAR(255),
            page VARCHAR(100),
            item_key VARCHAR(255),
            field_name VARCHAR(255),
            old_value TEXT,
            new_value TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
        )
    ''')

    # Seed Default Header Title if empty
    cursor.execute("SELECT count(*) FROM app_config WHERE config_key = 'header_title'")
    if cursor.fetchone()[0] == 0:
        cursor.execute("INSERT INTO app_config (config_key, config_value) VALUES ('header_title', 'Jira Analytics')")

    conn.commit()
    conn.close()

init_db()

# =========================
# PAGE ROUTE
# =========================
@app.route("/settings")
@login_required
@page_permission_required("settings")
def settings():
    return render_template("settings.html", project=PROJECT_KEY)

@app.route("/api/settings/jira", methods=["GET", "POST"])
@login_required
def jira_settings_api():
    if request.method == "POST" and not current_user.has_permission("manage_settings") and current_user.role_name != 'Admin':
        return jsonify({"error": "Permission denied"}), 403
        
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
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
            VALUES (1, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                email=VALUES(email),
                api_token=VALUES(api_token),
                project_key=VALUES(project_key),
                jira_domain=VALUES(jira_domain)
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

@app.route("/api/settings/app_config", methods=["GET", "POST"])
@login_required
def app_config_settings_api():
    if request.method == "POST" and current_user.role_name != 'Admin':
        return jsonify({"error": "Permission denied"}), 403
        
    conn, cursor = get_db_connection(dictionary=True)
    if not conn: return jsonify({"error": "Database error"}), 500
    
    if request.method == "POST":
        data = request.json or {}
        header_title = data.get("header_title", "").strip()
        
        if not header_title:
            conn.close()
            return jsonify({"error": "Header title is required"}), 400
            
        try:
            cursor.execute("""
                INSERT INTO app_config (config_key, config_value)
                VALUES ('header_title', %s)
                ON DUPLICATE KEY UPDATE config_value = VALUES(config_value)
            """, (header_title,))
            conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            conn.close()
    else: # GET
        try:
            cursor.execute("SELECT config_key, config_value FROM app_config")
            rows = cursor.fetchall()
            config = {row['config_key']: row['config_value'] for row in rows}
            return jsonify(config)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            conn.close()

@app.route("/api/settings/todo_tags", methods=["GET", "POST"])
def todo_tags_settings_api():
    conn, cursor = get_db_connection(dictionary=True)
    if not conn: return jsonify({"error": "Database error"}), 500
    
    if request.method == "POST":
        data = request.json or {}
        name = (data.get("name") or "").strip()
        color = (data.get("color") or "blue").strip()
        
        if not name:
            conn.close()
            return jsonify({"error": "Tag name is required"}), 400
        
        cursor.execute("INSERT INTO todo_tags (name, color) VALUES (%s, %s)", (name, color))
        conn.commit()
        tag_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": tag_id, "name": name, "color": color})
        
    else:
        cursor.execute("SELECT id, name, color FROM todo_tags ORDER BY id ASC")
        # Use column names since we use dictionary=True
        tags = [{"id": r['id'], "name": r['name'], "color": r['color']} for r in cursor.fetchall()]
        conn.close()
        return jsonify(tags)

@app.route("/api/settings/todo_tags/<int:tag_id>", methods=["DELETE"])
@login_required
def delete_todo_tag(tag_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    cursor.execute("DELETE FROM todo_tags WHERE id = %s", (tag_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

# =========================
# 🔑 AUTH ROUTES
# =========================
@app.route("/login")
def login_page():
    conn, cursor = get_db_connection(dictionary=True)
    if not conn: return render_template("login.html", header_title="Reports Dashboard")
    
    cursor.execute("SELECT config_value FROM app_config WHERE config_key = 'header_title'")
    row = cursor.fetchone()
    conn.close()
    
    header_title = row['config_value'] if row else "Reports Dashboard"
    return render_template("login.html", header_title=header_title)

@app.route("/login/google")
def login_google():
    redirect_uri = url_for('auth_callback', _external=True)
    return google.authorize_redirect(redirect_uri)

@app.route("/auth/callback")
def auth_callback():
    try:
        token = google.authorize_access_token()
        user_info = google.parse_id_token(token, nonce=None)
        
        email = user_info.get('email')
        name = user_info.get('name')
        google_id = user_info.get('sub')
        
        conn, cursor = get_db_connection()
        if not conn: return "Database error", 500
        
        # Check if user exists
        cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
        row = cursor.fetchone()
        
        if row:
            user_id = row[0]
            # Update google_id if missing
            cursor.execute("UPDATE users SET google_id = %s, name = %s WHERE id = %s", (google_id, name, user_id))
        else:
            # Create new user
            # Assign 'Viewer' role by default
            cursor.execute("SELECT id FROM roles WHERE name = 'Viewer'")
            role_row = cursor.fetchone()
            role_id = role_row[0] if role_row else 3
            
            api_token = secrets.token_hex(32)
            cursor.execute("INSERT INTO users (email, name, google_id, role_id, api_token) VALUES (%s, %s, %s, %s, %s)",
                           (email, name, google_id, role_id, api_token))
            user_id = cursor.lastrowid
            
        conn.commit()
        conn.close()
        
        # Login the user
        user_obj = load_user(user_id)
        login_user(user_obj)
        
        return redirect(url_for('index'))
        
    except Exception as e:
        print(f"Auth Error: {e}")
        return f"Authentication failed: {e}", 400

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for('login_page'))

# =========================
# 👑 ADMIN ROUTES
# =========================
@app.route("/admin/users")
@admin_required
def admin_users():
    conn, cursor = get_db_connection(dictionary=True)
    if not conn: return jsonify({"error": "Database error"}), 500
    cursor.execute("""
        SELECT u.*, r.name as role_name 
        FROM users u 
        LEFT JOIN roles r ON u.role_id = r.id
        ORDER BY u.created_at DESC
    """)
    users = cursor.fetchall()
    
    cursor.execute("SELECT * FROM roles")
    roles = cursor.fetchall()
    conn.close()
    return render_template("admin_users.html", users=users, roles=roles)

@app.route("/api/admin/users/<int:user_id>/role", methods=["POST"])
@admin_required
def update_user_role(user_id):
    role_id = request.json.get("role_id")
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    cursor.execute("UPDATE users SET role_id = %s WHERE id = %s", (role_id, user_id))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

PAGE_PERMISSIONS = [
    {"key": "dashboard", "label": "Dashboard"},
    {"key": "scoreboard", "label": "Scoreboard"},
    {"key": "explorer", "label": "Explorer"},
    {"key": "custom_reports", "label": "Custom Reports"},
    {"key": "scrum_notes", "label": "Scrum Notes"},
    {"key": "work_report", "label": "Work Report"},
    {"key": "planning", "label": "Sprint Planning"},
    {"key": "teams", "label": "Teams"},
    {"key": "settings", "label": "Settings"},
    {"key": "todo", "label": "Todo List"},
    {"key": "tracker", "label": "Tracker"},
    {"key": "status_tracker", "label": "Status Tracker"},
    {"key": "trackers_v2", "label": "Custom Trackers"},
    {"key": "query_builder", "label": "Query Builder"},
    {"key": "bulk_update", "label": "Bulk Update"},
    {"key": "merge_pdf", "label": "Merge PDF"}
]

@app.route("/admin/roles")
@admin_required
def admin_roles():
    conn, cursor = get_db_connection(dictionary=True)
    if not conn: return jsonify({"error": "Database error"}), 500
    cursor.execute("SELECT * FROM roles ORDER BY id ASC")
    roles = []
    for row in cursor.fetchall():
        roles.append({
            "id": row["id"],
            "name": row["name"],
            "permissions": json.loads(row["permissions"]) if row["permissions"] else {}
        })
    conn.close()
    
    # Define available permissions for the UI
    available_permissions = [
        {"key": "view_reports", "label": "View Reports & Dashboards"},
        {"key": "create_tickets", "label": "Create Tickets/Todos"},
        {"key": "edit_tickets", "label": "Edit Tickets/Todos"},
        {"key": "delete_tickets", "label": "Delete Tickets/Todos"},
        {"key": "manage_settings", "label": "Manage Settings"},
        {"key": "manage_teams", "label": "Manage Teams"},
        {"key": "manage_trackers", "label": "Manage Custom Trackers (Create/Edit Structure)"},
        {"key": "edit_tracker_data", "label": "Edit Tracker Data (Update Status/Fields)"}
    ]
    
    return render_template("admin_roles.html", roles=roles, permissions=available_permissions, page_permissions=PAGE_PERMISSIONS)

@app.route("/api/admin/roles", methods=["POST"])
@admin_required
def create_role():
    name = request.json.get("name")
    if not name:
        return jsonify({"error": "Role name is required"}), 400
    
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    try:
        cursor.execute("INSERT INTO roles (name, permissions) VALUES (%s, '{}')", (name,))
        conn.commit()
        role_id = cursor.lastrowid
        conn.close()
        return jsonify({"success": True, "id": role_id, "name": name})
    except Error:
        if conn: conn.close()
        return jsonify({"error": "Role name already exists or database error"}), 400

@app.route("/api/admin/roles/<int:role_id>", methods=["PUT", "DELETE"])
@admin_required
def manage_role(role_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    if request.method == "DELETE":
        # Prevent deleting the last Admin role or system roles if necessary
        # For now, just checking ID 1 (Admin)
        if role_id == 1:
            conn.close()
            return jsonify({"error": "Cannot delete the default Admin role"}), 400
            
        cursor.execute("DELETE FROM roles WHERE id = %s", (role_id,))
        # Optional: Reset users with this role to Viewer (ID 3) or NULL
        cursor.execute("UPDATE users SET role_id = 3 WHERE role_id = %s", (role_id,))
        
        conn.commit()
        conn.close()
        return jsonify({"success": True})
        
    elif request.method == "PUT":
        data = request.json
        permissions = data.get("permissions") # Expecting a dict/object
        
        if permissions is not None:
            perm_json = json.dumps(permissions)
            cursor.execute("UPDATE roles SET permissions = %s WHERE id = %s", (perm_json, role_id))
            conn.commit()
            conn.close()
            return jsonify({"success": True})
            
        conn.close()
        return jsonify({"error": "No data provided"}), 400

@app.route("/")
@login_required
@page_permission_required("dashboard")
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
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
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
@page_permission_required("scoreboard")
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
    team_ids = data.get("team_id")
    sprint_id = data.get("sprint_id") # Optional Jira Sprint ID
    report_id = data.get("report_id")
    production_only = data.get("production_only", False)
    force_refresh = data.get("force_refresh", False)
    custom_jql = data.get("custom_jql") # NEW: Support for Custom Reports
    
    if not team_ids and not sprint_id and not custom_jql and not report_id:
        return jsonify({"error": "Team, Sprint, Report, or Custom JQL is required"}), 400
        
    if team_ids and not isinstance(team_ids, list):
        team_ids = [team_ids]

    members = []
    member_ids_str = ""
    
    # 1. Get Team Members for all selected teams (IF TEAMS SELECTED)
    if team_ids:
        conn, cursor = get_db_connection()
        if not conn: return jsonify({"error": "Database error"}), 500
        
        placeholders = ', '.join(['%s'] * len(team_ids))
        cursor.execute(f"SELECT account_id, display_name, avatar_url FROM team_members WHERE team_id IN ({placeholders})", tuple(team_ids))
        
        # Use a dictionary to ensure members are unique across multiple teams
        members_map = {}
        for r in cursor.fetchall():
            members_map[r[0]] = {"id": r[0], "name": r[1], "avatar": r[2]}
        
        members = list(members_map.values())
        conn.close()
        
        if not members:
            # If teams selected but no members, we might return empty or error?
            # Let's return error as "No members found for this team" implies configuration issue.
            return jsonify({"error": "No members found for this team"}), 400
            
        member_ids = [m["id"] for m in members]
        member_ids_str = ", ".join([f'"{mid}"' for mid in member_ids])
    
    headers_dict = dict(HEADERS)
    project_key_str = str(PROJECT_KEY)

    if report_id and not custom_jql:
        conn, cursor = get_db_connection()
        if not conn:
            return jsonify({"error": "Database error"}), 500
        cursor.execute("SELECT jql FROM custom_reports WHERE id = %s", (report_id,))
        report_row = cursor.fetchone()
        conn.close()
        if not report_row:
            return jsonify({"error": "Report not found"}), 404
        custom_jql = report_row[0]

    # 2. Build JQL
    if custom_jql:
        # If Custom JQL is provided, use it directly (but append Prod filter if needed)
        jql = custom_jql
        if production_only:
            jql = f'({jql}) AND "platform[checkboxes]" = PRODUCTION'
    else:
        # Standard logic
        # We must have project key
        jql = f'project = "{project_key_str}"'
        
        # Optional Sprint filter
        if sprint_id:
            jql += f' AND sprint = {sprint_id}'
        
        # Optional Assignee filter (Only if teams selected)
        if member_ids_str:
            jql += f' AND assignee IN ({member_ids_str})'
        
        # Optional Production filter
        if production_only:
            jql += ' AND "platform[checkboxes]" = PRODUCTION'
    
    try:
        res = get_jira_cached(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params={
                "jql": jql, 
                "maxResults": 1000, 
                "fields": "summary,status,priority,assignee,issuetype"
            },
            force_refresh=force_refresh
        )
        
        if "errorMessages" in res:
            return jsonify({"error": res["errorMessages"]}), 400
            
        all_issues = res.get("issues", [])
        
        # 3. Calculate Stats
        # A ticket is considered "done" if its status category is 'done' 
        # OR if it's in a status that implies development is complete (like Ready for QA)
        done_statuses = ["DONE", "RESOLVED", "DEPLOYED", "STAGED", "READY FOR QA", "READY FOR STAGING", "READY FOR INTERNAL DEMO", "CLOSED", "NOT A BUG", "UNABLE TO REPRODUCE"]
        
        done_issues = []
        active_issues = []
        for i in all_issues:
            status_name = i["fields"]["status"]["name"].upper()
            status_cat = i["fields"]["status"]["statusCategory"]["key"].lower()
            
            if status_cat == "done" or status_name in done_statuses:
                done_issues.append(i)
            else:
                active_issues.append(i)
        
        total_solved = len(done_issues)
        bugs_solved = len([i for i in done_issues if i["fields"]["issuetype"]["name"].lower() == "bug"])
        
        # User Performance
        # If we have members (Team selected), we calculate for them.
        # If NO team selected (Custom JQL or just Sprint), we calculate for ALL assignees found in the tickets.
        
        performance = {}
        
        if members:
            # Initialize with team members
            for m in members:
                performance[m["id"]] = {"name": m["name"], "avatar": m["avatar"], "solved": 0, "active": 0}
        
        for i in all_issues:
            assignee = i["fields"].get("assignee")
            if assignee:
                aid = assignee["accountId"]
                
                # If no specific team selected, add any assignee encountered
                if not members and aid not in performance:
                     performance[aid] = {
                         "name": assignee["displayName"], 
                         "avatar": assignee["avatarUrls"]["48x48"], 
                         "solved": 0, 
                         "active": 0
                     }
                
                if aid in performance:
                    status_name = i["fields"]["status"]["name"].upper()
                    status_cat = i["fields"]["status"]["statusCategory"]["key"].lower()
                    
                    if status_cat == "done" or status_name in done_statuses:
                        performance[aid]["solved"] += 1
                    else:
                        performance[aid]["active"] += 1
                        
        # Today's Work: Active tickets with their current status
        today_work = []
        for i in active_issues:
            today_work.append({
                "key": i["key"],
                "summary": i["fields"]["summary"],
                "assignee": (i["fields"].get("assignee") or {}).get("displayName", "Unassigned"),
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
        # If we have a sprint_id, we fetch data for that sprint.
        # If we have a custom_jql (no sprint_id), we might want to fetch data based on issue keys?
        # But our DB structure links tickets to sprint_id.
        # So for Custom Reports (which might span multiple sprints or have no sprint), 
        # local tracking data might be tricky if it depends on sprint_id.
        
        # Current DB Schema: sprint_tickets (sprint_id, issue_key, ...)
        # If we don't have a sprint_id, we can't easily fetch local data unless we fetch by issue_key.
        # Let's try to fetch by issue_key for the issues returned by JQL.
        
        tracking_data = {}
        issue_keys = [i["key"] for i in all_issues]
        
        if issue_keys:
            conn, cursor = get_db_connection(dictionary=True)
            if conn:
                # If we have a sprint_id, we can filter by it to be precise.
                # If not (Custom Report), we fetch the LATEST entry for that ticket? 
                # Or just any entry? A ticket might be in multiple sprints over time.
                # Ideally, we should show the data for the current context.
                # If custom report, maybe we just show the latest known status?
                
                if sprint_id:
                    cursor.execute("""
                        SELECT issue_key, pr_raised, pr_merged, deploy_status, qa_assignee, qa_status, bugs_found, requirements_clear, completed, is_flagged, comment
                        FROM sprint_tickets 
                        WHERE sprint_id = %s
                    """, (sprint_id,))
                else:
                    # For Custom Reports: Fetch data for these keys.
                    # Problem: A ticket might appear multiple times in sprint_tickets table (different sprints).
                    # We should probably pick the most recent one (highest sprint_id or created_at? we don't have created_at).
                    # Let's assume fetching by issue_key is "okay" and if multiple exist, we pick one (or latest).
                    # Actually, let's just fetch all matching keys and overwrite (so we get one of them).
                    # Or better: `WHERE issue_key IN (...)`
                    
                    placeholders = ', '.join(['%s'] * len(issue_keys))
                    cursor.execute(f"""
                        SELECT issue_key, pr_raised, pr_merged, deploy_status, qa_assignee, qa_status, bugs_found, requirements_clear, completed, is_flagged, comment
                        FROM sprint_tickets 
                        WHERE issue_key IN ({placeholders})
                        ORDER BY id DESC
                    """, tuple(issue_keys))
                    
                    # Note: This might return multiple rows per key. The dictionary comprehension below 
                    # will overwrite earlier ones with later ones (due to ORDER BY id DESC, later ones come first? No, wait).
                    # If we iterate, the last one processed wins. 
                    # If we ORDER BY id ASC, the last one (newest) wins.
                    
                rows = cursor.fetchall()
                # If using IN clause with multiple rows per key, we want the latest.
                # If we ORDER BY id ASC, the loop will set key -> val, then update key -> newer_val.
                # So existing logic works fine if we order correctly or just accept "a" value.
                
                for r in rows:
                    tracking_data[r["issue_key"]] = {
                        "pr_raised": bool(r["pr_raised"]),
                        "pr_merged": bool(r["pr_merged"]),
                        "deploy_status": r["deploy_status"],
                        "qa_assignee": r["qa_assignee"],
                        "qa_status": r["qa_status"],
                        "bugs_found": r["bugs_found"],
                        "requirements_clear": r["requirements_clear"] or "No",
                        "completed": bool(r["completed"]),
                        "is_flagged": bool(r["is_flagged"]),
                        "comment": r["comment"] or ""
                    }
                conn.close()

        # Merge local data into all issues for the tracking table
        tracking_issues = []
        extra_bug_keys = set()
        for i in all_issues:
            key = i["key"]
            local = tracking_data.get(key, {
                "pr_raised": False,
                "pr_merged": False,
                "deploy_status": "N/A",
                "qa_assignee": "",
                "qa_status": "Pending",
                "bugs_found": "",
                "requirements_clear": "No",
                "completed": False,
                "is_flagged": False,
                "comment": ""
            })
            
            # Collect bug keys to fetch their status later if they are not in all_issues
            if local["bugs_found"]:
                for b_key in local["bugs_found"].split(","):
                    b_key = b_key.strip()
                    if b_key and b_key not in [iss["key"] for iss in all_issues]:
                        extra_bug_keys.add(b_key)

            tracking_issues.append({
                "key": key,
                "summary": i["fields"]["summary"],
                "status": i["fields"]["status"]["name"],
                "status_category": i["fields"]["status"]["statusCategory"]["key"],
                "assignee": (i["fields"].get("assignee") or {}).get("displayName", "Unassigned"),
                "type_icon": i["fields"]["issuetype"].get("iconUrl"),
                "type_name": i["fields"]["issuetype"].get("name"),
                "local": local
            })

        # Fetch status for extra bugs that are not in the main issue list
        if extra_bug_keys:
            try:
                extra_jql = f"key IN ({','.join(extra_bug_keys)})"
                extra_res = get_jira_cached(
                    f"{JIRA_DOMAIN}/rest/api/3/search/jql",
                    headers=headers_dict,
                    params={
                        "jql": extra_jql,
                        "fields": "status"
                    },
                    force_refresh=force_refresh
                )
                
                for i in extra_res.get("issues", []):
                    tracking_issues.append({
                        "key": i["key"],
                        "status": i["fields"]["status"]["name"],
                        "status_category": i["fields"]["status"]["statusCategory"]["key"],
                        "is_extra": True # Mark as extra info for frontend map
                    })
            except Exception as e:
                print(f"Error fetching extra bug statuses: {e}")

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
@login_required
def update_sprint_ticket_field():
    if not current_user.has_permission("edit_tickets") and current_user.role_name != 'Admin':
        return jsonify({"error": "Permission denied"}), 403

    data = request.json
    sprint_id = data.get("sprint_id")
    issue_key = data.get("issue_key")
    field = data.get("field")
    value = data.get("value")
    
    if not sprint_id or not issue_key or not field:
        return jsonify({"error": "Missing required fields"}), 400
        
    # Security: whitelist fields
    allowed_fields = ['pr_raised', 'pr_merged', 'deploy_status', 'qa_assignee', 'qa_status', 'bugs_found', 'requirements_clear', 'completed', 'comment', 'demo_done', 'is_flagged']
    if field not in allowed_fields:
        return jsonify({"error": "Invalid field"}), 400
        
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    # Check if record exists
    cursor.execute(f"SELECT id, {field} FROM sprint_tickets WHERE sprint_id = %s AND issue_key = %s", (sprint_id, issue_key))
    row = cursor.fetchone()
    
    old_value = None
    if row:
        old_value = row[1]
        cursor.execute(f"UPDATE sprint_tickets SET {field} = %s WHERE id = %s", (value, row[0]))
    else:
        cursor.execute(f"INSERT INTO sprint_tickets (sprint_id, issue_key, {field}) VALUES (%s, %s, %s)", (sprint_id, issue_key, value))
        
    conn.commit()
    conn.close()

    # Add Audit Log
    add_audit_log(page="Dashboard", item_key=issue_key, field_name=field, new_value=value, old_value=old_value)

    return jsonify({"success": True})

@app.route("/api/dashboard/recently_added", methods=["POST"])
@login_required
def recently_added_tickets():
    data = request.json
    team_ids = data.get("team_id")
    sprint_id = data.get("sprint_id")
    filter_mode = data.get("filter_mode", "team")
    
    if not team_ids:
        return jsonify({"error": "Team is required"}), 400
        
    if not isinstance(team_ids, list):
        team_ids = [team_ids]

    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    placeholders = ', '.join(['%s'] * len(team_ids))
    cursor.execute(f"SELECT account_id FROM team_members WHERE team_id IN ({placeholders})", tuple(team_ids))
    member_ids = [r[0] for r in cursor.fetchall() if r and r[0]]
    conn.close()

    headers_dict = dict(HEADERS)
    project_key_str = str(PROJECT_KEY)
    parent_team_filter = 'AND "team[team]" = "4da67a24-33ef-42a2-b940-840dd6e450bc"'
    # JQL Construction
    # We rely on 'Sprint CHANGED AFTER -3d' as requested.
    # Note: 'Sprint' field name is standard.
    # We add 'Sprint IS NOT EMPTY' to ensure we only target tickets that actually have a sprint context.
    jql_primary = f'project = "{project_key_str}" {parent_team_filter} AND Sprint IS NOT EMPTY AND Sprint CHANGED AFTER "-3d" ORDER BY updated DESC'
    
    # Fallback to updated time ONLY if the primary Sprint query fails (e.g. Jira instance config issue)
    jql_fallback = f'project = "{project_key_str}" {parent_team_filter} AND updated >= "-3d" ORDER BY updated DESC'
    
    def fetch_jira(jql_query):
        return requests.get(
            f"{JIRA_DOMAIN}/rest/api/3/search/jql",
            headers=headers_dict,
            params={
                "jql": jql_query,
                "maxResults": 50,
                "fields": "summary,status,assignee,created,updated,issuetype,customfield_10020",
                "expand": "changelog"
            }
        )

    def parse_jira_datetime(raw):
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S.%f%z")
        except Exception:
            try:
                return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S%z")
            except Exception:
                return None

    try:
        print(f"DEBUG: Trying Primary JQL: {jql_primary}")
        res = fetch_jira(jql_primary)
        if res.status_code == 400 and jql_primary != jql_fallback:
            print(f"DEBUG: Primary JQL failed ({res.text}). Trying Fallback: {jql_fallback}")
            res = fetch_jira(jql_fallback)
            
        jira_payload = res.json()
        if "errorMessages" in jira_payload:
            return jsonify({"error": jira_payload["errorMessages"]}), 400
            
        issues = jira_payload.get("issues", [])
        cutoff = datetime.now(timezone.utc) - timedelta(days=3)
        formatted = []

        print(f"DEBUG: Processing {len(issues)} issues for recent activity...")
        
        for i in issues:
            f = i["fields"]
            sprints = f.get("customfield_10020") or []
            sprint_ids = []

            if isinstance(sprints, list):
                for s in sprints:
                    if isinstance(s, dict) and s.get("id") is not None:
                        try:
                            sprint_ids.append(int(s.get("id")))
                        except Exception:
                            pass
                    elif isinstance(s, str):
                        m = re.search(r"id=(\d+)", s)
                        if m:
                            sprint_ids.append(int(m.group(1)))

            # Check if ticket was CREATED recently (e.g. within cutoff)
            created_at = parse_jira_datetime(f.get("created"))
            is_newly_created = created_at and created_at >= cutoff
            
            sprint_changed_recently = False
            
            if is_newly_created:
                sprint_changed_recently = True
            else:
                # Check changelog for Sprint changes
                changelog = i.get("changelog") or {}
                histories = changelog.get("histories") or []
                for h in histories:
                    changed_at = parse_jira_datetime(h.get("created"))
                    if not changed_at or changed_at < cutoff:
                        continue
                    for item in h.get("items") or []:
                        field_name = (item.get("field") or "").lower()
                        field_id = (item.get("fieldId") or "").lower()
                        # Some instances use 'Sprint' with capital S
                        if field_name == "sprint" or field_id == "customfield_10020":
                            sprint_changed_recently = True
                            break
                    if sprint_changed_recently:
                        break

            if not sprint_changed_recently:
                continue

            assignee_obj = f.get("assignee") or {}
            formatted.append({
                "key": i["key"],
                "summary": f.get("summary"),
                "status": (f.get("status") or {}).get("name"),
                "assignee": assignee_obj.get("displayName"),
                "assignee_id": assignee_obj.get("accountId"),
                "created": f.get("updated"), # Return Updated time as 'created' for sorting
                "type_icon": (f.get("issuetype") or {}).get("iconUrl"),
                "sprint_ids": sprint_ids,
                "is_new": is_newly_created
            })

        print(f"DEBUG: Found {len(formatted)} matching recent tickets.")

        return jsonify({
            "tickets": formatted,
            "team_member_ids": member_ids,
            "sprint_id": sprint_id,
            "mode": filter_mode
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
@login_required
@page_permission_required("dashboard") # Assuming user details is part of dashboard access
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
@page_permission_required("custom_reports")
def reports_page():
    return render_template("custom_reports.html", project=PROJECT_KEY)

# =========================
# CUSTOM REPORTS API
# =========================

@app.route("/api/reports", methods=["GET", "POST"])
def manage_reports():
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    if request.method == "POST":
        data = request.json
        name = data.get("name")
        jql = data.get("jql")
        
        if not name or not jql:
            return jsonify({"error": "Name and JQL are required"}), 400
            
        cursor.execute("INSERT INTO custom_reports (name, jql) VALUES (%s, %s)", (name, jql))
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
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    if request.method == "GET":
        cursor.execute("SELECT id, name, jql, created_at FROM custom_reports WHERE id = %s", (report_id,))
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
            
        cursor.execute("UPDATE custom_reports SET name = %s, jql = %s WHERE id = %s", (name, jql, report_id))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
        
    elif request.method == "DELETE":
        cursor.execute("DELETE FROM custom_reports WHERE id = %s", (report_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})

# =========================
# 📊 CUSTOM TRACKERS (V2)
# =========================
@app.route("/trackers_v2")
@login_required
def trackers_v2_page():
    _, _, _, domain = _get_jira_config()
    return render_template("trackers_v2.html", jira_domain=domain)

@app.route("/api/trackers_v2", methods=["GET", "POST"])
@login_required
def manage_trackers_v2():
    if request.method == "POST" and current_user.role_name == 'Viewer':
        return jsonify({"error": "Permission denied"}), 403
        
    conn, cursor = get_db_connection(dictionary=True)
    if not conn: return jsonify({"error": "Database error"}), 500
    
    if request.method == "POST":
        data = request.json
        name = data.get("name")
        jql = data.get("jql")
        columns = data.get("columns", [])
        is_public = data.get("is_public", False)
        
        if not name or not jql:
            return jsonify({"error": "Name and JQL are required"}), 400
            
        try:
            # Create Tracker
            cursor.execute("""
                INSERT INTO trackers_v2 (name, jql, created_by, is_public)
                VALUES (%s, %s, %s, %s)
            """, (name, jql, current_user.id, is_public))
            tracker_id = cursor.lastrowid
            
            # Create Columns
            for idx, col in enumerate(columns):
                cursor.execute("""
                    INSERT INTO tracker_columns (tracker_id, name, column_type, options, order_index)
                    VALUES (%s, %s, %s, %s, %s)
                """, (tracker_id, col["name"], col["type"], json.dumps(col.get("options", [])), idx))
                
            conn.commit()
            return jsonify({"id": tracker_id, "success": True})
        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500
        finally:
            conn.close()
    
    else:  # GET - List all trackers
        cursor.execute("""
            SELECT t.*, u.name as creator_name 
            FROM trackers_v2 t
            LEFT JOIN users u ON t.created_by = u.id
            WHERE t.created_by = %s OR t.is_public = TRUE
            ORDER BY t.created_at DESC
        """, (current_user.id,))
        trackers = cursor.fetchall()
        conn.close()
        return jsonify(trackers)

@app.route("/api/trackers_v2/<int:tracker_id>", methods=["GET", "PUT", "DELETE"])
@login_required
def tracker_v2_detail(tracker_id):
    if request.method in ["PUT", "DELETE"] and current_user.role_name == 'Viewer':
        return jsonify({"error": "Permission denied"}), 403
        
    conn, cursor = get_db_connection(dictionary=True)
    if not conn: return jsonify({"error": "Database error"}), 500
    
    if request.method == "DELETE":
        # Check ownership
        cursor.execute("SELECT created_by FROM trackers_v2 WHERE id = %s", (tracker_id,))
        tracker = cursor.fetchone()
        if not tracker:
            conn.close()
            return jsonify({"error": "Tracker not found"}), 404
        if tracker["created_by"] != current_user.id and current_user.role_name != 'Admin':
            conn.close()
            return jsonify({"error": "Permission denied"}), 403
            
        cursor.execute("DELETE FROM trackers_v2 WHERE id = %s", (tracker_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
        
    elif request.method == "PUT":
        data = request.json
        try:
            # 1. Update Tracker Metadata
            cursor.execute("""
                UPDATE trackers_v2 
                SET name = %s, jql = %s, is_public = %s
                WHERE id = %s AND (created_by = %s OR %s = 'Admin')
            """, (data["name"], data["jql"], data["is_public"], tracker_id, current_user.id, current_user.role_name))
            
            # 2. Update Columns
            new_columns = data.get("columns", [])
            existing_col_ids = []
            
            for idx, col in enumerate(new_columns):
                col_name = col.get("name")
                col_type = col.get("type")
                col_options = json.dumps(col.get("options", []))
                col_id = col.get("id")
                
                if col_id:
                    # Update existing column
                    cursor.execute("""
                        UPDATE tracker_columns 
                        SET name = %s, column_type = %s, options = %s, order_index = %s
                        WHERE id = %s AND tracker_id = %s
                    """, (col_name, col_type, col_options, idx, col_id, tracker_id))
                    existing_col_ids.append(col_id)
                else:
                    # Insert new column
                    cursor.execute("""
                        INSERT INTO tracker_columns (tracker_id, name, column_type, options, order_index)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (tracker_id, col_name, col_type, col_options, idx))
                    existing_col_ids.append(cursor.lastrowid)
            
            # 3. Delete removed columns
            if existing_col_ids:
                format_strings = ','.join(['%s'] * len(existing_col_ids))
                cursor.execute(f"DELETE FROM tracker_columns WHERE tracker_id = %s AND id NOT IN ({format_strings})", [tracker_id] + existing_col_ids)
            else:
                cursor.execute("DELETE FROM tracker_columns WHERE tracker_id = %s", (tracker_id,))
                
            conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500
        finally:
            conn.close()

    else: # GET
        cursor.execute("SELECT * FROM trackers_v2 WHERE id = %s", (tracker_id,))
        tracker = cursor.fetchone()
        if not tracker:
            conn.close()
            return jsonify({"error": "Tracker not found"}), 404
            
        cursor.execute("SELECT * FROM tracker_columns WHERE tracker_id = %s ORDER BY order_index ASC", (tracker_id,))
        columns = cursor.fetchall()
        for col in columns:
            col["options"] = json.loads(col["options"]) if col["options"] else []
            
        conn.close()
        return jsonify({**tracker, "columns": columns})

@app.route("/api/trackers_v2/<int:tracker_id>/data", methods=["GET", "POST"])
@login_required
def tracker_v2_data(tracker_id):
    if request.method == "POST" and current_user.role_name == 'Viewer':
        return jsonify({"error": "Permission denied"}), 403
        
    if request.method == "POST":
        data = request.json
        issue_key = data.get("issue_key")
        column_id = data.get("column_id")
        value = data.get("value")
        
        if not all([issue_key, column_id]):
            return jsonify({"error": "Missing key data"}), 400
            
        conn, cursor = get_db_connection()
        if not conn: return jsonify({"error": "Database error"}), 500
        try:
            # Get old value and column name for audit log
            cursor.execute("SELECT name FROM tracker_columns WHERE id = %s", (column_id,))
            col_row = cursor.fetchone()
            col_name = col_row[0] if col_row else f"Column {column_id}"

            cursor.execute("SELECT value FROM tracker_data_v2 WHERE tracker_id = %s AND issue_key = %s AND column_id = %s", (tracker_id, issue_key, column_id))
            old_val_row = cursor.fetchone()
            old_value = old_val_row[0] if old_val_row else None

            cursor.execute("""
                INSERT INTO tracker_data_v2 (tracker_id, issue_key, column_id, value)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE value = VALUES(value)
            """, (tracker_id, issue_key, column_id, value))
            conn.commit()

            # Add Audit Log
            add_audit_log(page=f"Tracker #{tracker_id}", item_key=issue_key, field_name=col_name, new_value=value, old_value=old_value)

            return jsonify({"success": True})
        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500
        finally:
            conn.close()
            
    else: # GET - Fetch Jira Data + Stored Data
        conn, cursor = get_db_connection(dictionary=True)
        if not conn: return jsonify({"error": "Database error"}), 500
        
        try:
            cursor.execute("SELECT jql FROM trackers_v2 WHERE id = %s", (tracker_id,))
            tracker = cursor.fetchone()
            if not tracker:
                return jsonify({"error": "Tracker not found"}), 404
                
            # Fetch Local Data
            cursor.execute("SELECT issue_key, column_id, value FROM tracker_data_v2 WHERE tracker_id = %s", (tracker_id,))
            stored_rows = cursor.fetchall()
            stored_data = {}
            for row in stored_rows:
                if row["issue_key"] not in stored_data:
                    stored_data[row["issue_key"]] = {}
                stored_data[row["issue_key"]][row["column_id"]] = row["value"]

            # Fetch RCA status
            cursor.execute("SELECT issue_key, submitted_at FROM tracker_rca WHERE tracker_id = %s", (tracker_id,))
            rca_rows = cursor.fetchall()
            rca_status = {row["issue_key"]: (row["submitted_at"] is not None) for row in rca_rows}
                
        except Exception as e:
            return jsonify({"error": f"Database Error: {str(e)}"}), 500
        finally:
            conn.close()

        # Fetch Jira Data using JQL
        email, token, _, domain = _get_jira_config()
        if not email or not token:
            return jsonify({"error": "Jira credentials not configured"}), 400
            
        auth_str = f"{email}:{token}"
        auth_b64 = base64.b64encode(auth_str.encode()).decode()
        headers = {
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/json"
        }
        
        jira_issues = []
        try:
            params = {
                "jql": tracker["jql"],
                "maxResults": 100,
                "fields": "summary,status,assignee,priority,issuetype"
            }
            # Use /search/jql as required by latest Jira Cloud API
            res = requests.get(f"{domain.rstrip('/')}/rest/api/3/search/jql", headers=headers, params=params)
            
            if res.status_code == 200:
                issues_data = res.json().get("issues", [])
                for issue in issues_data:
                    key = issue["key"]
                    fields = issue.get("fields", {})
                    jira_issues.append({
                        "key": key,
                        "summary": fields.get("summary", "No Summary"),
                        "status": fields.get("status", {}).get("name", "Unknown"),
                        "status_category": fields.get("status", {}).get("statusCategory", {}).get("key", "new"),
                        "priority": fields.get("priority", {}).get("name", "Medium"),
                        "type": fields.get("issuetype", {}).get("name", "Task"),
                        "type_icon": fields.get("issuetype", {}).get("iconUrl"),
                        "assignee": fields.get("assignee", {}).get("displayName", "Unassigned") if fields.get("assignee") else "Unassigned",
                        "custom_data": stored_data.get(key, {}),
                        "rca_filled": rca_status.get(key, False)
                    })
            else:
                # Jira error (e.g. 400 Bad JQL)
                error_msg = res.json().get("errorMessages", [res.text])[0]
                return jsonify({"error": f"Jira API Error: {error_msg}"}), res.status_code
                
        except Exception as e:
            return jsonify({"error": f"Jira Connection Error: {str(e)}"}), 500
            
        return jsonify({
            "issues": jira_issues
        })

@app.route("/api/trackers_v2/<int:tracker_id>/rca/<issue_key>", methods=["GET", "POST"])
@login_required
def tracker_v2_rca(tracker_id, issue_key):
    conn, cursor = get_db_connection(dictionary=True)
    if not conn: return jsonify({"error": "Database error"}), 500
    
    if request.method == "POST":
        if current_user.role_name == 'Viewer':
            conn.close()
            return jsonify({"error": "Permission denied"}), 403
            
        data = request.json
        rca_text = data.get("rca_text")
        fix_text = data.get("fix_text")
        prevention_text = data.get("prevention_text")
        issue_details = data.get("issue_details") # JSON
        
        try:
            cursor.execute("""
                INSERT INTO tracker_rca (tracker_id, issue_key, issue_details, rca_text, fix_text, prevention_text, submitted_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE 
                    rca_text = VALUES(rca_text), 
                    fix_text = VALUES(fix_text), 
                    prevention_text = VALUES(prevention_text),
                    submitted_at = NOW()
            """, (tracker_id, issue_key, json.dumps(issue_details) if issue_details else None, rca_text, fix_text, prevention_text))
            conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            conn.close()
    
    else: # GET
        cursor.execute("SELECT * FROM tracker_rca WHERE tracker_id = %s AND issue_key = %s", (tracker_id, issue_key))
        row = cursor.fetchone()
        conn.close()
        return jsonify(row if row else {})

@app.route("/api/trackers_v2/<int:tracker_id>/rca/link", methods=["POST"])
@login_required
def tracker_v2_rca_link(tracker_id, issue_key=None):
    if current_user.role_name == 'Viewer':
        return jsonify({"error": "Permission denied"}), 403
        
    data = request.json
    issue_key = data.get("issue_key")
    issue_details = data.get("issue_details") # Dict
    
    if not issue_key:
        return jsonify({"error": "Issue key is required"}), 400
        
    token = secrets.token_urlsafe(32)
    
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    try:
        cursor.execute("""
            INSERT INTO tracker_rca (tracker_id, issue_key, issue_details, token)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE token = VALUES(token), issue_details = VALUES(issue_details)
        """, (tracker_id, issue_key, json.dumps(issue_details), token))
        conn.commit()
        
        base_url = request.url_root.rstrip('/')
        link = f"{base_url}/rca/form/{token}"
        return jsonify({"link": link})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# Public RCA Form Route
@app.route("/rca/form/<token>")
def rca_public_form(token):
    conn, cursor = get_db_connection(dictionary=True)
    if not conn: return "Database Error", 500
    
    cursor.execute("SELECT * FROM tracker_rca WHERE token = %s", (token,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return "Invalid or expired link", 404
        
    issue = json.loads(row["issue_details"]) if row["issue_details"] else {"key": row["issue_key"]}
    return render_template("rca_form.html", token=token, issue=issue, rca=row, jira_domain=JIRA_DOMAIN)

# Public API for form submission
@app.route("/api/rca/submit/<token>", methods=["POST"])
def rca_public_submit(token):
    data = request.json
    rca_text = data.get("rca_text")
    fix_text = data.get("fix_text")
    prevention_text = data.get("prevention_text")
    
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    try:
        cursor.execute("""
            UPDATE tracker_rca 
            SET rca_text = %s, fix_text = %s, prevention_text = %s, submitted_at = NOW()
            WHERE token = %s
        """, (rca_text, fix_text, prevention_text, token))
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route("/tracker")
@page_permission_required("tracker")
def tracker():
    return render_template("tracker.html", project=PROJECT_KEY)

@app.route("/report_view")
@login_required
@page_permission_required("custom_reports")
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
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    if request.method == "POST":
        title = request.json.get("title")
        if not title:
            return jsonify({"error": "Title is required"}), 400
        cursor.execute("INSERT INTO trackers (title) VALUES (%s)", (title,))
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
@permission_required("manage_settings")
def delete_tracker(tracker_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    cursor.execute("DELETE FROM trackers WHERE id = %s", (tracker_id,))
    cursor.execute("DELETE FROM tracker_tickets WHERE tracker_id = %s", (tracker_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/api/trackers/<int:tracker_id>/tickets", methods=["GET", "POST"])
def tracker_tickets(tracker_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    if request.method == "POST":
        issue_key = request.json.get("issueKey")
        if not issue_key:
            return jsonify({"error": "Issue key is required"}), 400
        cursor.execute("INSERT INTO tracker_tickets (tracker_id, issue_key) VALUES (%s, %s)", (tracker_id, issue_key))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    
    else:
        cursor.execute("SELECT issue_key, comment FROM tracker_tickets WHERE tracker_id = %s", (tracker_id,))
        tickets = [{"issue_key": r[0], "comment": r[1]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(tickets)

@app.route("/api/trackers/<int:tracker_id>/tickets/<string:issue_key>", methods=["DELETE"])
def delete_tracker_ticket(tracker_id, issue_key):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    cursor.execute("DELETE FROM tracker_tickets WHERE tracker_id = %s AND issue_key = %s", (tracker_id, issue_key))
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
        
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    cursor.execute("UPDATE tracker_tickets SET comment = %s WHERE tracker_id = %s AND issue_key = %s", (comment, tracker_id, issue_key))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

# =========================
# TODO API
# =========================

@app.route("/todo")
@page_permission_required("todo")
def todo_page():
    return render_template("todo.html", project=PROJECT_KEY)

@app.route("/api/todos", methods=["GET", "POST"])
@login_required
def manage_todos():
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
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
            INSERT INTO todos (user_id, title, description, priority, due_date, tags)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (current_user.id, title, description, priority, due_date, tags))
        conn.commit()
        todo_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": todo_id, "title": title, "status": "Pending"})
    
    else:
        date_filter = request.args.get("date")
        if date_filter:
            cursor.execute("SELECT id, title, description, priority, due_date, status, tags FROM todos WHERE user_id = %s AND due_date = %s ORDER BY created_at DESC", (current_user.id, date_filter,))
        else:
            cursor.execute("SELECT id, title, description, priority, due_date, status, tags FROM todos WHERE user_id = %s ORDER BY due_date ASC, created_at DESC", (current_user.id,))
            
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
@login_required
def update_delete_todo(todo_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    # Check if the todo exists and belongs to the user (unless they are Admin)
    cursor.execute("SELECT user_id FROM todos WHERE id = %s", (todo_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Todo not found"}), 404
    
    todo_user_id = row[0]
    is_admin = current_user.role_name == 'Admin'
    
    if todo_user_id != current_user.id and not is_admin:
        conn.close()
        return jsonify({"error": "Permission denied"}), 403

    if request.method == "DELETE":
        if not current_user.has_permission("delete_tickets") and not is_admin and todo_user_id != current_user.id:
            conn.close()
            return jsonify({"error": "Permission denied"}), 403
            
        cursor.execute("DELETE FROM todos WHERE id = %s", (todo_id,))
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
            update_fields.append("status = %s")
            params.append(status)
        if title:
            update_fields.append("title = %s")
            params.append(title)
        if description is not None:
            update_fields.append("description = %s")
            params.append(description)
        if priority:
            update_fields.append("priority = %s")
            params.append(priority)
        if due_date:
            update_fields.append("due_date = %s")
            params.append(due_date)
        if tags is not None:
            update_fields.append("tags = %s")
            params.append(tags)
            
        if update_fields:
            params.append(todo_id)
            cursor.execute(f"UPDATE todos SET {', '.join(update_fields)} WHERE id = %s", params)
            conn.commit()
            
        conn.close()
        return jsonify({"success": True})

# =========================
# EXPLORER API
# =========================

@app.route("/explorer")
@page_permission_required("explorer")
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
@page_permission_required("query_builder")
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
@page_permission_required("bulk_update")
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
    
    query = request.args.get("q", "").lower()
    
    # NEW APPROACH (Optimized & Comprehensive):
    # 1. We prioritize "Scrum" boards and check ALL of them (not just top 2/5).
    # 2. We fetch Active/Future sprints FIRST.
    # 3. If query is present, we ALSO check closed sprints (limited history).
    # 4. We use aggressive caching to make it fast.

    try:
        # 1. Find ALL Boards
        # Cache this for a long time (1 hour) as boards rarely change
        board_res = get_jira_cached(
            f"{JIRA_DOMAIN}/rest/agile/1.0/board",
            headers=headers_dict,
            params={"projectKeyOrId": project_key_str},
            ttl=3600 
        )
        
        boards = board_res.get("values", [])
        if not boards:
             return get_sprints_fallback(query, headers_dict, project_key_str)

        # Filter: Only Scrum boards are relevant for sprints usually
        # But some projects might mix types, so let's keep others but deprioritize them
        boards.sort(key=lambda b: 0 if b.get("type") == "scrum" else 1)
        
        # User requested: "dont search for payroll board" (or specifically avoid hardcoding it?)
        # Wait, previous request was "only need sprint from payroll board".
        # Current request: "dont search for payroll board show app now" -> "don't search for payroll board SPECIFICALLY, show ALL now"?
        # OR "don't search for payroll board, show APP now"?
        # Re-reading: "once user login make a call a cache record for sprint and dont search for payroll board show app now"
        # It sounds like: "Pre-fetch/cache sprints on login so it's fast, and stop the specific 'payroll board' filter I asked for earlier."
        
        # So I will REMOVE the payroll-specific filter and search ALL boards.
        
        target_boards = boards # Search ALL boards (or maybe top 10 to be safe from timeout)
        if len(target_boards) > 10:
            target_boards = target_boards[:10]

        all_sprints_map = {}
        
        for board in target_boards:
            board_id = board.get("id")
            sprints_url = f"{JIRA_DOMAIN}/rest/agile/1.0/board/{board_id}/sprint"
            
            # Fetch ALL Active and Future sprints
            # We page through results to ensure we get every single one
            try:
                start_at_sprint = 0
                max_results_sprint = 50
                
                while True:
                    # Cache Key needs to include board_id and startAt
                    # TTL 5 mins for active/future lists is reasonable
                    res = get_jira_cached(
                        sprints_url,
                        headers=headers_dict,
                        params={
                            "state": "active,future",
                            "startAt": start_at_sprint,
                            "maxResults": max_results_sprint
                        },
                        ttl=300 
                    )
                    
                    sprints_page = res.get("values", [])
                    if not sprints_page:
                        break
                        
                    for s in sprints_page:
                        s_id = s.get("id")
                        if s_id in all_sprints_map: continue
                        s_name = s.get("name", "")
                        
                        if query and query not in s_name.lower() and query not in str(s_id):
                            continue
                            
                        all_sprints_map[s_id] = {"id": s_id, "name": s_name, "state": s.get("state")}
                    
                    if res.get("isLast"):
                        break
                    start_at_sprint += len(sprints_page)
                    
            except Exception as e:
                print(f"DEBUG: Failed to fetch sprints for board {board_id}: {e}")

            # If searching for specific sprint (query present) and found nothing yet, check CLOSED sprints
            # Limit to last 50 closed sprints per board to avoid slowness
            if query and len(all_sprints_map) == 0:
                 try:
                     res_closed = get_jira_cached(
                        sprints_url,
                        headers=headers_dict,
                        params={
                            "state": "closed",
                            "maxResults": 50 
                        },
                        ttl=600 # Cache closed sprints for 10 mins
                    )
                     for s in res_closed.get("values", []):
                        s_id = s.get("id")
                        if s_id in all_sprints_map: continue
                        s_name = s.get("name", "")
                        if query in s_name.lower() or query in str(s_id):
                            all_sprints_map[s_id] = {"id": s_id, "name": s_name, "state": s.get("state")}
                 except Exception:
                     pass

        all_sprints = list(all_sprints_map.values())
        
        # Sort: Active first, then Future, then Closed (descending ID)
        state_order = {"active": 0, "future": 1, "closed": 2}
        all_sprints.sort(key=lambda x: (state_order.get(x["state"], 3), -x["id"]))
        
        return jsonify(all_sprints)
        
    except Exception as e:
        print(f"Error fetching sprints via Board API: {e}")
        return get_sprints_fallback(query, headers_dict, project_key_str)

def get_sprints_fallback(query, headers_dict, project_key_str):
    # Use JQL and paginate through results to collect all sprint references
    jql = f'project = "{project_key_str}"'
    url = f"{JIRA_DOMAIN}/rest/api/3/search/jql"
    
    try:
        all_sprints_map = {}
        start_at = 0
        max_results = 100

        while True:
            # Limit scan to recent 500 issues to avoid timeouts
            if start_at > 500: break
            
            params = {
                "jql": jql,
                "maxResults": max_results,
                "startAt": start_at,
                "fields": "customfield_10020"
            }
            res = requests.get(url, headers=headers_dict, params=params)
            if res.status_code != 200:
                return jsonify({"error": f"Jira error: {res.text}"}), res.status_code

            data = res.json()
            issues = data.get("issues", [])
            if not issues:
                break

            for issue in issues:
                sprints = issue["fields"].get("customfield_10020")
                if sprints and isinstance(sprints, list):
                    for s in sprints:
                        s_id = s.get("id")
                        s_name = s.get("name", "")
                        s_state = s.get("state", "unknown")
                        if not s_id:
                            continue
                        if query and query not in s_name.lower() and query not in str(s_id):
                            continue
                        all_sprints_map[s_id] = {
                            "id": s_id,
                            "name": s_name,
                            "state": s_state
                        }

            total = data.get("total", 0)
            start_at += len(issues)
            if start_at >= total:
                break
        
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
@page_permission_required("teams")
def teams_page():
    return render_template("teams.html", project=PROJECT_KEY)

@app.route("/api/teams", methods=["GET", "POST"])
@login_required
def manage_teams():
    if request.method == "POST" and not current_user.has_permission("manage_teams") and current_user.role_name != 'Admin':
        return jsonify({"error": "Permission denied"}), 403
        
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    if request.method == "POST":
        name = request.json.get("name")
        if not name:
            return jsonify({"error": "Team name is required"}), 400
        cursor.execute("INSERT INTO teams (name) VALUES (%s)", (name,))
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
@permission_required("manage_teams")
def delete_team(team_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    cursor.execute("DELETE FROM teams WHERE id = %s", (team_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/api/teams/<int:team_id>/members", methods=["GET", "POST"])
def team_members(team_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    if request.method == "POST":
        data = request.json
        account_id = data.get("accountId")
        display_name = data.get("displayName")
        avatar_url = data.get("avatarUrl")
        if not account_id or not display_name:
            return jsonify({"error": "Account ID and Display Name are required"}), 400
        cursor.execute("INSERT INTO team_members (team_id, account_id, display_name, avatar_url) VALUES (%s, %s, %s, %s)",
                       (team_id, account_id, display_name, avatar_url))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    else:
        cursor.execute("SELECT id, account_id, display_name, avatar_url FROM team_members WHERE team_id = %s", (team_id,))
        members = [
            {"id": r[0], "accountId": r[1], "account_id": r[1], "displayName": r[2], "avatarUrl": r[3]}
            for r in cursor.fetchall()
        ]
        conn.close()
        return jsonify(members)

@app.route("/api/teams/<int:team_id>/members/<int:member_id>", methods=["DELETE"])
def delete_team_member(team_id, member_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    cursor.execute("DELETE FROM team_members WHERE id = %s AND team_id = %s", (member_id, team_id))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

# =========================
# SPRINT PLANNING API
# =========================

@app.route("/planning/<int:team_id>")
@page_permission_required("planning")
def planning_page(team_id):
    return render_template("planning.html", project=PROJECT_KEY, team_id=team_id)

@app.route("/api/teams/<int:team_id>/sprints", methods=["GET", "POST"])
def team_sprints(team_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    if request.method == "POST":
        name = request.json.get("name")
        if not name:
            return jsonify({"error": "Sprint name is required"}), 400
        cursor.execute("INSERT INTO sprints (team_id, name) VALUES (%s, %s)", (team_id, name))
        conn.commit()
        sprint_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": sprint_id, "name": name})
    else:
        cursor.execute("SELECT id, name, state, created_at FROM sprints WHERE team_id = %s ORDER BY created_at DESC", (team_id,))
        sprints = [{"id": r[0], "name": r[1], "state": r[2], "created_at": r[3]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(sprints)

@app.route("/api/sprints/<int:sprint_id>/weeks", methods=["GET", "POST"])
def sprint_weeks(sprint_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    if request.method == "POST":
        data = request.json
        week_number = data.get("weekNumber")
        goal = data.get("goal", "")
        cursor.execute("INSERT INTO sprint_weeks (sprint_id, week_number, goal) VALUES (%s, %s, %s)",
                       (sprint_id, week_number, goal))
        conn.commit()
        week_id = cursor.lastrowid
        conn.close()
        return jsonify({"id": week_id, "weekNumber": week_number})
    else:
        cursor.execute("SELECT id, week_number, goal FROM sprint_weeks WHERE sprint_id = %s ORDER BY week_number ASC", (sprint_id,))
        weeks = [{"id": r[0], "weekNumber": r[1], "goal": r[2]} for r in cursor.fetchall()]
        conn.close()
        return jsonify(weeks)

@app.route("/api/sprint_weeks/<int:week_id>", methods=["PUT", "DELETE"])
def manage_sprint_week(week_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    if request.method == "DELETE":
        cursor.execute("DELETE FROM sprint_weeks WHERE id = %s", (week_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    elif request.method == "PUT":
        data = request.json
        goal = data.get("goal")
        cursor.execute("UPDATE sprint_weeks SET goal = %s WHERE id = %s", (goal, week_id))
        conn.commit()
        conn.close()
        return jsonify({"success": True})

@app.route("/api/sprints/<int:sprint_id>/tickets", methods=["GET", "POST"])
def sprint_tickets_api(sprint_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    if request.method == "POST":
        data = request.json
        week_id = data.get("weekId")
        issue_key = data.get("issueKey")
        if not issue_key:
            return jsonify({"error": "Issue key is required"}), 400
        cursor.execute("INSERT INTO sprint_tickets (sprint_id, week_id, issue_key) VALUES (%s, %s, %s)",
                       (sprint_id, week_id, issue_key))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    else:
        cursor.execute("SELECT id, week_id, issue_key, comment, pr_raised, demo_done, pr_merged FROM sprint_tickets WHERE sprint_id = %s", (sprint_id,))
        tickets = [{"id": r[0], "weekId": r[1], "issueKey": r[2], "comment": r[3], "prRaised": bool(r[4]), "demoDone": bool(r[5]), "prMerged": bool(r[6])} for r in cursor.fetchall()]
        conn.close()
        return jsonify(tickets)

@app.route("/api/sprint_tickets/<int:ticket_id>", methods=["PUT", "DELETE"])
def manage_sprint_ticket(ticket_id):
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    if request.method == "DELETE":
        cursor.execute("DELETE FROM sprint_tickets WHERE id = %s", (ticket_id,))
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
        fields = ["comment = %s", "pr_raised = %s", "demo_done = %s", "pr_merged = %s"]
        params = [comment, pr_raised, demo_done, pr_merged]
        
        if week_id is not None:
            fields.append("week_id = %s")
            params.append(week_id)
            
        params.append(ticket_id)
        
        cursor.execute(f"""
            UPDATE sprint_tickets 
            SET {', '.join(fields)}
            WHERE id = %s
        """, params)
        
        conn.commit()
        conn.close()
        return jsonify({"success": True})

@app.route("/status_tracker")
@page_permission_required("status_tracker")
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
@page_permission_required("scrum_notes")
def scrum_notes_page():
    resp = make_response(render_template("scrum_notes.html", project=PROJECT_KEY, jira_domain=JIRA_DOMAIN))
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
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500

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
                WHERE team_id = %s
                  AND lower(trim(display_name)) = lower(trim(%s))
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
            WHERE date = %s AND team_id = %s
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
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500

    if request.method == "DELETE":
        cursor.execute("DELETE FROM scrum_notes WHERE id = %s", (note_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})

    else:  # PUT
        data = request.json
        fields, params = [], []
        if "comment" in data:
            fields.append("comment = %s")
            params.append(data["comment"])
        if "deadline" in data:
            fields.append("deadline = %s")
            params.append(data["deadline"] or None)
        if "status" in data:
            fields.append("status = %s")
            params.append(data["status"])
        if "tags" in data:
            fields.append("tags = %s")
            params.append(data["tags"])
        if not fields:
            conn.close()
            return jsonify({"error": "No fields to update"}), 400
        params.append(note_id)
        cursor.execute(f"UPDATE scrum_notes SET {', '.join(fields)} WHERE id = %s", params)
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
        
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    # Logic: Get unique tickets from scrum_notes for the range, 
    # Aggregate member names, and pick the latest non-empty comment/tags
    query = """
        SELECT ticket_key, 
               GROUP_CONCAT(DISTINCT member_name) as members,
               GROUP_CONCAT(DISTINCT member_id) as member_ids
        FROM scrum_notes
        WHERE date >= %s AND date <= %s
    """
    params = [start_date, end_date]
    
    if team_id:
        query += " AND team_id = %s"
        params.append(team_id)
        
    query += " GROUP BY ticket_key"
    
    try:
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        
        results = []
        for r in rows:
            # For each ticket, fetch the latest non-empty comment and tags
            # MySQL doesn't support the subquery in the same way SQLite did in the SELECT list easily without performance issues or specific syntax, 
            # but we can do it separately or use a join. For simplicity, we'll do it separately or keep the subquery if MySQL likes it.
            # MySQL supports subqueries in SELECT.
            
            cursor.execute("SELECT comment FROM scrum_notes WHERE ticket_key = %s AND comment != '' ORDER BY date DESC, created_at DESC LIMIT 1", (r[0],))
            c_row = cursor.fetchone()
            latest_comment = c_row[0] if c_row else ""
            
            cursor.execute("SELECT tags FROM scrum_notes WHERE ticket_key = %s AND tags != '' ORDER BY date DESC, created_at DESC LIMIT 1", (r[0],))
            t_row = cursor.fetchone()
            latest_tags = t_row[0] if t_row else ""

            results.append({
                "ticket_key": r[0],
                "members": (r[1].split(",") if r[1] else []),
                "member_ids": (r[2].split(",") if r[2] else []),
                "comment": latest_comment,
                "tags": latest_tags
            })
            
        conn.close()
        return jsonify(results)
    except Exception as e:
        if conn: conn.close()
        return jsonify({"error": str(e)}), 500

@app.route("/api/scrum_notes/ticket/<key>", methods=["PUT"])
def scrum_note_by_ticket(key):
    """Update tags or comment for all notes of a specific ticket."""
    data = request.json
    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
    
    fields, params = [], []
    if "comment" in data:
        fields.append("comment = %s")
        params.append(data["comment"])
    if "tags" in data:
        fields.append("tags = %s")
        params.append(data["tags"])
        
    if not fields:
        conn.close()
        return jsonify({"error": "No fields to update"}), 400
        
    try:
        # Update the LATEST note for this ticket
        # MySQL syntax for UPDATE with subquery on same table is tricky, using a temporary table or joining.
        # Simplest: Get the ID first.
        cursor.execute("SELECT id FROM scrum_notes WHERE ticket_key = %s ORDER BY date DESC, created_at DESC LIMIT 1", (key,))
        row = cursor.fetchone()
        if row:
            note_id = row[0]
            params.append(note_id)
            cursor.execute(f"UPDATE scrum_notes SET {', '.join(fields)} WHERE id = %s", params)
            conn.commit()
        
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        if conn: conn.close()
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
        WHERE date BETWEEN %s AND %s
    """
    params = [start, end]

    if team_id:
        query += " AND team_id = %s"
        params.append(team_id)
    if member_id:
        query += " AND member_id = %s"
        params.append(member_id)

    query += " ORDER BY date DESC, member_name, ticket_key"

    conn, cursor = get_db_connection()
    if not conn: return jsonify({"error": "Database error"}), 500
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
@page_permission_required("work_report")
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
@page_permission_required("merge_pdf")
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

@app.route("/api/audit_logs", methods=["GET"])
@login_required
def get_audit_logs():
    if current_user.role_name != 'Admin':
        return jsonify({"error": "Permission denied"}), 403
        
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    offset = (page - 1) * per_page

    conn, cursor = get_db_connection(dictionary=True)
    if not conn: return jsonify({"error": "Database error"}), 500
    
    try:
        # Get total count
        cursor.execute("SELECT COUNT(*) as total FROM audit_logs")
        total = cursor.fetchone()['total']

        # Get paginated logs
        cursor.execute("SELECT * FROM audit_logs ORDER BY created_at DESC LIMIT %s OFFSET %s", (per_page, offset))
        logs = cursor.fetchall()
        
        return jsonify({
            "logs": logs,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

if __name__ == "__main__":
    app.run(debug=True)
