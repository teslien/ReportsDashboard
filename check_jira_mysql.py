import mysql.connector
import json

MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'rohit',
    'password': 'rohit',
    'database': 'reports_dashboard'
}

def check_jira_config():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM jira_config WHERE id = 1")
        row = cursor.fetchone()
        print(f"Jira Config (ID=1): {row}")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_jira_config()
