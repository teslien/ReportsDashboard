import requests
import base64
import json
import sys
import os

# =========================
# 🔴 CONFIG (EXTRACTED FROM app.py)
# =========================
JIRA_DOMAIN = "https://lumberfi.atlassian.net/"
# Security: Use environment variables or rely on the main app's configuration
JIRA_EMAIL = os.environ.get("JIRA_EMAIL", "") 
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN", "")

if not JIRA_EMAIL or not JIRA_API_TOKEN:
    print("Warning: JIRA_EMAIL or JIRA_API_TOKEN not set in environment variables.")

AUTH = base64.b64encode(
    f"{JIRA_EMAIL}:{JIRA_API_TOKEN}".encode()
).decode()

HEADERS = {
    "Authorization": f"Basic {AUTH}",
    "Content-Type": "application/json"
}

def fetch_ticket(issue_key):
    """Fetches all data for a specific Jira ticket and saves it to a JSON file."""
    print(f"Fetching details for ticket: {issue_key}...")
    
    # The 'expand=changelog' parameter ensures we get the history of updates
    url = f"{JIRA_DOMAIN}/rest/api/3/issue/{issue_key}?expand=changelog"
    
    try:
        response = requests.get(url, headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            output_file = f"{issue_key}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                
            print(f"Successfully saved ticket details to {output_file}")
            return True
        else:
            print(f"Error: Received status code {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fetch_ticket.py <ISSUE_KEY>")
        print("Example: python fetch_ticket.py TIM-20448")
        sys.exit(1)
        
    issue_key = sys.argv[1].upper()
    fetch_ticket(issue_key)
