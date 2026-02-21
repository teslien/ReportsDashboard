import sqlite3

def debug_db():
    conn = sqlite3.connect('tracker.db')
    conn.row_factory = sqlite3.Row
    
    print("--- ALL TEAM MEMBERS ---")
    members = conn.execute("SELECT * FROM team_members").fetchall()
    for m in members:
        print(f"ID: {m['id']} | Team: {m['team_id']} | Name: {m['display_name']} | Account: {m['account_id']}")
        
    print("\n--- ALL SCRUM NOTES ---")
    notes = conn.execute("SELECT * FROM scrum_notes").fetchall()
    for n in notes:
        print(f"ID: {n['id']} | Date: {n['date']} | Team: {n['team_id']} | Member: {n['member_name']} ({n['member_id']}) | Ticket: {n['ticket_key']}")
        
    conn.close()

if __name__ == "__main__":
    debug_db()
