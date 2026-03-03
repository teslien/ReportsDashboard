
try:
    import app
    # app.init_db() # Already run
    
    import sqlite3
    conn = sqlite3.connect("tracker.db")
    cursor = conn.cursor()
    
    print("\nRoles:")
    cursor.execute("SELECT * FROM roles")
    for row in cursor.fetchall():
        print(row)
        
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
