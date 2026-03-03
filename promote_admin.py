
import sqlite3

try:
    conn = sqlite3.connect('tracker.db')
    cursor = conn.cursor()
    
    email = 'rohit.bairwa@lumberfi.com'
    
    # Check if user exists
    cursor.execute("SELECT id, name, role_id FROM users WHERE email = ?", (email,))
    user = cursor.fetchone()
    
    if user:
        print(f"Found user: {user[1]} (Current Role ID: {user[2]})")
        # Update to Admin (Role ID 1)
        cursor.execute("UPDATE users SET role_id = 1 WHERE id = ?", (user[0],))
        conn.commit()
        print(f"Successfully promoted {email} to Admin.")
    else:
        print(f"User {email} not found in database. Please log in once first.")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
