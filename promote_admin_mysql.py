
import mysql.connector

MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'rohit',
    'password': 'rohit',
    'database': 'reports_dashboard'
}

def promote_user(email):
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # Check if user exists
        cursor.execute("SELECT id, name, role_id FROM users WHERE email = %s", (email,))
        user = cursor.fetchone()
        
        if user:
            print(f"Found user: {user[1]} (Current Role: {user[2]})")
            # Admin role is usually ID 1 in the seeded roles
            cursor.execute("UPDATE users SET role_id = 1 WHERE email = %s", (email,))
            conn.commit()
            print(f"✅ Successfully promoted {email} to Admin.")
        else:
            print(f"❌ User {email} not found. Please ensure they have logged in at least once.")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    email_to_promote = input("Enter the email to promote to Admin: ").strip()
    if email_to_promote:
        promote_user(email_to_promote)
    else:
        print("No email provided.")
