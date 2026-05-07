import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='rohit',
    password='Rohit',
    database='rb_win'
)
cursor = conn.cursor()

# Check if user_id column exists
cursor.execute("""
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'rb_win'
    AND TABLE_NAME = 'team_members' 
    AND COLUMN_NAME = 'user_id'
""")

result = cursor.fetchone()
if result:
    print('user_id column already exists')
else:
    print('Adding user_id column...')
    cursor.execute('ALTER TABLE team_members ADD COLUMN user_id INT DEFAULT NULL')
    conn.commit()
    print('user_id column added!')
    
    # Add foreign key
    try:
        cursor.execute('ALTER TABLE team_members ADD CONSTRAINT fk_team_members_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL')
        conn.commit()
        print('Foreign key added!')
    except Exception as e:
        print(f'Foreign key note: {e}')

cursor.close()
conn.close()
print('Done!')
