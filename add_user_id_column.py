#!/usr/bin/env python3
"""
Script to add user_id column to team_members table
"""
import sys
import os

# Add the app directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the database connection from app
from app import get_db_connection

def add_user_id_column():
    conn, cursor = get_db_connection()
    if not conn:
        print("ERROR: Could not connect to database")
        return False
    
    try:
        # Check if user_id column exists
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'team_members' 
            AND COLUMN_NAME = 'user_id'
        """)
        
        result = cursor.fetchone()
        if result:
            print('✓ user_id column already exists in team_members table')
        else:
            print('Adding user_id column to team_members table...')
            cursor.execute("""
                ALTER TABLE team_members 
                ADD COLUMN user_id INT DEFAULT NULL
            """)
            conn.commit()
            print('✓ user_id column added successfully!')
            
            # Add foreign key constraint
            print('Adding foreign key constraint...')
            try:
                cursor.execute("""
                    ALTER TABLE team_members 
                    ADD CONSTRAINT fk_team_members_user 
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
                """)
                conn.commit()
                print('✓ Foreign key constraint added successfully!')
            except Exception as e:
                if 'Duplicate' in str(e):
                    print('✓ Foreign key constraint already exists')
                else:
                    print(f'Warning: Could not add foreign key: {e}')
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f'ERROR: {e}')
        if conn:
            conn.close()
        return False

if __name__ == '__main__':
    success = add_user_id_column()
    sys.exit(0 if success else 1)
