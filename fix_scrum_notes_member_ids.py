import sqlite3


def fix(db_path: str = "tracker.db") -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Update only rows that have clearly-invalid member_id values.
    cur.execute(
        """
        UPDATE scrum_notes
        SET member_id = (
            SELECT tm.account_id
            FROM team_members tm
            WHERE tm.team_id = scrum_notes.team_id
              AND lower(trim(tm.display_name)) = lower(trim(scrum_notes.member_name))
            LIMIT 1
        )
        WHERE member_id IS NULL
           OR trim(member_id) = ''
           OR lower(trim(member_id)) IN ('undefined', 'null')
        """
    )
    updated = cur.rowcount
    conn.commit()

    # Show any remaining problematic rows for manual resolution.
    cur.execute(
        """
        SELECT id, date, team_id, member_name, member_id, ticket_key
        FROM scrum_notes
        WHERE member_id IS NULL
           OR trim(member_id) = ''
           OR lower(trim(member_id)) IN ('undefined', 'null')
        ORDER BY date DESC, team_id, member_name, id
        """
    )
    remaining = cur.fetchall()

    conn.close()

    print(f"Updated rows: {updated}")
    if remaining:
        print("\nRows still unresolved (no exact name match in team_members):")
        for r in remaining:
            print(
                f"- id={r[0]} date={r[1]} team_id={r[2]} member={r[3]} member_id={r[4]} ticket={r[5]}"
            )
    else:
        print("All invalid rows have been fixed.")


if __name__ == "__main__":
    fix()

