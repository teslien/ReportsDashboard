# Authentication Setup Guide

This application uses Google OAuth 2.0 for user authentication.

## Prerequisites

You need to obtain OAuth credentials from the Google Cloud Console.

1.  Go to the [Google Cloud Console](https://console.cloud.google.com/).
2.  Create a new project or select an existing one.
3.  Navigate to **APIs & Services** > **Credentials**.
4.  Click **Create Credentials** > **OAuth client ID**.
5.  Select **Web application**.
6.  Set the **Authorized redirect URIs** to:
    *   `http://127.0.0.1:5000/auth/callback`
    *   `http://localhost:5000/auth/callback`
7.  Copy the **Client ID** and **Client Secret**.

## Configuration

You must set the following environment variables before running the application:

### PowerShell (Windows)
```powershell
$env:GOOGLE_CLIENT_ID="your-client-id"
$env:GOOGLE_CLIENT_SECRET="your-client-secret"
$env:FLASK_SECRET_KEY="some-random-secure-string"
python app.py
```

### Bash (Linux/Mac)
```bash
export GOOGLE_CLIENT_ID="your-client-id"
export GOOGLE_CLIENT_SECRET="your-client-secret"
export FLASK_SECRET_KEY="some-random-secure-string"
python app.py
```

## Admin Access

The first time the application starts, it seeds default roles:
- **Admin**: Full access.
- **Editor**: Can view and edit.
- **Viewer**: Read-only.

When you first log in with Google, you will be assigned the **Viewer** role.
To promote yourself to **Admin**, you will need to manually update the database or use a SQLite tool initially:

```sql
UPDATE users SET role_id = 1 WHERE email = 'your-email@gmail.com';
```

Once you are an Admin, you can manage other users via the **Admin Panel** in the Navbar.
