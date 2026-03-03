# Authentication & RBAC Implementation Plan

## Goal
Integrate a secure authentication system with Role-Based Access Control (RBAC) to manage user access, permissions, and API tokens.

## 1. Database Schema Changes
We will introduce three new tables to `tracker.db`:

### `roles` Table
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER PK | Unique Role ID |
| `name` | TEXT | Role Name (e.g., Admin, Editor, Viewer) |
| `permissions` | TEXT | JSON string of permissions (e.g., `{"view_reports": true, "delete_tickets": false}`) |

### `users` Table
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER PK | Unique User ID |
| `username` | TEXT | Unique Login Username |
| `password_hash` | TEXT | Hashed Password |
| `role_id` | INTEGER | FK to `roles.id` |
| `api_token` | TEXT | Unique API Token for programmatic access |
| `created_at` | DATETIME | Timestamp |

## 2. Backend Implementation (Flask)

### Dependencies
- Install `Flask-Login` for session management.
- Use `werkzeug.security` for password hashing (already available).

### Authentication Logic
- **Login/Logout**: Create routes `/login` and `/logout`.
- **User Loader**: Implement `load_user` for Flask-Login.
- **API Token Auth**: Middleware/Decorator to allow access via `Authorization: Bearer <token>` header for API routes.

### Authorization (RBAC) Logic
- **Decorators**:
  - `@login_required`: Protects routes requiring authentication.
  - `@permission_required(permission_name)`: Protects routes requiring specific permissions.
  - `@admin_required`: Shortcut for Admin-only routes.

### Default Permissions
- **Admin**: Full Access.
- **Editor**: Can View, Edit, Create, but not Delete users/roles.
- **Viewer**: Read-only access to reports and dashboards.

## 3. Frontend Implementation

### Pages
- **Login Page**: A clean, simple login form.
- **Admin Dashboard**:
  - **User Management**: List, Create, Edit, Delete users. Reset passwords. Regenerate API Tokens.
  - **Role Management**: Create/Edit roles and toggle permissions.

### UI Updates
- **Navbar**: Show "Logged in as [User]" and "Logout" button. Show "Admin" link if applicable.
- **Conditional Rendering**: Hide "Delete" buttons or "Settings" links if the user lacks permissions.

## 4. Migration Strategy
1.  Create the tables.
2.  Create a default **Admin** user (username: `admin`, password: `admin` - change on first login) and **Admin** role.
3.  Apply `@login_required` to all critical routes.

## 5. Execution Steps
1.  **Install Dependencies**: `pip install flask-login`.
2.  **Update `app.py`**: Add Database Schema & Models.
3.  **Create Templates**: `login.html`, `admin/users.html`, `admin/roles.html`.
4.  **Implement Routes**: Auth & Admin routes.
5.  **Protect Existing Routes**: Add decorators to `api/*` and page routes.
6.  **Verify**: Test login, token access, and permission enforcement.
