# Roles & Permissions Guide

This system uses a Role-Based Access Control (RBAC) model.

## Default Roles

1.  **Admin**: Has full access to everything. Cannot be deleted.
2.  **Editor**: Typically for users who manage day-to-day tasks (create tickets, update status).
3.  **Viewer**: Read-only access.

## Managing Permissions

1.  Log in as an **Admin**.
2.  Navigate to **Role Management** (in the user dropdown menu).
3.  You will see a matrix of permissions vs roles.
4.  Check/Uncheck boxes to grant or revoke permissions.
5.  Changes are saved immediately.

## Available Permissions

| Permission Key | Description |
| :--- | :--- |
| `view_reports` | Access to Reports, Explorer, Query Builder, Scoreboard. |
| `create_tickets` | Can create new To-Do items, Scrum Notes, etc. |
| `edit_tickets` | Can update status, comments, and details of tickets. |
| `delete_tickets` | Can delete To-Do items, Trackers, Scrum Notes. |
| `manage_settings` | Can update Jira API keys, General Settings, and Tags. |
| `manage_teams` | Can create/delete Teams and manage members. |

## Creating New Roles

1.  Click **Create Role** in the Role Management page.
2.  Enter a name (e.g., "Project Manager").
3.  Configure permissions for the new role.

## Assigning Roles to Users

1.  Navigate to **User Management**.
2.  Use the dropdown next to a user to change their role.
3.  Changes take effect immediately (or on next login/refresh).
