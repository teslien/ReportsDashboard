# Fix Google Login Error

The error **"redirect_uri_mismatch"** or **"doesn't comply with Google's OAuth 2.0 policy"** happens because Google only trusts specific URLs that you explicitly whitelist.

Your app is trying to redirect to:
`http://localhost:5000/auth/callback`

But your Google Cloud Console likely only has `http://127.0.0.1:5000/` or is missing the callback path entirely.

## ⚡ Action Required: Update Google Cloud Console

1.  Go to the **[Google Cloud Console Credentials Page](https://console.cloud.google.com/apis/credentials)**.
2.  Click the **Pencil Icon (Edit)** next to your OAuth 2.0 Client ID (the one you created for this project).
3.  Scroll down to the **"Authorized redirect URIs"** section.
4.  Click **"ADD URI"** and paste this EXACT URL:
    
    ```
    http://localhost:5000/auth/callback
    ```

5.  (Optional but Recommended) Click **"ADD URI"** again and add the IP version too, just in case:
    
    ```
    http://127.0.0.1:5000/auth/callback
    ```

6.  Click **SAVE**.

## 🔄 Try Again

1.  Wait about 1-2 minutes (Google settings can take a moment to propagate).
2.  Go back to your app: [http://localhost:5000/login](http://localhost:5000/login).
3.  Click "Sign in with Google" again.
