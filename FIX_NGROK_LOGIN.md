# Fix Google Login Error (Ngrok)

You are seeing this error because you are accessing the app via **ngrok** (`http://7822-49-200-103-178.ngrok-free.app`), but Google only knows about `localhost`.

## ⚡ Action Required: Update Google Cloud Console

1.  Go to the **[Google Cloud Console Credentials Page](https://console.cloud.google.com/apis/credentials)**.
2.  Click the **Pencil Icon (Edit)** next to your OAuth 2.0 Client ID.
3.  Scroll down to the **"Authorized redirect URIs"** section.
4.  Click **"ADD URI"** and paste your current ngrok URL:
    
    ```
    http://7822-49-200-103-178.ngrok-free.app/auth/callback
    ```
    
    *(Note: If you restart ngrok and get a new URL, you will need to add that one too)*

5.  Click **SAVE**.

## 🔄 Try Again

1.  Wait about 1 minute.
2.  Go back to your app: [http://7822-49-200-103-178.ngrok-free.app/login](http://7822-49-200-103-178.ngrok-free.app/login)
3.  Click "Sign in with Google" again.
