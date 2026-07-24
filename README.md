# LMS Active User Server

Real-time student presence service for the teacher class roster, plus a password-protected admin console for platform operators. Deploy this folder to [Render](https://render.com) as a Python web service.

Browsers send a heartbeat every **3 seconds**. The server sweeps stale users every **3 seconds** and marks anyone silent longer than `ACTIVE_USER_TTL_SECONDS` (default **9**) as offline. Closing a tab or logging out also calls `/leave` immediately.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check |
| `GET` | `/` | Redirects to `/admin` |
| `POST` | `/heartbeat` | LMS server heartbeat (`X-Active-User-Key` header) |
| `POST` | `/leave` | Mark a user inactive immediately |
| `GET` | `/active?user_ids=1,2,3` | Snapshot of active user IDs |
| `WS` | `/ws/roster?token=...` | Teacher roster live updates |
| `GET` | `/admin/login` | Server admin login page |
| `POST` | `/admin/login` | Submit admin credentials |
| `POST` | `/admin/logout` | End admin session |
| `GET` | `/admin` | Active users dashboard (session required) |
| `GET` | `/admin/api/users` | JSON list/filter/search for the dashboard |

## Environment variables

| Variable | Description |
|----------|-------------|
| `ACTIVE_USER_SECRET` | Shared secret with the LMS (`ACTIVE_USER_SERVER_SECRET`) |
| `ACTIVE_USER_TTL_SECONDS` | Seconds without a heartbeat before offline (default `9`) |
| `ACTIVE_USER_EXPIRY_INTERVAL_SECONDS` | How often stale users are swept (default `3`) |
| `ALLOWED_ORIGINS` | Comma-separated LMS origins for browser WebSocket CORS |
| `ADMIN_USERNAME` | Username for the `/admin` console |
| `ADMIN_PASSWORD` | Password for the `/admin` console |
| `ADMIN_SESSION_SECRET` | Cookie signing key (defaults to `ACTIVE_USER_SECRET`) |

If `ADMIN_USERNAME` or `ADMIN_PASSWORD` is missing, the admin console returns a configuration error.

## Local development

```bash
cd active-user-server
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn main:app --reload --port 8080
```

Open `http://localhost:8080/admin/login` after setting admin credentials in `.env`.

## Render deployment

1. Create a new **Web Service** on Render and point it at this folder.
2. Set `ACTIVE_USER_SECRET` to the same value as `ACTIVE_USER_SERVER_SECRET` in the LMS `.env`.
3. Set `ALLOWED_ORIGINS` to your LMS site URL(s).
4. Set `ADMIN_USERNAME` and `ADMIN_PASSWORD` for the ops console.
5. Set `ACTIVE_USER_TTL_SECONDS=9` and `ACTIVE_USER_EXPIRY_INTERVAL_SECONDS=3` (or rely on defaults after redeploy).
6. Copy the Render service URL into the LMS `.env` as `ACTIVE_USER_SERVER_URL` (no trailing slash).
7. Run LMS migration `065_user_last_login_seen.sql`.

Visit `https://your-service.onrender.com/admin/login` to browse active users by school, role, and search.

## LMS `.env` example

```env
USER_ACTIVE_THRESHOLD_SECONDS=9
ACTIVE_USER_SERVER_URL=https://your-service.onrender.com
ACTIVE_USER_SERVER_SECRET=your-shared-secret
```

When the Python server is not configured, the roster falls back to MySQL `last_seen_at`.
