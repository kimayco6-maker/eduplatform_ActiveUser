import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Form, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from starlette.middleware.sessions import SessionMiddleware

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

ACTIVE_USER_SECRET = os.getenv("ACTIVE_USER_SECRET", "")
ACTIVE_USER_TTL_SECONDS = int(os.getenv("ACTIVE_USER_TTL_SECONDS", "9"))
ACTIVE_USER_EXPIRY_INTERVAL_SECONDS = int(os.getenv("ACTIVE_USER_EXPIRY_INTERVAL_SECONDS", "3"))
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
]
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
ADMIN_SESSION_SECRET = os.getenv("ADMIN_SESSION_SECRET") or ACTIVE_USER_SECRET or "change-me-admin-session"


@dataclass
class UserPresence:
    user_id: int
    school_id: int
    school_code: str
    role: str
    name: str
    last_seen: float
    school_name: str = ""


@dataclass
class RosterSubscription:
    websocket: WebSocket
    school_code: str
    student_ids: set[int] = field(default_factory=set)


class PresenceStore:
    """
    Presence store keyed by composite "{school_code}:{user_id}".
    This prevents cross-school user ID collisions when each school has its
    own database where user IDs restart from 1.
    """

    def __init__(self, ttl_seconds: int) -> None:
        self.ttl_seconds = ttl_seconds
        # key: "{school_code}:{user_id}"
        self.users: dict[str, UserPresence] = {}
        self.subscriptions: list[RosterSubscription] = []
        self._lock = asyncio.Lock()

    @staticmethod
    def _key(school_code: str, user_id: int) -> str:
        return f"{school_code.upper()}:{user_id}"

    def is_active(self, school_code: str, user_id: int) -> bool:
        entry = self.users.get(self._key(school_code, user_id))
        if entry is None:
            return False
        return (time.time() - entry.last_seen) <= self.ttl_seconds

    def active_from(self, school_code: str, user_ids: list[int]) -> list[int]:
        return [uid for uid in user_ids if self.is_active(school_code, uid)]

    async def heartbeat(
        self,
        user_id: int,
        school_id: int,
        school_code: str,
        role: str,
        name: str,
        school_name: str = "",
    ) -> None:
        key = self._key(school_code, user_id)
        async with self._lock:
            self.users[key] = UserPresence(
                user_id=user_id,
                school_id=school_id,
                school_code=school_code.upper(),
                role=role,
                name=name,
                last_seen=time.time(),
                school_name=school_name,
            )
        await self.notify_watchers(school_code, {user_id})

    async def leave(self, school_code: str, user_id: int) -> bool:
        key = self._key(school_code, user_id)
        removed = False
        async with self._lock:
            if key in self.users:
                del self.users[key]
                removed = True
        if removed:
            await self.notify_watchers(school_code, {user_id})
        return removed

    async def expire_stale(self) -> None:
        now = time.time()
        expired: dict[str, set[int]] = {}
        async with self._lock:
            stale = [
                (key, entry)
                for key, entry in self.users.items()
                if (now - entry.last_seen) > self.ttl_seconds
            ]
            for key, entry in stale:
                expired.setdefault(entry.school_code, set()).add(entry.user_id)
                del self.users[key]

        for sc, ids in expired.items():
            await self.notify_watchers(sc, ids)

    async def add_subscription(
        self, websocket: WebSocket, school_code: str, student_ids: list[int]
    ) -> RosterSubscription:
        subscription = RosterSubscription(
            websocket=websocket,
            school_code=school_code.upper(),
            student_ids=set(student_ids),
        )
        async with self._lock:
            self.subscriptions.append(subscription)
        return subscription

    async def remove_subscription(self, subscription: RosterSubscription) -> None:
        async with self._lock:
            if subscription in self.subscriptions:
                self.subscriptions.remove(subscription)

    async def notify_watchers(self, school_code: str, changed_user_ids: set[int]) -> None:
        sc = school_code.upper()
        async with self._lock:
            targets = [
                sub
                for sub in self.subscriptions
                if sub.school_code == sc and changed_user_ids.intersection(sub.student_ids)
            ]

        for subscription in targets:
            active_ids = self.active_from(sc, sorted(subscription.student_ids))
            try:
                await subscription.websocket.send_json(
                    {"type": "presence", "active_user_ids": active_ids}
                )
            except Exception:
                await self.remove_subscription(subscription)

    def _active_entries(self) -> list[UserPresence]:
        now = time.time()
        return [
            entry
            for entry in self.users.values()
            if (now - entry.last_seen) <= self.ttl_seconds
        ]

    def list_active(
        self,
        school_id: int | None = None,
        school_code: str | None = None,
        role: str | None = None,
        q: str | None = None,
    ) -> list[UserPresence]:
        query = (q or "").strip().lower()
        role_filter = (role or "").strip().lower()
        sc_filter = (school_code or "").strip().upper()
        results: list[UserPresence] = []

        for entry in self._active_entries():
            if school_id is not None and entry.school_id != school_id:
                continue
            if sc_filter and entry.school_code != sc_filter:
                continue
            if role_filter and entry.role.lower() != role_filter:
                continue
            if query:
                name_match = query in entry.name.lower()
                id_match = query.isdigit() and int(query) == entry.user_id
                school_match = query in entry.school_name.lower()
                if not (name_match or id_match or school_match):
                    continue
            results.append(entry)

        results.sort(key=lambda item: (-item.last_seen, item.name.lower(), item.user_id))
        return results

    def stats(self) -> dict[str, Any]:
        entries = self._active_entries()
        by_role: dict[str, int] = {}
        by_school: dict[str, dict[str, Any]] = {}

        for entry in entries:
            by_role[entry.role] = by_role.get(entry.role, 0) + 1
            school_key = entry.school_code or str(entry.school_id)
            if school_key not in by_school:
                by_school[school_key] = {
                    "school_id": entry.school_id,
                    "school_code": entry.school_code,
                    "school_name": entry.school_name or (
                        "Platform" if entry.school_id == 0 else f"School #{entry.school_id}"
                    ),
                    "count": 0,
                }
            by_school[school_key]["count"] += 1
            if entry.school_name and not by_school[school_key]["school_name"]:
                by_school[school_key]["school_name"] = entry.school_name

        schools = sorted(
            by_school.values(),
            key=lambda item: (-int(item["count"]), str(item["school_name"]).lower()),
        )
        return {
            "total": len(entries),
            "by_role": dict(sorted(by_role.items(), key=lambda item: item[0])),
            "by_school": schools,
        }


store = PresenceStore(ACTIVE_USER_TTL_SECONDS)


async def expiry_loop() -> None:
    interval = max(1, ACTIVE_USER_EXPIRY_INTERVAL_SECONDS)
    while True:
        await asyncio.sleep(interval)
        await store.expire_stale()


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(expiry_loop())
    yield
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


app = FastAPI(title="LMS Active User Server", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    SessionMiddleware,
    secret_key=ADMIN_SESSION_SECRET,
    session_cookie="active_user_admin",
    same_site="lax",
    https_only=False,
)

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


class HeartbeatPayload(BaseModel):
    user_id: int = Field(gt=0)
    school_id: int = Field(ge=0)
    school_code: str = Field(default="", max_length=20)
    role: str = Field(min_length=1, max_length=32)
    name: str = Field(default="", max_length=255)
    school_name: str = Field(default="", max_length=255)


class LeavePayload(BaseModel):
    user_id: int = Field(gt=0)
    school_code: str = Field(default="", max_length=20)


def require_server_key(header: str | None) -> None:
    if not ACTIVE_USER_SECRET:
        raise HTTPException(status_code=503, detail="Server secret not configured.")
    if not header or not hmac.compare_digest(header, ACTIVE_USER_SECRET):
        raise HTTPException(status_code=401, detail="Invalid server key.")


def admin_configured() -> bool:
    return bool(ADMIN_USERNAME and ADMIN_PASSWORD)


def verify_ws_token(token: str) -> dict[str, Any]:
    if not ACTIVE_USER_SECRET:
        raise HTTPException(status_code=503, detail="Server secret not configured.")

    parts = token.split(".", 1)
    if len(parts) != 2:
        raise HTTPException(status_code=401, detail="Invalid token format.")

    payload_b64, signature = parts
    try:
        payload_json = base64.b64decode(payload_b64).decode("utf-8")
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid token payload.") from exc

    expected = hmac.new(
        ACTIVE_USER_SECRET.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="Invalid token signature.")

    payload = json.loads(payload_json)
    if int(payload.get("exp", 0)) < int(time.time()):
        raise HTTPException(status_code=401, detail="Token expired.")

    student_ids = payload.get("student_ids")
    if not isinstance(student_ids, list):
        raise HTTPException(status_code=401, detail="Invalid token student list.")

    return payload


def presence_to_dict(entry: UserPresence) -> dict[str, Any]:
    now = time.time()
    return {
        "user_id": entry.user_id,
        "name": entry.name,
        "role": entry.role,
        "school_id": entry.school_id,
        "school_code": entry.school_code,
        "school_name": entry.school_name
        or ("Platform" if entry.school_id == 0 else f"School #{entry.school_id}"),
        "last_seen": entry.last_seen,
        "seconds_ago": max(0, int(now - entry.last_seen)),
    }


def require_admin_session(request: Request) -> None:
    if not admin_configured():
        raise HTTPException(status_code=503, detail="Admin login is not configured.")
    if request.session.get("admin_authenticated") is not True:
        raise HTTPException(status_code=401, detail="Admin login required.")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
async def root() -> RedirectResponse:
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/heartbeat")
async def heartbeat(
    payload: HeartbeatPayload,
    x_active_user_key: str | None = Header(default=None, alias="X-Active-User-Key"),
) -> dict[str, bool]:
    require_server_key(x_active_user_key)
    await store.heartbeat(
        user_id=payload.user_id,
        school_id=payload.school_id,
        school_code=payload.school_code,
        role=payload.role,
        name=payload.name,
        school_name=payload.school_name,
    )
    return {"ok": True}


@app.post("/leave")
async def leave(
    payload: LeavePayload,
    x_active_user_key: str | None = Header(default=None, alias="X-Active-User-Key"),
) -> dict[str, bool]:
    require_server_key(x_active_user_key)
    removed = await store.leave(payload.school_code, payload.user_id)
    return {"ok": True, "removed": removed}


@app.get("/active")
async def active_users(
    user_ids: str = Query(default=""),
    school_code: str = Query(default=""),
    x_active_user_key: str | None = Header(default=None, alias="X-Active-User-Key"),
) -> dict[str, list[int]]:
    require_server_key(x_active_user_key)

    ids = [int(part) for part in user_ids.split(",") if part.strip().isdigit()]
    return {"active": store.active_from(school_code, ids)}


@app.websocket("/ws/roster")
async def roster_ws(websocket: WebSocket, token: str = Query(default="")) -> None:
    payload = verify_ws_token(token)
    school_code = str(payload.get("school_code", ""))
    student_ids = [int(value) for value in payload.get("student_ids", []) if str(value).isdigit()]

    await websocket.accept()
    subscription = await store.add_subscription(websocket, school_code, student_ids)

    await websocket.send_json(
        {
            "type": "presence",
            "active_user_ids": store.active_from(school_code, student_ids),
        }
    )

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await store.remove_subscription(subscription)


@app.get("/admin/login", response_class=HTMLResponse, response_model=None)
async def admin_login_page(request: Request):
    if request.session.get("admin_authenticated") is True and admin_configured():
        return RedirectResponse(url="/admin", status_code=302)
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "error": None,
            "admin_configured": admin_configured(),
        },
    )


@app.post("/admin/login", response_model=None)
async def admin_login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    if not admin_configured():
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "error": "Admin login is not configured. Set ADMIN_USERNAME and ADMIN_PASSWORD.",
                "admin_configured": False,
            },
            status_code=503,
        )

    user_ok = hmac.compare_digest(username, ADMIN_USERNAME)
    pass_ok = hmac.compare_digest(password, ADMIN_PASSWORD)
    if not (user_ok and pass_ok):
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "error": "Invalid username or password.",
                "admin_configured": True,
            },
            status_code=401,
        )

    request.session["admin_authenticated"] = True
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/logout")
async def admin_logout(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse(url="/admin/login", status_code=303)


@app.get("/admin", response_class=HTMLResponse, response_model=None)
async def admin_dashboard(
    request: Request,
    school_id: int | None = Query(default=None),
    school_code: str = Query(default=""),
    role: str = Query(default=""),
    q: str = Query(default=""),
) -> HTMLResponse:
    if not admin_configured():
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "error": "Admin login is not configured. Set ADMIN_USERNAME and ADMIN_PASSWORD.",
                "admin_configured": False,
            },
            status_code=503,
        )
    if request.session.get("admin_authenticated") is not True:
        return RedirectResponse(url="/admin/login", status_code=303)

    role_filter = role.strip() or None
    sc_filter = school_code.strip() or None
    users = [
        presence_to_dict(entry)
        for entry in store.list_active(school_id, sc_filter, role_filter, q)
    ]
    stats = store.stats()

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "users": users,
            "stats": stats,
            "filters": {
                "school_id": school_id,
                "school_code": school_code.strip(),
                "role": role.strip(),
                "q": q.strip(),
            },
            "roles": ["student", "teacher", "school_admin", "super_admin"],
        },
    )


@app.get("/admin/api/users")
async def admin_api_users(
    request: Request,
    school_id: int | None = Query(default=None),
    school_code: str = Query(default=""),
    role: str = Query(default=""),
    q: str = Query(default=""),
) -> dict[str, Any]:
    require_admin_session(request)
    role_filter = role.strip() or None
    sc_filter = school_code.strip() or None
    users = [
        presence_to_dict(entry)
        for entry in store.list_active(school_id, sc_filter, role_filter, q)
    ]
    return {
        "users": users,
        "stats": store.stats(),
        "filters": {
            "school_id": school_id,
            "school_code": school_code.strip(),
            "role": role.strip(),
            "q": q.strip(),
        },
    }
