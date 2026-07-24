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
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

load_dotenv()

ACTIVE_USER_SECRET = os.getenv("ACTIVE_USER_SECRET", "")
ACTIVE_USER_TTL_SECONDS = int(os.getenv("ACTIVE_USER_TTL_SECONDS", "300"))
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
]


@dataclass
class UserPresence:
    user_id: int
    school_id: int
    role: str
    name: str
    last_seen: float


@dataclass
class RosterSubscription:
    websocket: WebSocket
    student_ids: set[int] = field(default_factory=set)


class PresenceStore:
    def __init__(self, ttl_seconds: int) -> None:
        self.ttl_seconds = ttl_seconds
        self.users: dict[int, UserPresence] = {}
        self.subscriptions: list[RosterSubscription] = []
        self._lock = asyncio.Lock()

    def is_active(self, user_id: int) -> bool:
        entry = self.users.get(user_id)
        if entry is None:
            return False
        return (time.time() - entry.last_seen) <= self.ttl_seconds

    def active_from(self, user_ids: list[int]) -> list[int]:
        return [user_id for user_id in user_ids if self.is_active(user_id)]

    async def heartbeat(self, user_id: int, school_id: int, role: str, name: str) -> None:
        async with self._lock:
            self.users[user_id] = UserPresence(
                user_id=user_id,
                school_id=school_id,
                role=role,
                name=name,
                last_seen=time.time(),
            )
        await self.notify_watchers({user_id})

    async def expire_stale(self) -> None:
        now = time.time()
        expired_ids: set[int] = set()
        async with self._lock:
            stale = [
                user_id
                for user_id, entry in self.users.items()
                if (now - entry.last_seen) > self.ttl_seconds
            ]
            for user_id in stale:
                expired_ids.add(user_id)
                del self.users[user_id]

        if expired_ids:
            await self.notify_watchers(expired_ids)

    async def add_subscription(self, websocket: WebSocket, student_ids: list[int]) -> RosterSubscription:
        subscription = RosterSubscription(websocket=websocket, student_ids=set(student_ids))
        async with self._lock:
            self.subscriptions.append(subscription)
        return subscription

    async def remove_subscription(self, subscription: RosterSubscription) -> None:
        async with self._lock:
            if subscription in self.subscriptions:
                self.subscriptions.remove(subscription)

    async def notify_watchers(self, changed_user_ids: set[int]) -> None:
        async with self._lock:
            targets = [
                sub
                for sub in self.subscriptions
                if changed_user_ids.intersection(sub.student_ids)
            ]

        for subscription in targets:
            active_ids = self.active_from(sorted(subscription.student_ids))
            try:
                await subscription.websocket.send_json(
                    {"type": "presence", "active_user_ids": active_ids}
                )
            except Exception:
                await self.remove_subscription(subscription)


store = PresenceStore(ACTIVE_USER_TTL_SECONDS)


async def expiry_loop() -> None:
    while True:
        await asyncio.sleep(30)
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


class HeartbeatPayload(BaseModel):
    user_id: int = Field(gt=0)
    school_id: int = Field(ge=0)
    role: str = Field(min_length=1, max_length=32)
    name: str = Field(default="", max_length=255)


def require_server_key(header: str | None) -> None:
    if not ACTIVE_USER_SECRET:
        raise HTTPException(status_code=503, detail="Server secret not configured.")
    if not header or not hmac.compare_digest(header, ACTIVE_USER_SECRET):
        raise HTTPException(status_code=401, detail="Invalid server key.")


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


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/heartbeat")
async def heartbeat(
    payload: HeartbeatPayload,
    x_active_user_key: str | None = Header(default=None, alias="X-Active-User-Key"),
) -> dict[str, bool]:
    require_server_key(x_active_user_key)
    await store.heartbeat(
        user_id=payload.user_id,
        school_id=payload.school_id,
        role=payload.role,
        name=payload.name,
    )
    return {"ok": True}


@app.get("/active")
async def active_users(
    user_ids: str = Query(default=""),
    x_active_user_key: str | None = Header(default=None, alias="X-Active-User-Key"),
) -> dict[str, list[int]]:
    require_server_key(x_active_user_key)

    ids = [int(part) for part in user_ids.split(",") if part.strip().isdigit()]
    return {"active": store.active_from(ids)}


@app.websocket("/ws/roster")
async def roster_ws(websocket: WebSocket, token: str = Query(default="")) -> None:
    payload = verify_ws_token(token)
    student_ids = [int(value) for value in payload.get("student_ids", []) if str(value).isdigit()]

    await websocket.accept()
    subscription = await store.add_subscription(websocket, student_ids)

    await websocket.send_json(
        {
            "type": "presence",
            "active_user_ids": store.active_from(student_ids),
        }
    )

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await store.remove_subscription(subscription)
