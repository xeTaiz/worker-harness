"""Browser-only login, self-service API keys and user administration routes."""

from __future__ import annotations

import secrets
import time
from typing import Awaitable, Callable

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .auth import (
    SESSION_COOKIE,
    SESSION_TTL_SECONDS,
    Principal,
    client_address,
    hash_token,
    new_api_key,
    normalize_username,
    origin_matches,
    require_web_admin,
    require_web_user,
    validate_display_name,
    validate_password,
)
from .db import Database, IdentityConflict


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LoginRequest(_Strict):
    username: str = Field(max_length=64)
    password: str = Field(max_length=1024)


class PasswordChangeRequest(_Strict):
    current_password: str = Field(max_length=1024)
    new_password: str

    @field_validator("new_password")
    @classmethod
    def _check(cls, value: str) -> str:
        return validate_password(value)


class KeyCreateRequest(_Strict):
    label: str

    @field_validator("label")
    @classmethod
    def _label(cls, value: str) -> str:
        value = value.strip()
        if not 1 <= len(value) <= 80:
            raise ValueError("label must be 1-80 characters")
        return value


class UserCreateRequest(_Strict):
    username: str
    display_name: str
    password: str
    is_admin: bool = False

    @field_validator("username")
    @classmethod
    def _username(cls, value: str) -> str:
        return normalize_username(value)

    @field_validator("display_name")
    @classmethod
    def _display(cls, value: str) -> str:
        return validate_display_name(value)

    @field_validator("password")
    @classmethod
    def _password(cls, value: str) -> str:
        return validate_password(value)


class UserUpdateRequest(_Strict):
    display_name: str | None = None
    disabled: bool | None = None
    is_admin: bool | None = None

    @field_validator("display_name")
    @classmethod
    def _display(cls, value: str | None) -> str | None:
        return None if value is None else validate_display_name(value)


class PasswordResetRequest(_Strict):
    password: str

    @field_validator("password")
    @classmethod
    def _password(cls, value: str) -> str:
        return validate_password(value)


def create_auth_router(
    *, on_user_disabled: Callable[[str], Awaitable[None]],
) -> APIRouter:
    router = APIRouter()

    def state(request: Request):
        return request.app.state

    def me_payload(user: dict, csrf_token: str, bootstrap_id: str | None) -> dict:
        return {
            "user": Database._safe_user(user),
            "csrf_token": csrf_token,
            "is_bootstrap": bootstrap_id == user["id"],
        }

    @router.post("/api/v1/auth/login")
    async def login(payload: LoginRequest, request: Request):
        app_state = state(request)
        if not origin_matches(request, app_state.web_origin):
            raise HTTPException(status_code=403, detail="origin not allowed")
        db: Database = app_state.db
        username = payload.username.strip().lower()
        client = client_address(request, app_state.trusted_proxies)
        keys = [f"user:{username}", f"client:{client}"]
        retry_after = app_state.login_limiter.reserve(keys)
        if retry_after:
            return JSONResponse(
                status_code=429,
                content={"detail": {"code": "RATE_LIMITED", "message": "too many failed logins"}},
                headers={"Retry-After": str(retry_after), "Cache-Control": "no-store"},
            )
        failed = False
        try:
            user = await db.get_user_by_username(username)
            verified_hash = user["password_hash"] if user else None
            valid = await app_state.passwords.verify(verified_hash, payload.password)
            if not valid or user is None or user["disabled"]:
                failed = True
                raise HTTPException(status_code=401, detail="invalid username or password")
            if app_state.passwords.needs_rehash(verified_hash):
                rehashed = await app_state.passwords.hash(payload.password)
                if await db.rehash_password(user["id"], rehashed, expected_hash=verified_hash):
                    verified_hash = rehashed
            token = secrets.token_urlsafe(32)
            csrf_token = secrets.token_urlsafe(32)
            now = int(time.time())
            # Issued only if the verified password is still current and the
            # account still enabled (a concurrent reset/disable wins).
            issued = await db.create_web_session(
                user_id=user["id"], token_hash=hash_token(token), csrf_token=csrf_token,
                now=now, expires_at=now + SESSION_TTL_SECONDS, password_hash=verified_hash,
            )
            if issued is None:
                failed = True
                raise HTTPException(status_code=401, detail="invalid username or password")
        finally:
            app_state.login_limiter.finish(keys, failed=failed)
        response = JSONResponse(me_payload(user, csrf_token, await db.bootstrap_user_id()))
        response.set_cookie(
            SESSION_COOKIE, token, max_age=SESSION_TTL_SECONDS, path="/", httponly=True,
            samesite="strict", secure=app_state.web_origin.startswith("https://"),
        )
        return response

    @router.get("/api/v1/auth/me")
    async def me(request: Request, principal: Principal = Depends(require_web_user)):
        db: Database = state(request).db
        user = await db.get_user(principal.user_id)
        return me_payload(user, principal.csrf_token, await db.bootstrap_user_id())

    @router.post("/api/v1/auth/logout", status_code=204)
    async def logout(request: Request, principal: Principal = Depends(require_web_user)):
        await state(request).db.delete_web_session(principal.web_session_id)
        response = Response(status_code=204)
        response.delete_cookie(SESSION_COOKIE, path="/")
        return response

    @router.post("/api/v1/auth/password", status_code=204)
    async def change_password(
        payload: PasswordChangeRequest, request: Request,
        principal: Principal = Depends(require_web_user),
    ):
        app_state = state(request)
        user = await app_state.db.get_user(principal.user_id)
        if not await app_state.passwords.verify(user["password_hash"], payload.current_password):
            raise HTTPException(status_code=403, detail="current password is incorrect")
        await app_state.db.set_password_hash(
            principal.user_id, await app_state.passwords.hash(payload.new_password), now=int(time.time()),
        )
        response = Response(status_code=204)
        response.delete_cookie(SESSION_COOKIE, path="/")
        return response

    async def issue_key(db: Database, user_id: str, label: str) -> dict:
        token, token_hash, prefix = new_api_key()
        try:
            key = await db.create_api_key(
                user_id=user_id, label=label, token_hash=token_hash, prefix=prefix, now=int(time.time()),
            )
        except IdentityConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="user not found") from exc
        return {"key": key, "token": token}

    @router.get("/api/v1/auth/keys")
    async def list_own_keys(request: Request, principal: Principal = Depends(require_web_user)):
        return await state(request).db.list_api_keys(principal.user_id)

    @router.post("/api/v1/auth/keys", status_code=201)
    async def create_own_key(
        payload: KeyCreateRequest, request: Request, principal: Principal = Depends(require_web_user),
    ):
        return await issue_key(state(request).db, principal.user_id, payload.label)

    @router.delete("/api/v1/auth/keys/{key_id}", status_code=204)
    async def revoke_own_key(key_id: str, request: Request, principal: Principal = Depends(require_web_user)):
        if not await state(request).db.revoke_api_key(principal.user_id, key_id, now=int(time.time())):
            raise HTTPException(status_code=404, detail="key not found")
        return Response(status_code=204)

    @router.get("/api/v1/admin/users")
    async def list_users(request: Request, _admin: Principal = Depends(require_web_admin)):
        return await state(request).db.list_users()

    @router.post("/api/v1/admin/users", status_code=201)
    async def create_user(
        payload: UserCreateRequest, request: Request, _admin: Principal = Depends(require_web_admin),
    ):
        app_state = state(request)
        try:
            return await app_state.db.create_user(
                username=payload.username, display_name=payload.display_name,
                password_hash=await app_state.passwords.hash(payload.password),
                is_admin=payload.is_admin, now=int(time.time()),
            )
        except IdentityConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.patch("/api/v1/admin/users/{user_id}")
    async def update_user(
        user_id: str, payload: UserUpdateRequest, request: Request,
        _admin: Principal = Depends(require_web_admin),
    ):
        try:
            user, newly_disabled = await state(request).db.update_user(
                user_id, display_name=payload.display_name, disabled=payload.disabled,
                is_admin=payload.is_admin, now=int(time.time()),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="user not found") from exc
        except IdentityConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if newly_disabled:
            await on_user_disabled(user_id)
        return user

    @router.post("/api/v1/admin/users/{user_id}/password", status_code=204)
    async def reset_password(
        user_id: str, payload: PasswordResetRequest, request: Request,
        _admin: Principal = Depends(require_web_admin),
    ):
        app_state = state(request)
        try:
            await app_state.db.set_password_hash(
                user_id, await app_state.passwords.hash(payload.password), now=int(time.time()),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="user not found") from exc
        return Response(status_code=204)

    @router.get("/api/v1/admin/users/{user_id}/keys")
    async def list_user_keys(user_id: str, request: Request, _admin: Principal = Depends(require_web_admin)):
        db: Database = state(request).db
        if await db.get_user(user_id) is None:
            raise HTTPException(status_code=404, detail="user not found")
        return await db.list_api_keys(user_id)

    @router.post("/api/v1/admin/users/{user_id}/keys", status_code=201)
    async def create_user_key(
        user_id: str, payload: KeyCreateRequest, request: Request,
        _admin: Principal = Depends(require_web_admin),
    ):
        return await issue_key(state(request).db, user_id, payload.label)

    @router.delete("/api/v1/admin/users/{user_id}/keys/{key_id}", status_code=204)
    async def revoke_user_key(
        user_id: str, key_id: str, request: Request, _admin: Principal = Depends(require_web_admin),
    ):
        if not await state(request).db.revoke_api_key(user_id, key_id, now=int(time.time())):
            raise HTTPException(status_code=404, detail="key not found")
        return Response(status_code=204)

    return router
