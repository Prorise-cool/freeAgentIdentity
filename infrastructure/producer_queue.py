"""向下游提供 ChatGPT 账号的持久生产队列。"""
from __future__ import annotations

import ast
import base64
import hashlib
import json
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from application.account_exports import _chatgpt_export_payload
from core.db import engine
from infrastructure.accounts_repository import AccountsRepository


_QUEUE_LOCK = threading.RLock()


class QueueConflict(RuntimeError):
    """租约状态与请求不一致。"""


def _now() -> datetime:
    """返回带时区的 UTC 当前时间。"""
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    """把时间统一序列化为可按文本比较的 UTC 格式。"""
    return value.astimezone(timezone.utc).isoformat()


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    """解码 JWT payload；格式无效时返回空对象。"""
    try:
        parts = str(token or "").split(".")
        if len(parts) != 3:
            return {}
        encoded = parts[1].replace("-", "+").replace("_", "/")
        encoded += "=" * ((4 - len(encoded) % 4) % 4)
        payload = json.loads(base64.b64decode(encoded).decode("utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _cookie_header(value: Any) -> str:
    """把字典、JSON 或旧 Python repr cookies 转为标准 Cookie 请求头。"""
    if isinstance(value, dict):
        return "; ".join(f"{key}={item}" for key, item in value.items() if key and item is not None)
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        try:
            parsed = ast.literal_eval(raw)
        except (SyntaxError, ValueError):
            return raw
    if not isinstance(parsed, dict):
        return raw
    return "; ".join(f"{key}={item}" for key, item in parsed.items() if key and item is not None)


def _ensure_queue_column(connection: Any, name: str, definition: str) -> None:
    """为已有 SQLite 生产队列表补充缺失字段。"""
    columns = {
        str(row[1])
        for row in connection.execute(text("PRAGMA table_info(producer_deliveries)")).fetchall()
    }
    if name not in columns:
        connection.execute(text(f"ALTER TABLE producer_deliveries ADD COLUMN {name} {definition}"))


def init_producer_queue() -> None:
    """创建队列表，并把升级前已有的 ChatGPT 账号补入可用队列。"""
    now = _iso(_now())
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS producer_deliveries (
                account_id INTEGER PRIMARY KEY,
                state TEXT NOT NULL DEFAULT 'available',
                lease_id TEXT NOT NULL DEFAULT '',
                consumer_id TEXT NOT NULL DEFAULT '',
                lease_expires_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                credential_revision TEXT NOT NULL DEFAULT '',
                acked_lease_id TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                acked_at TEXT,
                FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
            )
        """))
        _ensure_queue_column(connection, "credential_revision", "TEXT NOT NULL DEFAULT ''")
        _ensure_queue_column(connection, "acked_lease_id", "TEXT NOT NULL DEFAULT ''")
        connection.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_producer_deliveries_state ON producer_deliveries(state, account_id)"
        ))
        connection.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_producer_deliveries_lease ON producer_deliveries(lease_id, state)"
        ))
        connection.execute(
            text("""
                INSERT INTO producer_deliveries (account_id, state, created_at, updated_at)
                SELECT accounts.id, 'available', :now, :now
                FROM accounts
                WHERE accounts.platform = 'chatgpt'
                  AND NOT EXISTS (
                      SELECT 1 FROM producer_deliveries
                      WHERE producer_deliveries.account_id = accounts.id
                  )
            """),
            {"now": now},
        )


def enqueue_account(account_id: int) -> None:
    """注册成功或凭据刷新后，仅在凭据版本变化时重新开放投递。"""
    safe_id = int(account_id or 0)
    if safe_id <= 0:
        raise ValueError("账号 ID 无效")
    init_producer_queue()
    now = _iso(_now())
    try:
        revision = str(_delivery_payload(safe_id)["credential_revision"])
    except ValueError as exc:
        with _QUEUE_LOCK, engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO producer_deliveries
                        (account_id, state, created_at, updated_at, last_error)
                    VALUES (:account_id, 'blocked', :now, :now, :error)
                    ON CONFLICT(account_id) DO UPDATE SET
                        state = 'blocked', lease_id = '', consumer_id = '',
                        lease_expires_at = NULL, acked_lease_id = '',
                        last_error = excluded.last_error, updated_at = excluded.updated_at
                """),
                {"account_id": safe_id, "now": now, "error": str(exc)[:500]},
            )
        return
    with _QUEUE_LOCK, engine.begin() as connection:
        current = connection.execute(
            text("SELECT state, credential_revision FROM producer_deliveries WHERE account_id = :account_id"),
            {"account_id": safe_id},
        ).mappings().first()
        if current and str(current["credential_revision"] or "") == revision and current["state"] != "blocked":
            return
        connection.execute(
            text("""
                INSERT INTO producer_deliveries
                    (account_id, state, credential_revision, created_at, updated_at)
                VALUES (:account_id, 'available', :revision, :now, :now)
                ON CONFLICT(account_id) DO UPDATE SET
                    state = 'available', lease_id = '', consumer_id = '',
                    lease_expires_at = NULL, credential_revision = excluded.credential_revision,
                    acked_lease_id = '', last_error = '', acked_at = NULL,
                    updated_at = excluded.updated_at
            """),
            {"account_id": safe_id, "revision": revision, "now": now},
        )


def _reclaim_expired(connection: Any, now: str) -> int:
    """回收已经超时的租约，保证消费者崩溃后账号不会丢失。"""
    result = connection.execute(
        text("""
            UPDATE producer_deliveries
            SET state = 'available', lease_id = '', consumer_id = '',
                lease_expires_at = NULL, updated_at = :now,
                last_error = CASE WHEN last_error = '' THEN '租约超时自动释放' ELSE last_error END
            WHERE state = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= :now
        """),
        {"now": now},
    )
    return int(result.rowcount or 0)


def inventory() -> dict[str, Any]:
    """返回各队列状态数量，并顺手回收过期租约。"""
    init_producer_queue()
    now = _iso(_now())
    with _QUEUE_LOCK, engine.begin() as connection:
        reclaimed = _reclaim_expired(connection, now)
        rows = connection.execute(
            text("SELECT state, COUNT(*) AS total FROM producer_deliveries GROUP BY state")
        ).mappings().all()
    counts = {str(row["state"]): int(row["total"] or 0) for row in rows}
    return {
        "available": counts.get("available", 0),
        "leased": counts.get("leased", 0),
        "acked": counts.get("acked", 0),
        "blocked": counts.get("blocked", 0),
        "reclaimed": reclaimed,
    }


def _delivery_payload(account_id: int) -> dict[str, Any]:
    """校验账号状态和令牌后，转换为稳定的生产 API 凭据格式。"""
    record = AccountsRepository().get(account_id)
    if record is None or record.platform != "chatgpt":
        raise ValueError("ChatGPT 账号不存在")
    if record.lifecycle_status in {"invalid", "expired", "deleted", "disabled", "cancelled", "canceled"}:
        raise ValueError(f"账号生命周期不可投递: {record.lifecycle_status}")
    if record.validity_status == "invalid":
        raise ValueError("账号有效性检测为 invalid")
    payload = _chatgpt_export_payload(record)
    email = str(payload.get("email") or "").strip().lower()
    if not email or "@" not in email:
        raise ValueError("缺少有效 email")
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise ValueError("缺少 access_token")
    claims = _decode_jwt_payload(access_token)
    if not claims:
        raise ValueError("access_token 不是有效 JWT")
    expires_at = int(claims.get("exp") or 0)
    if expires_at <= int(_now().timestamp()) + 60:
        raise ValueError("access_token 已过期或即将过期")
    auth = claims.get("https://api.openai.com/auth")
    profile = claims.get("https://api.openai.com/profile")
    auth = auth if isinstance(auth, dict) else {}
    profile = profile if isinstance(profile, dict) else {}
    token_email = str(profile.get("email") or claims.get("email") or "").strip().lower()
    if token_email and token_email != email:
        raise ValueError("access_token 邮箱与账号记录不一致")
    account_id_value = str(payload.get("account_id") or "").strip()
    if not account_id_value:
        raise ValueError("缺少 account_id")
    token_account_id = str(
        auth.get("chatgpt_account_id")
        or auth.get("account_id")
        or claims.get("chatgpt_account_id")
        or claims.get("account_id")
        or ""
    ).strip()
    if token_account_id and token_account_id != account_id_value:
        raise ValueError("access_token account_id 与账号记录不一致")
    cookie_header = _cookie_header(payload.get("cookies"))
    revision_source = json.dumps(
        {
            "email": email,
            "account_id": account_id_value,
            "access_token": access_token,
            "refresh_token": str(payload.get("refresh_token") or ""),
            "session_token": str(payload.get("session_token") or ""),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    credential_revision = hashlib.sha256(revision_source.encode("utf-8")).hexdigest()
    raw_session: dict[str, Any] = {
        "accessToken": access_token,
        "sessionToken": str(payload.get("session_token") or ""),
        "cookie_header": cookie_header,
    }
    if account_id_value:
        raw_session["account"] = {"id": account_id_value}
    return {
        "delivery_id": account_id,
        "producer_account_id": account_id,
        "producer_namespace": "freeagent",
        "email": email,
        "password": str(payload.get("password") or ""),
        "account_id": account_id_value,
        "access_token": access_token,
        "refresh_token": str(payload.get("refresh_token") or ""),
        "id_token": str(payload.get("id_token") or ""),
        "session_token": str(payload.get("session_token") or ""),
        "cookie_header": cookie_header,
        "raw_session": raw_session,
        "credential_source": "freeagent_producer",
        "credential_revision": credential_revision,
        "token_expires_at": expires_at,
    }


def lease_accounts(consumer_id: str, limit: int, lease_seconds: int) -> dict[str, Any]:
    """原子领取一批有效账号；无效凭据转 blocked，不反复投递。"""
    init_producer_queue()
    safe_consumer = str(consumer_id or "").strip()
    if not safe_consumer:
        raise ValueError("consumer_id 不能为空")
    safe_limit = max(1, min(100, int(limit or 1)))
    safe_seconds = max(30, min(3600, int(lease_seconds or 300)))
    now_dt = _now()
    now = _iso(now_dt)
    expires_at = _iso(now_dt + timedelta(seconds=safe_seconds))
    lease_id = uuid.uuid4().hex
    items: list[dict[str, Any]] = []
    with _QUEUE_LOCK, engine.begin() as connection:
        _reclaim_expired(connection, now)
        candidates = connection.execute(
            text("""
                SELECT account_id FROM producer_deliveries
                WHERE state = 'available'
                ORDER BY account_id ASC
                LIMIT :candidate_limit
            """),
            {"candidate_limit": safe_limit * 4},
        ).scalars().all()
        for raw_id in candidates:
            account_id = int(raw_id)
            try:
                item = _delivery_payload(account_id)
            except ValueError as exc:
                connection.execute(
                    text("""
                        UPDATE producer_deliveries
                        SET state = 'blocked', last_error = :error, updated_at = :now
                        WHERE account_id = :account_id AND state = 'available'
                    """),
                    {"account_id": account_id, "error": str(exc)[:500], "now": now},
                )
                continue
            result = connection.execute(
                text("""
                    UPDATE producer_deliveries
                    SET state = 'leased', lease_id = :lease_id, consumer_id = :consumer_id,
                        lease_expires_at = :expires_at, attempts = attempts + 1,
                        credential_revision = :credential_revision,
                        acked_lease_id = '', last_error = '', updated_at = :now
                    WHERE account_id = :account_id AND state = 'available'
                """),
                {
                    "account_id": account_id,
                    "lease_id": lease_id,
                    "consumer_id": safe_consumer,
                    "expires_at": expires_at,
                    "credential_revision": item["credential_revision"],
                    "now": now,
                },
            )
            if int(result.rowcount or 0) == 1:
                items.append(item)
            if len(items) >= safe_limit:
                break
    return {"lease_id": lease_id if items else "", "expires_at": expires_at if items else "", "items": items}


def _finish_lease(lease_id: str, delivery_ids: list[int], *, ack: bool, reason: str = "") -> dict[str, int]:
    """带过期 fencing 确认或释放租约；确认请求支持安全重放。"""
    safe_lease = str(lease_id or "").strip()
    ids = list(dict.fromkeys(int(value) for value in delivery_ids if int(value) > 0))
    if not safe_lease or not ids:
        raise ValueError("lease_id 和 delivery_ids 不能为空")
    now = _iso(_now())
    target_state = "acked" if ack else "available"
    changed = 0
    with _QUEUE_LOCK, engine.begin() as connection:
        _reclaim_expired(connection, now)
        for account_id in ids:
            current = connection.execute(
                text("""
                    SELECT state, lease_id, lease_expires_at, acked_lease_id
                    FROM producer_deliveries WHERE account_id = :account_id
                """),
                {"account_id": account_id},
            ).mappings().first()
            if ack and current and current["state"] == "acked" and current["acked_lease_id"] == safe_lease:
                changed += 1
                continue
            result = connection.execute(
                text("""
                    UPDATE producer_deliveries
                    SET state = :target_state, lease_id = '', consumer_id = '',
                        lease_expires_at = NULL, last_error = :last_error,
                        acked_lease_id = :acked_lease_id,
                        acked_at = :acked_at, updated_at = :now
                    WHERE account_id = :account_id AND state = 'leased'
                      AND lease_id = :lease_id AND lease_expires_at > :now
                """),
                {
                    "target_state": target_state,
                    "last_error": "" if ack else str(reason or "消费失败")[:500],
                    "acked_at": now if ack else None,
                    "acked_lease_id": safe_lease if ack else "",
                    "now": now,
                    "account_id": account_id,
                    "lease_id": safe_lease,
                },
            )
            changed += int(result.rowcount or 0)
        if changed != len(ids):
            raise QueueConflict("租约已过期、条目不属于该租约或已被处理")
    return {"processed": changed}


def ack_lease(lease_id: str, delivery_ids: list[int]) -> dict[str, int]:
    """确认已经被下游持久化的条目。"""
    return _finish_lease(lease_id, delivery_ids, ack=True)


def release_lease(lease_id: str, delivery_ids: list[int], reason: str = "") -> dict[str, int]:
    """释放导入失败的条目，使其可立即重试。"""
    return _finish_lease(lease_id, delivery_ids, ack=False, reason=reason)
