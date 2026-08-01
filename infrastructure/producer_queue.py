"""向下游提供 ChatGPT 账号的持久生产队列。"""
from __future__ import annotations

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


def _cookie_header(value: Any) -> str:
    """把字典或 JSON cookies 转为标准 Cookie 请求头。"""
    if isinstance(value, dict):
        return "; ".join(f"{key}={item}" for key, item in value.items() if key and item is not None)
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return raw
    if not isinstance(parsed, dict):
        return raw
    return "; ".join(f"{key}={item}" for key, item in parsed.items() if key and item is not None)


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
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                acked_at TEXT,
                FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
            )
        """))
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
    """注册成功或凭据刷新后，把账号重新置为可领取。"""
    safe_id = int(account_id or 0)
    if safe_id <= 0:
        raise ValueError("账号 ID 无效")
    init_producer_queue()
    now = _iso(_now())
    with _QUEUE_LOCK, engine.begin() as connection:
        connection.execute(
            text("""
                INSERT INTO producer_deliveries (account_id, state, created_at, updated_at)
                VALUES (:account_id, 'available', :now, :now)
                ON CONFLICT(account_id) DO UPDATE SET
                    state = 'available', lease_id = '', consumer_id = '',
                    lease_expires_at = NULL, last_error = '', acked_at = NULL,
                    updated_at = excluded.updated_at
            """),
            {"account_id": safe_id, "now": now},
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
    """把内部账号记录转换为稳定的生产 API 凭据格式。"""
    record = AccountsRepository().get(account_id)
    if record is None or record.platform != "chatgpt":
        raise ValueError("ChatGPT 账号不存在")
    payload = _chatgpt_export_payload(record)
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise ValueError("缺少 access_token")
    cookie_header = _cookie_header(payload.get("cookies"))
    account_id_value = str(payload.get("account_id") or "").strip()
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
        "email": str(payload.get("email") or "").strip().lower(),
        "password": str(payload.get("password") or ""),
        "account_id": account_id_value,
        "access_token": access_token,
        "refresh_token": str(payload.get("refresh_token") or ""),
        "id_token": str(payload.get("id_token") or ""),
        "session_token": str(payload.get("session_token") or ""),
        "cookie_header": cookie_header,
        "raw_session": raw_session,
        "credential_source": "freeagent_producer",
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
                        last_error = '', updated_at = :now
                    WHERE account_id = :account_id AND state = 'available'
                """),
                {
                    "account_id": account_id,
                    "lease_id": lease_id,
                    "consumer_id": safe_consumer,
                    "expires_at": expires_at,
                    "now": now,
                },
            )
            if int(result.rowcount or 0) == 1:
                items.append(item)
            if len(items) >= safe_limit:
                break
    return {"lease_id": lease_id if items else "", "expires_at": expires_at if items else "", "items": items}


def _finish_lease(lease_id: str, delivery_ids: list[int], *, ack: bool, reason: str = "") -> dict[str, int]:
    """严格确认或释放指定租约条目，拒绝部分静默成功。"""
    safe_lease = str(lease_id or "").strip()
    ids = list(dict.fromkeys(int(value) for value in delivery_ids if int(value) > 0))
    if not safe_lease or not ids:
        raise ValueError("lease_id 和 delivery_ids 不能为空")
    now = _iso(_now())
    target_state = "acked" if ack else "available"
    changed = 0
    with _QUEUE_LOCK, engine.begin() as connection:
        for account_id in ids:
            result = connection.execute(
                text("""
                    UPDATE producer_deliveries
                    SET state = :target_state, lease_id = '', consumer_id = '',
                        lease_expires_at = NULL, last_error = :last_error,
                        acked_at = :acked_at, updated_at = :now
                    WHERE account_id = :account_id AND state = 'leased' AND lease_id = :lease_id
                """),
                {
                    "target_state": target_state,
                    "last_error": "" if ack else str(reason or "消费失败")[:500],
                    "acked_at": now if ack else None,
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
