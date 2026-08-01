"""生产者租约 API 回归测试。"""
from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from core.base_platform import Account
from core.db import engine, save_account
from infrastructure.producer_queue import enqueue_account, inventory


def _headers() -> dict[str, str]:
    """返回测试生产 API 鉴权头。"""
    return {"X-Producer-Key": "producer-test-key"}


def _access_token(email: str, account_id: str, *, expires_in: int = 3600) -> str:
    """生成带邮箱、账号 ID 和过期时间的测试 JWT。"""
    payload = {
        "exp": int((datetime.now(timezone.utc) + timedelta(seconds=expires_in)).timestamp()),
        "https://api.openai.com/profile": {"email": email},
        "https://api.openai.com/auth": {"chatgpt_account_id": account_id},
    }
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii").rstrip("=")
    return f"header.{encoded}.signature"


def _create_account(*, email: str = "producer@test.com", access_token: str | None = None) -> tuple[int, str]:
    """创建并入队一个 ChatGPT 测试账号。"""
    account_id = f"acct-{email.split('@', 1)[0]}"
    resolved_token = _access_token(email, account_id) if access_token is None else access_token
    model = save_account(
        Account(
            platform="chatgpt",
            email=email,
            password="Pass123!",
            user_id=account_id,
            extra={
                "access_token": resolved_token,
                "refresh_token": "refresh-token",
                "session_token": "session-token",
                "cookies": {"__Secure-next-auth.session-token": "session-token"},
            },
        )
    )
    enqueue_account(int(model.id))
    return int(model.id), resolved_token


def test_producer_api_rejects_wrong_key(client, monkeypatch):
    """错误服务密钥不能查看或领取库存。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    response = client.get("/api/producer/inventory", headers={"X-Producer-Key": "wrong"})
    assert response.status_code == 401


def test_lease_ack_flow_is_exclusive(client, monkeypatch):
    """同一账号只能处于一个有效租约，确认后进入终态。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    account_id, access_token = _create_account()
    first = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "teamauto", "limit": 10, "lease_seconds": 300},
    )
    assert first.status_code == 200
    lease = first.json()
    assert [item["delivery_id"] for item in lease["items"]] == [account_id]
    assert lease["items"][0]["raw_session"]["accessToken"] == access_token
    assert lease["items"][0]["producer_namespace"] == "freeagent"
    assert len(lease["items"][0]["credential_revision"]) == 64

    second = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "other", "limit": 10, "lease_seconds": 300},
    )
    assert second.json()["items"] == []

    acknowledged = client.post(
        f"/api/producer/leases/{lease['lease_id']}/ack",
        headers=_headers(),
        json={"delivery_ids": [account_id]},
    )
    assert acknowledged.status_code == 200
    assert inventory()["acked"] == 1
    replay = client.post(
        f"/api/producer/leases/{lease['lease_id']}/ack",
        headers=_headers(),
        json={"delivery_ids": [account_id]},
    )
    assert replay.status_code == 200
    assert replay.json()["processed"] == 1


def test_release_makes_account_available_again(client, monkeypatch):
    """消费失败释放后，账号可被下一轮重新领取。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    account_id, _ = _create_account(email="release@test.com")
    leased = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "teamauto", "limit": 1, "lease_seconds": 300},
    ).json()
    response = client.post(
        f"/api/producer/leases/{leased['lease_id']}/release",
        headers=_headers(),
        json={"delivery_ids": [account_id], "reason": "数据库暂时不可用"},
    )
    assert response.status_code == 200
    assert inventory()["available"] == 1


def test_missing_access_token_is_blocked(client, monkeypatch):
    """缺必要凭据的账号不会交付给消费者。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    _create_account(email="blocked@test.com", access_token="")
    leased = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "teamauto", "limit": 1, "lease_seconds": 300},
    ).json()
    assert leased["items"] == []
    assert inventory()["blocked"] == 1


def test_expired_access_token_is_blocked(client, monkeypatch):
    """已经过期的 JWT 不得进入下游预热池。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    email = "expired@test.com"
    _create_account(email=email, access_token=_access_token(email, "acct-expired", expires_in=-60))
    leased = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "teamauto", "limit": 1, "lease_seconds": 300},
    ).json()
    assert leased["items"] == []
    assert inventory()["blocked"] == 1


def test_expired_lease_cannot_be_acknowledged(client, monkeypatch):
    """租约到期后旧消费者不能确认条目。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    account_id, _ = _create_account(email="lease-expired@test.com")
    lease = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "teamauto", "limit": 1, "lease_seconds": 300},
    ).json()
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE producer_deliveries SET lease_expires_at = :expired WHERE account_id = :account_id"),
            {"expired": "2000-01-01T00:00:00+00:00", "account_id": account_id},
        )
    response = client.post(
        f"/api/producer/leases/{lease['lease_id']}/ack",
        headers=_headers(),
        json={"delivery_ids": [account_id]},
    )
    assert response.status_code == 409
    assert inventory()["available"] == 1


def test_credential_update_requeues_same_account(client, monkeypatch):
    """已确认账号的凭据版本变化后，应以同一生产账号 ID 再次投递。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    email = "refresh@test.com"
    account_id, old_token = _create_account(email=email)
    first = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "teamauto", "limit": 1, "lease_seconds": 300},
    ).json()
    client.post(
        f"/api/producer/leases/{first['lease_id']}/ack",
        headers=_headers(),
        json={"delivery_ids": [account_id]},
    )
    new_token = _access_token(email, "acct-refresh", expires_in=7200)
    updated = client.patch(
        f"/api/accounts/{account_id}",
        json={"credentials": {"access_token": new_token}},
    )
    assert updated.status_code == 200

    second = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "teamauto", "limit": 1, "lease_seconds": 300},
    ).json()
    assert second["items"][0]["producer_account_id"] == account_id
    assert second["items"][0]["access_token"] == new_token
    assert second["items"][0]["access_token"] != old_token
